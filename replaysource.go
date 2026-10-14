package liftingcast

import (
	"io"
	"sync"
	"time"
)

// ReplayMeetID is the MeetID a ReplaySource reports in its status. Recordings
// hold meet states only, and MeetState has no meet ID, so it is a placeholder.
const ReplayMeetID = "replay"

// ReplayOptions configures a ReplaySource.
type ReplayOptions struct {
	// Speed is the rate of the replay: 1 waits the gaps between the meet
	// states' receive times, 2 waits half as long, 0.5 twice as long. Zero,
	// or less, replays as fast as the listeners can take the states, which
	// suits tests.
	Speed float64

	// Loop starts the recording again, from an empty meet state, when it
	// ends. A recording with no meet states does not loop.
	Loop bool
}

// ReplaySource is a Source that plays a recording back in real time, or at a
// set speed, instead of following a meet live. Like the Hub it hands every
// listener each meet state in order, on the listener's own goroutine,
// starting with the current meet state if there is one. It never sends
// anything upstream.
//
// The replay starts at the first Listen, so that listener sees every meet
// state, and ends with the recording, or with Close. Once the recording has
// ended, and while looping, the last meet state stays the current one for
// late listeners. A slow handler delays only its own listener: states queue
// for it without limit and none is dropped, so pacing never depends on a
// handler.
//
// The receive times and connection numbers are the recording's own, so a
// looping replay repeats them on every pass, and the gap between the end of a
// recording and its start is not waited.
//
// Replay stops at the first error reading the recording, see Err.
type ReplaySource struct {
	r    io.ReadSeeker
	opts ReplayOptions

	// wait waits d, or until done closes, and reports whether the full time
	// passed. Tests replace it.
	wait func(d time.Duration, done <-chan struct{}) bool

	mu        sync.Mutex
	listeners map[*listener]struct{}
	current   *Update
	started   bool
	closed    bool
	err       error

	done      chan struct{}
	exited    chan struct{}
	closeOnce sync.Once
}

// NewReplaySource returns a ReplaySource for the recording read from r, in
// the format WithRecorder writes. r is read from its current position and
// rewound with Seek only when looping, and must not be used elsewhere until
// Close returns.
func NewReplaySource(r io.ReadSeeker, opts ReplayOptions) *ReplaySource {
	return &ReplaySource{
		r:         r,
		opts:      opts,
		wait:      realWait,
		listeners: make(map[*listener]struct{}),
		done:      make(chan struct{}),
		exited:    make(chan struct{}),
	}
}

func realWait(d time.Duration, done <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-done:
		return false
	}
}

// Listen registers handler to be called with every meet state, in order,
// starting with the current meet state if there is one, on a goroutine of its
// own, and starts the replay if this is the first call. The meet state is
// shared with every listener and must not be modified. It returns a stop that
// is safe to call any number of times: once it returns, handler is not
// running and will not be called again. Called from inside any listener's
// handler, stop does not wait for a call in progress. After Close, Listen
// never calls handler and returns a stop that does nothing.
func (s *ReplaySource) Listen(handler func(Update)) (stop func()) {
	l := newListener(handler)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return func() {}
	}
	l.start()
	if s.current != nil {
		l.enqueue(*s.current)
	}
	s.listeners[l] = struct{}{}
	if !s.started {
		s.started = true
		go s.run()
	}
	s.mu.Unlock()

	return func() {
		l.stop()
		s.mu.Lock()
		delete(s.listeners, l)
		s.mu.Unlock()
	}
}

// Status returns a connected status for the meet ReplayMeetID, whose meet
// name is that of the last meet state replayed, and whose LastMeetStateAt is
// that state's receive time. The status does not change when the recording
// ends or loops. After Close it returns the zero HubStatus.
func (s *ReplaySource) Status() HubStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return HubStatus{}
	}
	status := HubStatus{Connected: true, MeetID: ReplayMeetID}
	if s.current != nil {
		status.MeetName = s.current.State.Name
		status.LastMeetStateAt = s.current.ReceivedAt
	}
	return status
}

// Err returns the error that stopped the replay, if the recording could not
// be read or rewound, and nil otherwise.
func (s *ReplaySource) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close stops the replay and every listener, and returns once the replay has
// stopped and no handler is running, unless called from inside a handler, in
// which case it does not wait for handlers. It is safe to call any number of
// times.
func (s *ReplaySource) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		started := s.started
		listeners := s.listeners
		s.listeners = nil
		s.mu.Unlock()

		close(s.done)
		for l := range listeners {
			l.signalStop()
		}
		if started {
			<-s.exited
		}
		for l := range listeners {
			l.wait()
		}
	})
}

// run plays the recording to the listeners until it ends, or Close.
func (s *ReplaySource) run() {
	defer close(s.exited)

	for {
		var previous time.Time
		count := 0
		for u, err := range NewReplayer(s.r).All() {
			if err != nil {
				s.fail(err)
				return
			}
			if !s.pace(previous, u.ReceivedAt) {
				return
			}
			if !u.ReceivedAt.IsZero() {
				previous = u.ReceivedAt
			}
			s.publish(u)
			count++
		}
		if !s.opts.Loop || count == 0 {
			return
		}
		if _, err := s.r.Seek(0, io.SeekStart); err != nil {
			s.fail(err)
			return
		}
	}
}

// pace waits for the gap between two receive times at the set speed, and
// reports false if Close came first. A gap that is not positive, or unknown,
// is not waited.
func (s *ReplaySource) pace(previous, at time.Time) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	if s.opts.Speed <= 0 || previous.IsZero() || at.IsZero() || !at.After(previous) {
		return true
	}
	return s.wait(time.Duration(float64(at.Sub(previous))/s.opts.Speed), s.done)
}

func (s *ReplaySource) publish(u Update) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.current = &u
	for l := range s.listeners {
		l.enqueue(u)
	}
}

func (s *ReplaySource) fail(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}
