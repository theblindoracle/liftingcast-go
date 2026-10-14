package liftingcast

import (
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// recorderBuffer is how many events may wait for the recording's writer
// before new ones are dropped.
const recorderBuffer = 64

// ClientInfo tells the WithRecorder factory which Client a recording is for.
// It never holds credentials.
type ClientInfo struct {
	// MeetID is the meet the Client follows.
	MeetID string
	// Sequence numbers the Clients of one run, starting at 1. A recording
	// with a higher Sequence replaced the one before it.
	Sequence uint64
	// Start is when the Client was created.
	Start time.Time

	// replaces is the Sequence of the recording this one replaced, or zero
	replaces uint64
}

// RecorderFactory creates the writer for one Client's recording. The library
// closes the writer after the recording's final event.
type RecorderFactory func(ClientInfo) (io.WriteCloser, error)

// WithRecorder makes each Client keep a recording of its upstream
// connection, see ADR-0004. newWriter is called once for each Client, and a
// Client whose factory fails runs without a recording. Events the writer
// cannot take are dropped, never waited for, and a recorder_dropped event
// says how many.
func WithRecorder(newWriter RecorderFactory) Option {
	return func(o *options) {
		o.recorder = newWriter
	}
}

// eventName is the kind of a recordEvent. A meet state line has none.
type eventName string

const (
	eventStart           eventName = "start"
	eventDial            eventName = "dial"
	eventHandshake       eventName = "handshake"
	eventConnected       eventName = "connected"
	eventText            eventName = "text"
	eventClose           eventName = "close"
	eventReadError       eventName = "read_error"
	eventTimeout         eventName = "timeout"
	eventBackoff         eventName = "backoff"
	eventRejected        eventName = "rejected"
	eventClosed          eventName = "closed"
	eventReplaced        eventName = "replaced"
	eventRecorderDropped eventName = "recorder_dropped"
)

// recordEvent is one line of a recording. Its fields are shared with
// cmd/lc-tap; changes only add fields or events.
type recordEvent struct {
	Rx       string    `json:"rx"`
	Conn     uint64    `json:"conn"`
	Event    eventName `json:"event,omitempty"`
	Count    int64     `json:"count,omitempty"`    // recorder_dropped
	MeetID   string    `json:"meetId,omitempty"`   // start
	Sequence uint64    `json:"sequence,omitempty"` // start
	Replaces uint64    `json:"replaces,omitempty"` // start

	// The shapes below are those of cmd/lc-tap
	Status int             `json:"status,omitempty"` // handshake
	Code   int             `json:"code,omitempty"`   // close
	Bytes  int             `json:"bytes,omitempty"`  // meet state, text
	Msg    json.RawMessage `json:"msg,omitempty"`    // meet state
	Text   string          `json:"text,omitempty"`   // text, close
	Error  string          `json:"error,omitempty"`  // handshake, read_error, rejected
	Body   string          `json:"body,omitempty"`   // handshake

	WaitMs int64 `json:"waitMs,omitempty"` // backoff
}

// recorder writes a Client's events to its recording on its own goroutine,
// so a slow or failing writer cannot hold up the Client. A nil *recorder
// records nothing.
type recorder struct {
	events  chan recordEvent
	dropped atomic.Int64

	// mu orders record against finish, which closes events
	mu       sync.Mutex
	finished bool
}

// newRecorder calls factory and starts writing the recording, beginning with
// its start event. It returns the factory's error if it fails.
func newRecorder(factory RecorderFactory, info ClientInfo) (*recorder, error) {
	w, err := factory(info)
	if err != nil {
		return nil, err
	}
	r := &recorder{events: make(chan recordEvent, recorderBuffer)}
	r.record(recordEvent{
		Rx:       formatRecordTime(info.Start),
		Event:    eventStart,
		MeetID:   info.MeetID,
		Sequence: info.Sequence,
		Replaces: info.replaces,
	})
	go r.write(w)
	return r, nil
}

func formatRecordTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func recordTimeNow() string {
	return formatRecordTime(time.Now())
}

// record queues e without blocking, or counts it as dropped if the queue is
// full. It does nothing after finish.
func (r *recorder) record(e recordEvent) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	select {
	case r.events <- e:
	default:
		r.dropped.Add(1)
	}
}

// finish ends the recording with the final event, which is never dropped:
// if the queue is full it waits for room on its own goroutine, so finish
// does not wait for the writer. The writer then drains and closes the
// recording.
func (r *recorder) finish(final eventName) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	r.finished = true
	e := recordEvent{Rx: recordTimeNow(), Event: final}
	select {
	case r.events <- e:
		close(r.events)
	default:
		go func() {
			r.events <- e
			close(r.events)
		}()
	}
}

// write is the recorder's goroutine: one Write call for each line. Events
// lost to a full queue or a failed write are reported by a recorder_dropped
// event ahead of the next line that gets written.
func (r *recorder) write(w io.WriteCloser) {
	defer w.Close()
	writeLine := func(e recordEvent) bool {
		line, err := json.Marshal(e)
		if err != nil {
			return false
		}
		_, err = w.Write(append(line, '\n'))
		return err == nil
	}
	flushDropped := func() {
		if n := r.dropped.Swap(0); n > 0 {
			e := recordEvent{Rx: recordTimeNow(), Event: eventRecorderDropped, Count: n}
			if !writeLine(e) {
				r.dropped.Add(n)
			}
		}
	}
	for e := range r.events {
		flushDropped()
		if !writeLine(e) {
			r.dropped.Add(1)
		}
	}
	flushDropped()
}
