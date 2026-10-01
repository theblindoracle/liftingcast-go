package liftingcast

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
)

// listener is one Listen call: a handler with its own unbounded queue and
// goroutine. The handler is called with one state at a time, in the order the
// Hub produced them, so a slow handler delays only its own listener.
//
// Only the Hub stops a listener, through stop, which is safe to call any
// number of times.
type listener struct {
	handler func(*MeetApiResponse)

	// Queue of states not yet handed to handler, and whether the listener
	// has stopped. wake is signalled whenever a state is added; done is
	// closed when the listener stops; exited is closed when the delivery
	// goroutine returns, after its last handler call.
	mu      sync.Mutex
	pending []*MeetApiResponse
	stopped bool
	wake    chan struct{}
	done    chan struct{}
	exited  chan struct{}
}

func newListener(handler func(*MeetApiResponse)) *listener {
	return &listener{
		handler: handler,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		exited:  make(chan struct{}),
	}
}

// start runs the goroutine that delivers queued states to handler until the
// listener stops.
func (l *listener) start() {
	go l.deliver()
}

// enqueue adds a state to the listener's queue. It never blocks, and does
// nothing once the listener has stopped.
func (l *listener) enqueue(state *MeetApiResponse) {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return
	}
	l.pending = append(l.pending, state)
	l.mu.Unlock()
	signal(l.wake)
}

// stop discards the queued states, ensures no handler call starts after it
// returns, and waits for a call already in progress to finish. From inside
// any listener's handler it does not wait: see wait.
func (l *listener) stop() {
	l.signalStop()
	l.wait()
}

// signalStop discards the queued states and ensures no handler call starts
// after it returns, without waiting for one in progress.
func (l *listener) signalStop() {
	l.mu.Lock()
	if l.stopped {
		l.mu.Unlock()
		return
	}
	l.stopped = true
	l.pending = nil
	l.mu.Unlock()
	close(l.done)
}

// wait waits for the delivery goroutine to return, once the listener has
// been signalled to stop, unless the caller is itself inside a handler. A
// handler waiting for its own goroutine would never return, and two
// handlers stopping each other would each wait for the other.
func (l *listener) wait() {
	if !inHandler() {
		<-l.exited
	}
}

// deliveryGoroutines holds the goroutine ID of every running delivery
// goroutine, so stop and Close can tell whether they were called from
// inside a handler. Go has no other way to tell.
var deliveryGoroutines sync.Map

// inHandler reports whether the caller is a delivery goroutine, which is
// only ever running the delivery loop or a handler.
func inHandler() bool {
	_, ok := deliveryGoroutines.Load(goroutineID())
	return ok
}

// goroutineID returns the calling goroutine's ID, read from the first line
// of its stack trace: "goroutine 123 [running]:".
func goroutineID() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i >= 0 {
		b = b[:i]
	}
	id, _ := strconv.ParseUint(string(b), 10, 64)
	return id
}

func (l *listener) deliver() {
	id := goroutineID()
	deliveryGoroutines.Store(id, struct{}{})
	defer deliveryGoroutines.Delete(id)
	defer close(l.exited)

	for {
		select {
		case <-l.done:
			return
		case <-l.wake:
		}

		for {
			// A state is taken off the queue under mu only while the
			// listener hasn't stopped, so once signalStop has set stopped
			// no further call starts, and the one in progress, if any, is
			// the last before exited closes.
			l.mu.Lock()
			if l.stopped {
				l.mu.Unlock()
				return
			}
			if len(l.pending) == 0 {
				l.mu.Unlock()
				break
			}
			state := l.pending[0]
			l.pending[0] = nil
			l.pending = l.pending[1:]
			l.mu.Unlock()

			l.handler(state)
		}
	}
}
