package liftingcast

import "sync"

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
	// closed when the listener stops.
	mu      sync.Mutex
	pending []*MeetApiResponse
	stopped bool
	wake    chan struct{}
	done    chan struct{}
}

func newListener(handler func(*MeetApiResponse)) *listener {
	return &listener{
		handler: handler,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
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

// stop discards the queued states and ensures no handler call starts after
// it returns. It never waits for the handler, so it is safe to call from
// inside it; a call already in progress runs to completion.
func (l *listener) stop() {
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

func (l *listener) deliver() {
	for {
		select {
		case <-l.done:
			return
		case <-l.wake:
		}

		for {
			// A call starts when its state is taken off the queue, which
			// happens under mu only while the listener hasn't stopped. So
			// once stop has set stopped under mu and returned, no call can
			// start, without stop ever having to wait for the handler. That
			// is what lets stop be called from inside the handler: Go has
			// no way to tell the caller is this goroutine, and waiting for
			// the handler from inside it would deadlock.
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
