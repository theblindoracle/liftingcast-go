package liftingcast

import "sync"

// BackendListenerHandler is a function that processes meet data updates
type BackendListenerHandler func(*MeetApiResponse)

// BackendListener represents an in-process listener that receives meet data updates
//
// Once registered with a Hub, the listener has its own unbounded queue and
// goroutine: Handler is called with one state at a time, in the order the Hub
// produced them, and a slow Handler delays only its own listener.
//
// A listener is single-use: once it is unregistered or closed it cannot be
// registered again, and the Hub ignores any attempt to do so. To listen
// again, create a new one with NewBackendListener.
type BackendListener struct {
	ID      string
	Handler BackendListenerHandler
	Done    chan struct{}

	// Queue of states not yet handed to Handler. wake is signalled whenever
	// a state is added; exited is closed when the delivery goroutine returns.
	mu      sync.Mutex
	pending []*MeetApiResponse
	wake    chan struct{}
	exited  chan struct{}
}

// NewBackendListener creates a new backend listener
func NewBackendListener(id string, handler BackendListenerHandler) *BackendListener {
	return &BackendListener{
		ID:      id,
		Handler: handler,
		Done:    make(chan struct{}),
		wake:    make(chan struct{}, 1),
		exited:  make(chan struct{}),
	}
}

// Close signals the listener to stop. States still queued are discarded.
func (l *BackendListener) Close() {
	close(l.Done)
}

// start runs the goroutine that delivers queued states to Handler until
// Close.
func (l *BackendListener) start() {
	go l.deliver()
}

// enqueue adds a state to the listener's queue. It never blocks.
func (l *BackendListener) enqueue(state *MeetApiResponse) {
	l.mu.Lock()
	l.pending = append(l.pending, state)
	l.mu.Unlock()
	signal(l.wake)
}

func (l *BackendListener) deliver() {
	defer close(l.exited)
	defer func() {
		l.mu.Lock()
		l.pending = nil
		l.mu.Unlock()
	}()

	for {
		select {
		case <-l.Done:
			return
		case <-l.wake:
		}

		for {
			l.mu.Lock()
			if len(l.pending) == 0 {
				l.mu.Unlock()
				break
			}
			state := l.pending[0]
			l.pending[0] = nil
			l.pending = l.pending[1:]
			l.mu.Unlock()

			select {
			case <-l.Done:
				return
			default:
			}
			l.Handler(state)
		}
	}
}
