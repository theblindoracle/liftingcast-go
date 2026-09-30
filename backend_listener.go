package liftingcast

// BackendListenerHandler is a function that processes meet data updates
type BackendListenerHandler func(*MeetApiResponse)

// BackendListener represents an in-process listener that receives meet data updates
type BackendListener struct {
	ID      string
	Handler BackendListenerHandler
	Done    chan struct{}
}

// NewBackendListener creates a new backend listener
func NewBackendListener(id string, handler BackendListenerHandler) *BackendListener {
	return &BackendListener{
		ID:      id,
		Handler: handler,
		Done:    make(chan struct{}),
	}
}

// Close signals the listener to stop
func (l *BackendListener) Close() {
	close(l.Done)
}
