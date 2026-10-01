package liftingcast

import (
	"encoding/json"
	"log"
	"sync"
)

// ReconnectRequest contains the credentials for a new upstream connection
type ReconnectRequest struct {
	BaseURL  string
	MeetID   string
	Password string
	APIKey   string
}

// ConnectionStatus is the Client's ClientStatus plus the meet it is for
type ConnectionStatus struct {
	Connected bool   `json:"connected"`
	Rejected  bool   `json:"rejected"`
	MeetID    string `json:"meetId"`
	MeetName  string `json:"meetName"`
	Error     string `json:"error,omitempty"`
}

// Hub keeps one upstream LiftingCast connection alive, merges its messages
// into a Cache, and hands each merged state to in-process listeners added
// with Listen. It runs its own loop from NewHub or NewIdleHub until Close.
type Hub struct {
	// upstream carries raw messages from the upstream connection to the
	// loop, which merges them, so the cache never runs ahead of what
	// listeners have been sent
	upstream chan upstreamMessage

	// Listeners, keyed by themselves. Only the loop adds them, so a new
	// listener's first state is never older or newer than the next one the
	// loop hands out; Listen's stop and the loop's shutdown remove them.
	mu        sync.RWMutex
	listeners map[*listener]struct{}
	listen    chan listenRequest

	// Shared cache for meet data
	cache *Cache

	// Single persistent upstream LiftingCast connection and its meet. Only
	// the loop changes them, under upstreamMu, so GetStatus can read them.
	upstreamMu     sync.RWMutex
	upstreamClient *Client
	meetID         string

	// Reconnection channel
	reconnect chan *ReconnectRequest

	// closing is closed by the first Close, which then waits for the loop to
	// shut down and close closed
	closeOnce sync.Once
	closing   chan struct{}
	closed    chan struct{}
}

// listenRequest asks the loop to add a listener, and is answered on
// registered once it has
type listenRequest struct {
	listener   *listener
	registered chan struct{}
}

// NewHub creates a new hub instance with an upstream connection
func NewHub(baseURL, meetID, password, apiKey string) *Hub {
	hub := newHub()
	hub.meetID = meetID
	hub.upstreamClient = hub.startClient(&ReconnectRequest{
		BaseURL:  baseURL,
		MeetID:   meetID,
		Password: password,
		APIKey:   apiKey,
	})
	go hub.run()
	return hub
}

// NewIdleHub creates a hub without an upstream connection (idle mode)
func NewIdleHub() *Hub {
	hub := newHub()
	go hub.run()
	return hub
}

// newHub creates a hub whose loop has not started
func newHub() *Hub {
	return &Hub{
		upstream:  make(chan upstreamMessage, 10),
		listeners: make(map[*listener]struct{}),
		listen:    make(chan listenRequest),
		cache:     NewCache(),
		reconnect: make(chan *ReconnectRequest),
		closing:   make(chan struct{}),
		closed:    make(chan struct{}),
	}
}

// run is the hub's loop. It alone merges upstream messages, adds listeners
// and replaces the upstream connection, until Close.
func (h *Hub) run() {
	defer close(h.closed)
	for {
		select {
		case req := <-h.listen:
			h.addListener(req.listener)
			close(req.registered)
		case msg := <-h.upstream:
			h.handleUpstreamMessage(msg)
		case req := <-h.reconnect:
			h.handleReconnect(req)
		case <-h.closing:
			h.shutdown()
			return
		}
	}
}

// Listen calls handler with every meet state from now on, in order, until the
// returned stop is called or the Hub is closed. The first state is the
// current meet state if there is one; after that no state is skipped,
// repeated or older than the one before. Listen returns once the listener is
// in place, so every state merged after it returns reaches handler.
//
// Every listener is handed the same state, concurrently, so handler must
// treat it as read-only. The Hub never modifies a state once it has handed
// it out, so handler may keep it, for example to compare with the next. A
// handler that needs to modify the state must copy it first.
//
// Each listener has its own unbounded queue and goroutine: handler is called
// with one state at a time, and a slow handler delays only its own listener.
//
// stop may be called any number of times, from any goroutine, including from
// inside handler. Once it returns, no further call to handler starts; a call
// already in progress runs to completion, and stop does not wait for it.
// After Close, Listen never calls handler and returns a stop that does
// nothing.
func (h *Hub) Listen(handler func(*MeetApiResponse)) (stop func()) {
	l := newListener(handler)
	req := listenRequest{listener: l, registered: make(chan struct{})}
	select {
	case h.listen <- req:
	case <-h.closing:
		return func() {}
	}
	// The loop always answers a request it has taken
	<-req.registered

	return func() {
		l.stop()
		h.mu.Lock()
		delete(h.listeners, l)
		h.mu.Unlock()
	}
}

// Reconnect triggers a reconnection with new credentials. After Close it
// does nothing.
func (h *Hub) Reconnect(baseURL, meetID, password, apiKey string) {
	h.requestReconnect(&ReconnectRequest{
		BaseURL:  baseURL,
		MeetID:   meetID,
		Password: password,
		APIKey:   apiKey,
	})
}

// Disconnect stops the upstream connection and puts the hub in idle mode.
// After Close it does nothing.
func (h *Hub) Disconnect() {
	h.requestReconnect(nil)
}

// requestReconnect hands req to the loop, unless the hub is closing
func (h *Hub) requestReconnect(req *ReconnectRequest) {
	select {
	case h.reconnect <- req:
	case <-h.closing:
	}
}

// Close closes the upstream connection and stops every listener, with the
// same guarantee as each listener's stop: once Close returns, no further
// handler call starts, and a call already in progress runs to completion.
// It never waits for a handler, so it is safe to call from inside one. Close
// may be called any number of times. Afterwards the hub stays closed: see
// Listen, Reconnect, Disconnect and GetStatus.
func (h *Hub) Close() {
	h.closeOnce.Do(func() { close(h.closing) })
	<-h.closed
}

// shutdown closes the upstream connection, forgets the meet and stops every
// listener, as the loop's last act
func (h *Hub) shutdown() {
	if h.upstreamClient != nil {
		h.upstreamClient.Close()
	}
	h.upstreamMu.Lock()
	h.upstreamClient = nil
	h.meetID = ""
	h.upstreamMu.Unlock()
	h.cache.Clear()

	h.mu.Lock()
	for l := range h.listeners {
		l.stop()
	}
	h.listeners = nil
	h.mu.Unlock()
	log.Println("Hub closed")
}

// GetStatus returns the current connection status. After Close it returns
// the zero ConnectionStatus.
func (h *Hub) GetStatus() ConnectionStatus {
	h.upstreamMu.RLock()
	client, meetID := h.upstreamClient, h.meetID
	h.upstreamMu.RUnlock()

	var clientStatus ClientStatus
	if client != nil {
		clientStatus = client.Status()
	}
	status := ConnectionStatus{
		Connected: clientStatus.Connected,
		Rejected:  clientStatus.Rejected,
		MeetID:    meetID,
		Error:     clientStatus.LastError,
	}

	// Try to get meet name from cache
	if cachedData := h.cache.Get(); cachedData != nil {
		status.MeetName = cachedData.Name
	}

	return status
}

// handleReconnect processes a reconnection request
func (h *Hub) handleReconnect(req *ReconnectRequest) {
	log.Println("Processing reconnection request...")

	// Stop existing upstream connection if present
	if h.upstreamClient != nil {
		log.Println("Stopping existing upstream connection")
		h.upstreamClient.Close()
		h.upstreamMu.Lock()
		h.upstreamClient = nil
		h.upstreamMu.Unlock()
	}

	// Clear cache
	h.cache.Clear()

	// If nil request, stay in idle mode
	if req == nil {
		log.Println("Entering idle mode (no upstream connection)")
		h.upstreamMu.Lock()
		h.meetID = ""
		h.upstreamMu.Unlock()
		return
	}

	client := h.startClient(req)
	h.upstreamMu.Lock()
	h.upstreamClient = client
	h.meetID = req.MeetID
	h.upstreamMu.Unlock()

	log.Printf("Reconnection initiated for meet: %s", req.MeetID)
}

// startClient starts a new upstream connection and forwards its messages to
// the loop until it is closed or the hub has closed.
func (h *Hub) startClient(req *ReconnectRequest) *Client {
	client := NewClient(req.BaseURL, req.MeetID, req.Password, req.APIKey)
	client.Start()
	go func() {
		for raw := range client.Messages() {
			select {
			case h.upstream <- upstreamMessage{client: client, raw: raw}:
			case <-h.closed:
				return
			}
		}
	}()
	return client
}

// upstreamMessage is one raw message and the client it arrived on
type upstreamMessage struct {
	client *Client
	raw    json.RawMessage
}

// handleUpstreamMessage merges a message into the cache and queues the
// merged state for every listener
func (h *Hub) handleUpstreamMessage(msg upstreamMessage) {
	// Drop messages still in flight from a client that Reconnect or
	// Disconnect has since replaced, so they can't seed the cleared cache
	if msg.client != h.upstreamClient {
		return
	}

	merged, err := h.cache.Merge(msg.raw)
	if err != nil {
		log.Printf("Failed to merge data: %v", err)
		return
	}

	h.broadcast(merged)
}

// broadcast queues a merged state for every listener
func (h *Hub) broadcast(state *MeetApiResponse) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for l := range h.listeners {
		l.enqueue(state)
	}
}

// addListener starts delivering to a new listener, beginning with the cached
// state if there is one. Only the loop calls it, so no merge can land
// between reading the cache and the listener joining the next broadcast.
func (h *Hub) addListener(l *listener) {
	if cached := h.cache.Get(); cached != nil {
		l.enqueue(cached)
	}
	l.start()

	h.mu.Lock()
	h.listeners[l] = struct{}{}
	count := len(h.listeners)
	h.mu.Unlock()

	log.Printf("Listener added (total listeners: %d)", count)
}
