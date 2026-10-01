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
	MeetID    string `json:"meetId"`
	MeetName  string `json:"meetName"`
	Error     string `json:"error,omitempty"`
}

// Hub keeps one upstream LiftingCast connection alive, merges its messages
// into a Cache, and hands each merged state to in-process BackendListeners.
type Hub struct {
	mu sync.RWMutex

	// upstream carries raw messages from the upstream connection to Run,
	// which merges them, so the cache never runs ahead of what listeners
	// have been sent
	upstream chan upstreamMessage

	// Backend listeners (in-process subscribers)
	backendListeners  map[string]*BackendListener
	registerBackend   chan *BackendListener
	unregisterBackend chan *BackendListener

	// Shared cache for meet data
	cache *Cache

	// Single persistent upstream LiftingCast connection and its meet. Only
	// Run changes them, under upstreamMu, so GetStatus can read them.
	upstreamMu     sync.RWMutex
	upstreamClient *Client
	meetID         string

	// Reconnection channel
	reconnect chan *ReconnectRequest
}

// NewHub creates a new hub instance with an upstream connection
func NewHub(baseURL, meetID, password, apiKey string) *Hub {
	hub := NewIdleHub()
	hub.meetID = meetID
	hub.upstreamClient = hub.startClient(&ReconnectRequest{
		BaseURL:  baseURL,
		MeetID:   meetID,
		Password: password,
		APIKey:   apiKey,
	})
	return hub
}

// NewIdleHub creates a hub without an upstream connection (idle mode)
func NewIdleHub() *Hub {
	return &Hub{
		upstream:          make(chan upstreamMessage, 10),
		backendListeners:  make(map[string]*BackendListener),
		registerBackend:   make(chan *BackendListener),
		unregisterBackend: make(chan *BackendListener),
		cache:             NewCache(),
		reconnect:         make(chan *ReconnectRequest),
	}
}

// Run starts the hub's main loop
func (h *Hub) Run() {
	for {
		select {
		case listener := <-h.registerBackend:
			h.registerBackendListener(listener)
		case listener := <-h.unregisterBackend:
			h.unregisterBackendListener(listener)
		case msg := <-h.upstream:
			h.handleUpstreamMessage(msg)
		case req := <-h.reconnect:
			h.handleReconnect(req)
		}
	}
}

// Reconnect triggers a reconnection with new credentials
func (h *Hub) Reconnect(baseURL, meetID, password, apiKey string) {
	h.reconnect <- &ReconnectRequest{
		BaseURL:  baseURL,
		MeetID:   meetID,
		Password: password,
		APIKey:   apiKey,
	}
}

// Disconnect stops the upstream connection and puts the hub in idle mode
func (h *Hub) Disconnect() {
	h.reconnect <- nil
}

// GetStatus returns the current connection status
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
// Run until it is closed.
func (h *Hub) startClient(req *ReconnectRequest) *Client {
	client := NewClient(req.BaseURL, req.MeetID, req.Password, req.APIKey)
	client.Start()
	go func() {
		for raw := range client.Messages() {
			h.upstream <- upstreamMessage{client: client, raw: raw}
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

	h.broadcastToBackendListeners(merged)
}

// broadcastToBackendListeners queues meet data for all registered backend listeners
func (h *Hub) broadcastToBackendListeners(data *MeetApiResponse) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, listener := range h.backendListeners {
		listener.enqueue(data)
	}
}

// registerBackendListener registers a new backend listener and starts
// delivering to it, beginning with the cached state if there is one. A
// listener it replaces under the same ID is closed.
func (h *Hub) registerBackendListener(listener *BackendListener) {
	select {
	case <-listener.Done:
		log.Printf("Ignoring registration of closed backend listener: %s", listener.ID)
		return
	default:
	}

	h.mu.Lock()
	previous, replaced := h.backendListeners[listener.ID]
	if replaced && previous == listener {
		h.mu.Unlock()
		return
	}
	if replaced {
		previous.Close()
	}
	h.backendListeners[listener.ID] = listener
	count := len(h.backendListeners)

	// Queue the cached state before the listener can see any later broadcast
	if cachedData := h.cache.Get(); cachedData != nil {
		listener.enqueue(cachedData)
		log.Printf("Queued cached data for backend listener: %s", listener.ID)
	}
	listener.start()
	h.mu.Unlock()

	log.Printf("Backend listener registered: %s (total listeners: %d)", listener.ID, count)
}

// unregisterBackendListener removes a backend listener, unless it has
// already been replaced by another listener with the same ID
func (h *Hub) unregisterBackendListener(listener *BackendListener) {
	h.mu.Lock()
	if h.backendListeners[listener.ID] == listener {
		delete(h.backendListeners, listener.ID)
		listener.Close()
		count := len(h.backendListeners)
		h.mu.Unlock()
		log.Printf("Backend listener unregistered: %s (remaining listeners: %d)", listener.ID, count)
	} else {
		h.mu.Unlock()
	}
}

// RegisterBackendListener returns the backend listener registration channel
func (h *Hub) RegisterBackendListener() chan<- *BackendListener {
	return h.registerBackend
}

// UnregisterBackendListener returns the backend listener unregistration channel
func (h *Hub) UnregisterBackendListener() chan<- *BackendListener {
	return h.unregisterBackend
}
