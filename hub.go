package liftingcast

import (
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

// ConnectionStatus represents the current status of the upstream connection
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

	// broadcast carries merged states from the upstream connection to Run
	broadcast chan *MeetApiResponse

	// Backend listeners (in-process subscribers)
	backendListeners  map[string]*BackendListener
	registerBackend   chan *BackendListener
	unregisterBackend chan *BackendListener

	// Shared cache for meet data
	cache *Cache

	// Single persistent upstream LiftingCast connection
	upstreamClient *Client
	upstreamDone   chan struct{} // Signals to stop upstream connection goroutine

	// Reconnection channel
	reconnect chan *ReconnectRequest

	// LiftingCast connection configuration
	baseURL  string
	meetID   string
	password string
	apiKey   string

	// Connection status
	statusMu  sync.RWMutex
	connected bool
	lastError string
}

// NewHub creates a new hub instance with an upstream connection
func NewHub(baseURL, meetID, password, apiKey string) *Hub {
	hub := NewIdleHub()
	hub.baseURL = baseURL
	hub.meetID = meetID
	hub.password = password
	hub.apiKey = apiKey

	// Create and start the persistent upstream connection
	hub.upstreamClient = NewClient(baseURL, meetID, password, apiKey)
	go hub.maintainUpstreamConnection(hub.upstreamClient, hub.upstreamDone)

	return hub
}

// NewIdleHub creates a hub without an upstream connection (idle mode)
func NewIdleHub() *Hub {
	return &Hub{
		broadcast:         make(chan *MeetApiResponse, 10),
		backendListeners:  make(map[string]*BackendListener),
		registerBackend:   make(chan *BackendListener),
		unregisterBackend: make(chan *BackendListener),
		cache:             NewCache(),
		upstreamDone:      make(chan struct{}),
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
		case meetData := <-h.broadcast:
			h.broadcastToBackendListeners(meetData)
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
	h.statusMu.RLock()
	defer h.statusMu.RUnlock()

	status := ConnectionStatus{
		Connected: h.connected,
		MeetID:    h.meetID,
		Error:     h.lastError,
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

		// Signal maintainUpstreamConnection to stop before closing the
		// client, so it doesn't report the shutdown as an error
		close(h.upstreamDone)
		h.upstreamClient.Close()
		h.upstreamClient = nil
	}

	// Clear cache
	h.cache.Clear()

	// Update status
	h.statusMu.Lock()
	h.connected = false
	h.lastError = ""
	h.statusMu.Unlock()

	// If nil request, stay in idle mode
	if req == nil {
		log.Println("Entering idle mode (no upstream connection)")
		h.statusMu.Lock()
		h.meetID = ""
		h.baseURL = ""
		h.password = ""
		h.apiKey = ""
		h.statusMu.Unlock()
		return
	}

	// Update configuration
	h.statusMu.Lock()
	h.baseURL = req.BaseURL
	h.meetID = req.MeetID
	h.password = req.Password
	h.apiKey = req.APIKey
	h.statusMu.Unlock()

	// Create new done channel
	h.upstreamDone = make(chan struct{})

	// Create new client and start connection
	h.upstreamClient = NewClient(req.BaseURL, req.MeetID, req.Password, req.APIKey)
	go h.maintainUpstreamConnection(h.upstreamClient, h.upstreamDone)

	log.Printf("Reconnection initiated for meet: %s", req.MeetID)
}

// maintainUpstreamConnection manages the persistent LiftingCast connection.
// client and done are passed in because handleReconnect replaces the hub's
// fields while this goroutine may still be running.
func (h *Hub) maintainUpstreamConnection(client *Client, done <-chan struct{}) {
	// Connect to LiftingCast API, retrying until it succeeds or the client
	// is closed
	if err := client.Connect(); err != nil {
		log.Printf("Failed to connect to LiftingCast: %v", err)
		h.statusMu.Lock()
		h.lastError = err.Error()
		h.statusMu.Unlock()

		client.reconnect()
		if client.stopped() {
			return
		}
	}

	// Update status to connected
	h.statusMu.Lock()
	h.connected = true
	h.lastError = ""
	h.statusMu.Unlock()

	// Handle messages from upstream LiftingCast connection
	for {
		select {
		case <-done:
			log.Println("Upstream connection stopped")
			return
		case meetData := <-client.DataUpdate():
			// Merge with cache
			merged, err := h.cache.Merge(meetData)
			if err != nil {
				log.Printf("Failed to merge data: %v", err)
				continue
			}

			h.statusMu.Lock()
			h.connected = true
			h.statusMu.Unlock()

			select {
			case h.broadcast <- merged:
			case <-done:
				return
			}

		case err := <-client.Errors():
			// Most errors mean the connection dropped and the client is
			// reconnecting; the next message marks the hub connected again.
			log.Printf("LiftingCast upstream error: %v", err)
			h.statusMu.Lock()
			h.connected = false
			h.lastError = err.Error()
			h.statusMu.Unlock()
		}
	}
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
