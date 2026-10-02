package liftingcast

import (
	"sync"
	"time"
)

// HubStatus is the connection status of a Hub's upstream connection (see
// ClientStatus) plus the meet it is following. Its JSON form is meant to be
// served as is, for example from a status endpoint. Without an upstream
// connection it is the zero HubStatus.
type HubStatus struct {
	Connected bool   `json:"connected"`
	Rejected  bool   `json:"rejected"`
	MeetID    string `json:"meetId"`
	MeetName  string `json:"meetName"`
	LastError string `json:"error,omitempty"`

	// LastMeetStateAt is the receive time of the last meet state merged
	// from the upstream connection, or zero if there has been none yet.
	// Reconnects leave it as it is; Connect and Disconnect reset it.
	LastMeetStateAt time.Time `json:"lastMeetStateAt,omitzero"`
}

// Update is one meet state handed to a listener, with the connection it
// arrived on and when it was received.
type Update struct {
	// State is the full merged meet state, shared with every listener: see
	// Listen.
	State *MeetState

	// Connection numbers the upstream connection the state arrived on,
	// across every connection the Hub has made. It never repeats or goes
	// backwards, and rises on every reconnect: a redial after a drop, and
	// the first connection after Connect. Any change means a gap, however
	// short, in which states may have been missed.
	Connection uint64

	// ReceivedAt is when the message that produced State was received
	// (see Message). The current state handed to a new listener keeps its
	// original ReceivedAt.
	ReceivedAt time.Time
}

// Hub keeps one upstream LiftingCast connection alive, merges its messages
// into a Cache, and hands each merged state to in-process listeners added
// with Listen. It starts with no upstream connection: Connect starts one and
// Disconnect stops it. It runs its own loop from NewHub until Close.
type Hub struct {
	opts options

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

	// Single persistent upstream LiftingCast connection, its meet and the
	// last update handed out from it, the zero Update before the first.
	// Only the loop changes them, under upstreamMu, so Status can read
	// them.
	upstreamMu     sync.RWMutex
	upstreamClient *Client
	meetID         string
	latest         Update

	// Turn the current client's connection numbers into the Hub's, which
	// only go up: connectionBase is added to them, and lastConnection is
	// the highest the Hub has handed out. Only the loop uses them.
	connectionBase uint64
	lastConnection uint64

	// Carries Connect's and Disconnect's requests to the loop
	connect chan connectRequest

	// tuneClient, if set, adjusts each new client before it starts. Tests
	// use it to shorten the client's timings.
	tuneClient func(*Client)

	// closing is closed by the first Close, which then waits for the loop to
	// shut down and close closed. The loop leaves the listeners it stopped
	// in final, for Close to wait for.
	closeOnce sync.Once
	closing   chan struct{}
	closed    chan struct{}
	final     []*listener
}

// connectRequest asks the loop to replace the upstream connection with one
// for cfg, or with none if cfg is nil, and is answered on done once it has
type connectRequest struct {
	cfg  *Config
	done chan struct{}
}

// listenRequest asks the loop to add a listener, and is answered on
// registered once it has
type listenRequest struct {
	listener   *listener
	registered chan struct{}
}

// NewHub creates a hub with no upstream connection and starts its loop. Call
// Connect to follow a meet, and Close when done with the hub.
func NewHub(opts ...Option) *Hub {
	hub := &Hub{
		opts:      newOptions(opts),
		upstream:  make(chan upstreamMessage, 10),
		listeners: make(map[*listener]struct{}),
		listen:    make(chan listenRequest),
		cache:     NewCache(),
		connect:   make(chan connectRequest),
		closing:   make(chan struct{}),
		closed:    make(chan struct{}),
	}
	go hub.run()
	return hub
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
		case req := <-h.connect:
			h.handleConnect(req.cfg)
			close(req.done)
		case <-h.closing:
			h.shutdown()
			return
		}
	}
}

// Listen calls handler with every meet state from now on, in order, until the
// returned stop is called or the Hub is closed. The first state is the
// current meet state if there is one, with its original Connection and
// ReceivedAt; after that no state is skipped, repeated or older than the one
// before. Listen returns once the listener is in place, so every state
// merged after it returns reaches handler. A change in Update.Connection
// from one state to the next means there was a reconnect between them.
//
// Every listener is handed the same state, concurrently, so handler must
// treat it as read-only. The Hub never modifies a state once it has handed
// it out, so handler may keep it, for example to compare with the next. A
// handler that needs to modify the state must copy it first.
//
// Each listener has its own unbounded queue and goroutine: handler is called
// with one state at a time, and a slow handler delays only its own listener.
//
// stop may be called any number of times, from any goroutine. Once it
// returns, handler is not running and will not be called again, so whatever
// handler uses can be torn down. Called from inside any listener's handler,
// stop only makes sure no further call starts and does not wait for one in
// progress, since a handler waiting for itself, or two handlers waiting for
// each other, would never return. After Close, Listen never calls handler
// and returns a stop that does nothing.
func (h *Hub) Listen(handler func(Update)) (stop func()) {
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

// Connect replaces the upstream connection, if there is one, with a new one
// for the meet and credentials in cfg, and clears the meet state. Listeners
// stay in place and receive the new meet's states. It returns once the new
// connection has been started, so Status reports the new meet. After Close
// it does nothing.
func (h *Hub) Connect(cfg Config) {
	h.requestConnect(&cfg)
}

// Disconnect stops the upstream connection, if there is one, and clears the
// meet state. Listeners stay in place for the next Connect. It returns once
// the connection has been stopped. After Close it does nothing.
func (h *Hub) Disconnect() {
	h.requestConnect(nil)
}

// requestConnect hands cfg to the loop and waits until the loop has acted
// on it, unless the hub is closing
func (h *Hub) requestConnect(cfg *Config) {
	req := connectRequest{cfg: cfg, done: make(chan struct{})}
	select {
	case h.connect <- req:
	case <-h.closing:
		return
	}
	// The loop always answers a request it has taken
	<-req.done
}

// Close closes the upstream connection and stops every listener, with the
// same guarantee as each listener's stop: once Close returns, no handler is
// running or will be called again. Called from inside a handler, it only
// makes sure no further handler call starts, as stop does. Close may be
// called any number of times. Afterwards the hub stays closed: see Listen,
// Connect, Disconnect and Status.
func (h *Hub) Close() {
	h.closeOnce.Do(func() { close(h.closing) })
	<-h.closed
	for _, l := range h.final {
		l.wait()
	}
}

// shutdown closes the upstream connection, forgets the meet and signals
// every listener to stop, as the loop's last act. It leaves waiting for
// their handlers to Close, so the loop never blocks on a handler.
func (h *Hub) shutdown() {
	if h.upstreamClient != nil {
		h.upstreamClient.Close()
	}
	h.upstreamMu.Lock()
	h.upstreamClient = nil
	h.meetID = ""
	h.latest = Update{}
	h.upstreamMu.Unlock()
	h.cache.Clear()

	h.mu.Lock()
	for l := range h.listeners {
		l.signalStop()
		h.final = append(h.final, l)
	}
	h.listeners = nil
	h.mu.Unlock()
	h.opts.logger.Info("hub closed")
}

// Status returns the current connection status. After Close it returns
// the zero HubStatus.
func (h *Hub) Status() HubStatus {
	h.upstreamMu.RLock()
	client, meetID, lastMeetStateAt := h.upstreamClient, h.meetID, h.latest.ReceivedAt
	h.upstreamMu.RUnlock()

	var clientStatus ClientStatus
	if client != nil {
		clientStatus = client.Status()
	}
	status := HubStatus{
		Connected:       clientStatus.Connected,
		Rejected:        clientStatus.Rejected,
		MeetID:          meetID,
		LastError:       clientStatus.LastError,
		LastMeetStateAt: lastMeetStateAt,
	}

	// Try to get meet name from cache
	if cachedData := h.cache.Get(); cachedData != nil {
		status.MeetName = cachedData.Name
	}

	return status
}

// handleConnect replaces the upstream connection with one for cfg, or with
// none if cfg is nil
func (h *Hub) handleConnect(cfg *Config) {
	// Stop existing upstream connection if present
	if h.upstreamClient != nil {
		h.opts.logger.Info("stopping upstream connection", "meetID", h.meetID)
		h.upstreamClient.Close()
		h.upstreamMu.Lock()
		h.upstreamClient = nil
		h.upstreamMu.Unlock()
	}

	// Clear cache and the last update
	h.cache.Clear()
	h.upstreamMu.Lock()
	h.latest = Update{}
	h.upstreamMu.Unlock()

	// The next client's connections number on from the highest so far
	h.connectionBase = h.lastConnection

	// Disconnect leaves the hub without an upstream connection
	if cfg == nil {
		h.upstreamMu.Lock()
		h.meetID = ""
		h.upstreamMu.Unlock()
		return
	}

	client := h.startClient(*cfg)
	h.upstreamMu.Lock()
	h.upstreamClient = client
	h.meetID = cfg.MeetID
	h.upstreamMu.Unlock()
}

// startClient starts a new upstream connection and forwards its messages to
// the loop until it is closed or the hub has closed.
func (h *Hub) startClient(cfg Config) *Client {
	client := NewClient(cfg, WithLogger(h.opts.logger))
	if h.tuneClient != nil {
		h.tuneClient(client)
	}
	client.Start()
	go func() {
		for msg := range client.Messages() {
			select {
			case h.upstream <- upstreamMessage{client: client, msg: msg}:
			case <-h.closed:
				return
			}
		}
	}()
	return client
}

// upstreamMessage is one message and the client it arrived on
type upstreamMessage struct {
	client *Client
	msg    Message
}

// handleUpstreamMessage merges a message into the cache and queues the
// merged state for every listener
func (h *Hub) handleUpstreamMessage(msg upstreamMessage) {
	// Drop messages still in flight from a client that Connect or
	// Disconnect has since replaced, so they can't seed the cleared cache
	if msg.client != h.upstreamClient {
		return
	}

	merged, err := h.cache.Merge(msg.msg.Raw)
	if err != nil {
		h.opts.logger.Warn("failed to merge meet state", "meetID", h.meetID, "err", err)
		return
	}

	// A client's connection numbers only go up, so the Hub's do too
	h.lastConnection = h.connectionBase + msg.msg.Connection
	update := Update{
		State:      merged,
		Connection: h.lastConnection,
		ReceivedAt: msg.msg.ReceivedAt,
	}
	h.upstreamMu.Lock()
	h.latest = update
	h.upstreamMu.Unlock()

	h.broadcast(update)
}

// broadcast queues an update for every listener
func (h *Hub) broadcast(update Update) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for l := range h.listeners {
		l.enqueue(update)
	}
}

// addListener starts delivering to a new listener, beginning with the last
// update if there is one. Only the loop calls it, so no merge can land
// between reading the last update and the listener joining the next
// broadcast.
func (h *Hub) addListener(l *listener) {
	if h.latest.State != nil {
		l.enqueue(h.latest)
	}
	l.start()

	h.mu.Lock()
	h.listeners[l] = struct{}{}
	count := len(h.listeners)
	h.mu.Unlock()

	h.opts.logger.Debug("listener added", "listeners", count)
}
