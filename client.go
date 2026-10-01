package liftingcast

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Heartbeat ping interval
	pingInterval = 30 * time.Second

	// Connection timeout - if no message received in this time, reconnect
	messageTimeout = 60 * time.Second

	// Initial backoff for reconnection
	initialBackoff = 2 * time.Second

	// Longest wait between reconnect attempts
	maxBackoff = 30 * time.Second

	// Longest a dial may take, from TCP connect to WebSocket handshake
	handshakeTimeout = 10 * time.Second

	// Write deadline for sending messages
	writeWait = 10 * time.Second
)

// errClientClosed is returned by dial after Close has been called.
var errClientClosed = errors.New("liftingcast: client closed")

// rejectedError is the error that rejected the client: a handshake refused
// as unauthorized or forbidden, or a server error known to be permanent.
type rejectedError struct{ error }

func isRejected(err error) bool {
	var rejected rejectedError
	return errors.As(err, &rejected)
}

// permanentServerErrors are the server errors that reject the client, exactly
// as LiftingCast words them (see ADR-0003). Any other server error is
// temporary.
var permanentServerErrors = map[string]bool{
	"Error: invalid api key": true,
	"Error: unauthorized - meet id or password in auth param are incorrect": true,
}

// ClientStatus is the connection status of a Client: whether it is connected
// right now, whether it has been rejected, and the most recent error it saw.
// Connected means LiftingCast accepted the connection, not that the
// credentials are known to be good. A rejected client stays rejected and
// never reconnects; replace it.
type ClientStatus struct {
	Connected bool
	Rejected  bool
	LastError string
}

// Client is the upstream connection to LiftingCast for one meet.
//
// Once started it dials, and redials with backoff whenever a dial fails or a
// connection drops or times out, until Close is called or LiftingCast
// rejects it. Each connection gets its own ping and timeout goroutines, which
// stop when that connection ends.
type Client struct {
	cfg Config
	log *slog.Logger

	// Current WebSocket connection, replaced on each reconnect, and whether
	// Start has run
	mu      sync.Mutex
	conn    *websocket.Conn
	started bool

	// messages carries meet state. Only run sends on it, and closes it when
	// it exits; Close closes it if run never started.
	messages chan json.RawMessage
	stop     chan struct{} // Closed by Close; ends the client for good

	// Connection status, set by run as it sees each change
	statusMu sync.Mutex
	status   ClientStatus

	// Timings, defaulted from the package constants and shortened in tests
	pingInterval     time.Duration
	messageTimeout   time.Duration
	initialBackoff   time.Duration
	maxBackoff       time.Duration
	handshakeTimeout time.Duration

	// Wait before the next dial, only used by run
	backoff time.Duration
}

// NewClient creates a LiftingCast WebSocket client for the meet and
// credentials in cfg. It does not connect until Start is called.
func NewClient(cfg Config, opts ...Option) *Client {
	o := newOptions(opts)
	return &Client{
		cfg:              cfg,
		log:              o.logger.With("meetID", cfg.MeetID),
		messages:         make(chan json.RawMessage, 10),
		stop:             make(chan struct{}),
		pingInterval:     pingInterval,
		messageTimeout:   messageTimeout,
		initialBackoff:   initialBackoff,
		maxBackoff:       maxBackoff,
		handshakeTimeout: handshakeTimeout,
	}
}

// Start begins connecting, and keeps the client connected until Close. It
// does nothing if the client is already started or closed.
func (c *Client) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.stopped() {
		return
	}
	c.started = true
	go c.run()
}

// run dials, serves each connection until it ends, and waits out the backoff
// before dialling again, until Close. Once rejected it stops dialling and
// waits for Close.
func (c *Client) run() {
	defer close(c.messages)
	c.backoff = c.initialBackoff
	for {
		conn, err := c.dial()
		if errors.Is(err, errClientClosed) {
			return
		}
		if err != nil {
			c.log.Warn("failed to connect to LiftingCast", "err", err)
		} else {
			c.setStatus(ClientStatus{Connected: true})
			var resetBackoff bool
			resetBackoff, err = c.serve(conn)
			if err == nil {
				return
			}
			// Being accepted, or getting the meet state every new connection
			// gets, isn't enough to reset the backoff: two clients taking the
			// one connection Hosted LiftingCast allows from each other would
			// never slow down.
			if resetBackoff {
				c.backoff = c.initialBackoff
			}
			c.log.Warn("LiftingCast connection dropped", "err", err)
		}
		if isRejected(err) {
			c.setStatus(ClientStatus{Rejected: true, LastError: err.Error()})
			<-c.stop
			return
		}
		c.setStatus(ClientStatus{LastError: err.Error()})

		c.log.Info("reconnecting to LiftingCast", "after", c.backoff)
		select {
		case <-time.After(c.backoff):
		case <-c.stop:
			return
		}
		c.backoff = min(c.backoff*2, c.maxBackoff)
	}
}

// dial makes one attempt to open a new connection and make it the current
// one. Close abandons a pending dial.
func (c *Client) dial() (*websocket.Conn, error) {
	// Build WebSocket URL with query parameters
	wsURL, err := c.buildURL()
	if err != nil {
		return nil, fmt.Errorf("failed to build URL: %w", err)
	}

	c.log.Info("connecting to LiftingCast")

	// gorilla/websocket stops watching ctx once the TCP connection is up, so
	// Close interrupts the handshake by closing that connection itself.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var netMu sync.Mutex
	var netConn net.Conn
	go func() {
		select {
		case <-c.stop:
			cancel()
			netMu.Lock()
			if netConn != nil {
				netConn.Close()
			}
			netMu.Unlock()
		case <-ctx.Done():
		}
	}()

	dialer := &websocket.Dialer{
		ReadBufferSize:   131072, // 128KB buffer
		WriteBufferSize:  131072, // 128KB buffer
		HandshakeTimeout: c.handshakeTimeout,
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			// Either the stop watcher sees netConn, or this sees the stop
			netMu.Lock()
			defer netMu.Unlock()
			if c.stopped() {
				conn.Close()
				return nil, errClientClosed
			}
			netConn = conn
			return conn, nil
		},
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		if c.stopped() {
			return nil, errClientClosed
		}
		if resp != nil && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) {
			return nil, rejectedError{fmt.Errorf("handshake refused: %s", resp.Status)}
		}
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	c.log.Info("connected to LiftingCast")

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped() {
		conn.Close()
		return nil, errClientClosed
	}
	c.conn = conn
	return conn, nil
}

// buildURL constructs the WebSocket URL with query parameters
func (c *Client) buildURL() (string, error) {
	u, err := url.Parse(c.cfg.BaseURL)
	if err != nil {
		return "", err
	}

	// Build auth parameter: base64(meetId:password)
	auth := base64.StdEncoding.EncodeToString([]byte(c.cfg.MeetID + ":" + c.cfg.Password))

	// Add query parameters
	q := u.Query()
	q.Set("meetId", c.cfg.MeetID)
	q.Set("auth", auth)
	q.Set("apiKey", c.cfg.APIKey)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// serve reads from conn until it fails, alongside conn's ping and timeout
// goroutines, which have stopped by the time it returns. It returns why the
// connection ended, or nil if the client was closed: the last server error if
// there was one, since LiftingCast drops the connection right after sending
// it. A server error known to be permanent ends the connection at once. It
// also reports whether the backoff should reset: the connection delivered
// meet state and did not end with a server error.
func (c *Client) serve(conn *websocket.Conn) (resetBackoff bool, err error) {
	done := make(chan struct{})
	heartbeat := make(chan struct{}, 1)
	var wg sync.WaitGroup
	var timedOut bool
	wg.Add(2)
	go func() {
		defer wg.Done()
		c.pingPump(conn, done)
	}()
	go func() {
		defer wg.Done()
		timedOut = c.timeoutMonitor(conn, done, heartbeat)
	}()

	var gotMeetState bool
	var serverErr error
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			conn.Close()
			close(done)
			wg.Wait()

			resetBackoff := gotMeetState && serverErr == nil
			switch {
			case c.stopped():
				return resetBackoff, nil
			case serverErr != nil:
				return resetBackoff, serverErr
			case timedOut:
				return resetBackoff, errors.New("connection timeout")
			}
			return resetBackoff, err
		}

		meetState, err := c.handleMessage(message, heartbeat)
		if meetState {
			// The connection survived the server error, so it isn't why the
			// connection ends
			gotMeetState = true
			serverErr = nil
		}
		if err != nil {
			serverErr = err
			if isRejected(err) {
				conn.Close()
			}
		}
	}
}

// handleMessage processes received messages. It reports whether message was
// meet state, or returns the server error if it was one, as a rejectedError
// if it is known to be permanent.
func (c *Client) handleMessage(message []byte, heartbeat chan<- struct{}) (meetState bool, err error) {
	msgStr := string(message)

	// Check for pong response
	if msgStr == "pong" {
		// Signal heartbeat for timeout monitoring
		signal(heartbeat)
		return false, nil
	}

	// Check the message is meet-state JSON, but pass on the raw bytes so the
	// cache can tell which fields it left out
	var meetData *MeetApiResponse
	err = json.Unmarshal(message, &meetData)
	if err == nil && meetData == nil {
		err = errors.New("message is null")
	}
	if err != nil {
		// Not meet-state JSON - treat as error message
		c.log.Warn("LiftingCast sent a server error", "message", msgStr)
		err = fmt.Errorf("server error: %s", msgStr)
		if permanentServerErrors[msgStr] {
			err = rejectedError{err}
		}
		c.setLastError(err.Error())
		return false, err
	}

	// Valid meet state
	select {
	case c.messages <- json.RawMessage(message):
	case <-c.stop:
		return false, nil
	}

	// Signal heartbeat for timeout monitoring (non-blocking)
	signal(heartbeat)
	return true, nil
}

// pingPump sends periodic ping messages on one connection
func (c *Client) pingPump(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sendPing(conn); err != nil {
				// The failed write breaks the connection; serve sees it.
				c.log.Warn("failed to send ping", "err", err)
				return
			}
		case <-done:
			return
		}
	}
}

// sendPing sends a ping message to the server
func sendPing(conn *websocket.Conn) error {
	conn.SetWriteDeadline(time.Now().Add(writeWait))
	return conn.WriteMessage(websocket.TextMessage, []byte("ping"))
}

// timeoutMonitor closes one connection if nothing arrives on it for
// messageTimeout, and reports whether it did. Closing it makes serve's read
// fail.
func (c *Client) timeoutMonitor(conn *websocket.Conn, done <-chan struct{}, heartbeat <-chan struct{}) bool {
	timer := time.NewTimer(c.messageTimeout)
	defer timer.Stop()

	for {
		select {
		case <-heartbeat:
			timer.Reset(c.messageTimeout)
		case <-timer.C:
			c.log.Warn("LiftingCast connection timed out", "after", c.messageTimeout)
			conn.Close()
			return true
		case <-done:
			return false
		}
	}
}

// setStatus records status, unless the client has been closed: Close clears
// the status, and nothing seen afterwards may overwrite that.
func (c *Client) setStatus(status ClientStatus) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	if !c.stopped() {
		c.status = status
	}
}

// setLastError records err without changing whether the client is connected,
// unless the client has been closed.
func (c *Client) setLastError(err string) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	if !c.stopped() {
		c.status.LastError = err
	}
}

func (c *Client) stopped() bool {
	select {
	case <-c.stop:
		return true
	default:
		return false
	}
}

// Status returns the client's connection status right now, even if meet
// states sent before a change are still unread on Messages.
func (c *Client) Status() ClientStatus {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	return c.status
}

// Messages returns the channel of meet state, each the raw JSON of one
// message. Pass them to Cache.Merge to build the full state. It is closed
// once the client is closed and nothing more can be sent on it.
func (c *Client) Messages() <-chan json.RawMessage {
	return c.messages
}

// Close closes the WebSocket connection and stops reconnecting. The client
// cannot be reused afterwards.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped() {
		return
	}
	close(c.stop)
	c.statusMu.Lock()
	c.status = ClientStatus{}
	c.statusMu.Unlock()
	if c.conn != nil {
		c.conn.Close()
	}
	if !c.started {
		close(c.messages)
	}
}
