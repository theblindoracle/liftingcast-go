package liftingcast

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

	// Write deadline for sending messages
	writeWait = 10 * time.Second
)

// ErrClientClosed is returned by Connect after Close has been called.
var ErrClientClosed = errors.New("liftingcast: client closed")

// Client represents a WebSocket client connection to the LiftingCast API.
//
// It reconnects on its own whenever a connection drops or times out, until
// Close is called. Each connection gets its own read, ping and timeout
// goroutines, which all stop when that connection ends.
type Client struct {
	// Connection configuration
	baseURL  string
	meetID   string
	password string
	apiKey   string

	// Current WebSocket connection, replaced on each reconnect
	mu   sync.Mutex
	conn *websocket.Conn

	// Channels
	dataUpdate chan json.RawMessage
	errorChan  chan error
	stop       chan struct{} // Closed by Close; ends the client for good
	stopOnce   sync.Once

	// Timings, defaulted from the package constants and shortened in tests
	pingInterval   time.Duration
	messageTimeout time.Duration
	initialBackoff time.Duration

	// Reconnection state. Only one goroutine reconnects at a time: the
	// readPump of the connection that ended, before it starts the next one.
	backoff time.Duration
}

// NewClient creates a new LiftingCast WebSocket client
func NewClient(baseURL, meetID, password, apiKey string) *Client {
	return &Client{
		baseURL:        baseURL,
		meetID:         meetID,
		password:       password,
		apiKey:         apiKey,
		dataUpdate:     make(chan json.RawMessage, 10),
		errorChan:      make(chan error, 10),
		stop:           make(chan struct{}),
		pingInterval:   pingInterval,
		messageTimeout: messageTimeout,
		initialBackoff: initialBackoff,
		backoff:        initialBackoff,
	}
}

// Connect establishes the WebSocket connection
func (c *Client) Connect() error {
	conn, err := c.dial()
	if err != nil {
		return err
	}
	c.start(conn)
	return nil
}

// dial opens a new connection and makes it the current one.
func (c *Client) dial() (*websocket.Conn, error) {
	// Build WebSocket URL with query parameters
	wsURL, err := c.buildURL()
	if err != nil {
		return nil, fmt.Errorf("failed to build URL: %w", err)
	}

	log.Printf("Connecting to LiftingCast API for meet %s", c.meetID)

	// Establish WebSocket connection
	conn, err := c.dialWithRetry(wsURL, 5, time.Second*2)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped() {
		conn.Close()
		return nil, ErrClientClosed
	}
	c.conn = conn
	return conn, nil
}

// start runs the read, ping and timeout goroutines for conn.
func (c *Client) start(conn *websocket.Conn) {
	// done and heartbeat belong to this connection only, so a reconnect
	// starts the ping and timeout goroutines afresh.
	done := make(chan struct{})
	heartbeat := make(chan struct{}, 1)

	go c.readPump(conn, done, heartbeat)
	go c.pingPump(conn, done)
	go c.timeoutMonitor(conn, done, heartbeat)
}

func (c *Client) dialWithRetry(url string, maxRetries int, initialDelay time.Duration) (*websocket.Conn, error) {
	var conn *websocket.Conn
	var err error
	delay := initialDelay
	dialer := &websocket.Dialer{
		ReadBufferSize:  131072, // 128KB buffer
		WriteBufferSize: 131072, // 128KB buffer
	}

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			log.Printf("Retrying connection attempt %d/%d after %v delay...\n", i+1, maxRetries, delay)
			select {
			case <-time.After(delay):
			case <-c.stop:
				return nil, ErrClientClosed
			}
			delay *= 2 // Exponential backoff
		}

		conn, _, err = dialer.Dial(url, nil)
		if err == nil {
			log.Println("Liftingcast connection established successfully")
			return conn, nil
		}

		// Handle specific errors if needed
		log.Printf("Dial error: %v\n", err)
	}

	return nil, fmt.Errorf("after %d attempts, last error: %s", maxRetries, err)
}

// buildURL constructs the WebSocket URL with query parameters
func (c *Client) buildURL() (string, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return "", err
	}

	// Build auth parameter: base64(meetId:password)
	auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", c.meetID, c.password)))

	// Add query parameters
	q := u.Query()
	q.Set("meetId", c.meetID)
	q.Set("auth", auth)
	q.Set("apiKey", c.apiKey)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

// readPump reads messages from one connection. It owns that connection's
// lifetime: when a read fails it ends the connection and reconnects.
func (c *Client) readPump(conn *websocket.Conn, done chan struct{}, heartbeat chan<- struct{}) {
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			// Stop this connection's ping and timeout goroutines before a
			// new connection starts its own.
			conn.Close()
			close(done)

			if c.stopped() {
				return
			}
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			c.reportError(err)
			c.reconnect()
			return
		}

		// Handle different message types
		c.handleMessage(message, heartbeat)
	}
}

// handleMessage processes received messages
func (c *Client) handleMessage(message []byte, heartbeat chan<- struct{}) {
	msgStr := string(message)

	// Check for pong response
	if msgStr == "pong" {
		// Heartbeat response - just log it
		log.Println("Received pong from LiftingCast")
		// Signal heartbeat for timeout monitoring
		signal(heartbeat)
		return
	}

	// Check the message is meet-state JSON, but pass on the raw bytes so the
	// cache can tell which fields it left out
	var meetData MeetApiResponse
	if err := json.Unmarshal(message, &meetData); err != nil {
		// Not valid JSON - treat as error message
		log.Printf("error unmarshalling Liftingcast message: %s", err)
		c.reportError(fmt.Errorf("server error: %s", msgStr))
		return
	}

	// Valid meet data update
	select {
	case c.dataUpdate <- json.RawMessage(message):
	case <-c.stop:
		return
	}

	// Signal heartbeat for timeout monitoring (non-blocking)
	signal(heartbeat)
}

func signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// pingPump sends periodic ping messages on one connection
func (c *Client) pingPump(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sendPing(conn); err != nil {
				// The failed write breaks the connection; readPump reconnects.
				log.Printf("Failed to send ping: %v", err)
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
// messageTimeout. Closing it makes readPump fail and reconnect.
func (c *Client) timeoutMonitor(conn *websocket.Conn, done <-chan struct{}, heartbeat <-chan struct{}) {
	timer := time.NewTimer(c.messageTimeout)
	defer timer.Stop()

	for {
		select {
		case <-heartbeat:
			timer.Reset(c.messageTimeout)
		case <-timer.C:
			log.Println("Connection timeout - no messages received")
			c.reportError(fmt.Errorf("connection timeout"))
			conn.Close()
			return
		case <-done:
			return
		}
	}
}

// reconnect retries Connect with exponential backoff until it succeeds or
// the client is closed.
func (c *Client) reconnect() {
	for {
		log.Printf("Attempting to reconnect in %v", c.backoff)

		select {
		case <-time.After(c.backoff):
		case <-c.stop:
			log.Println("Reconnection stopped")
			return
		}

		// Double the backoff for next attempt
		c.backoff = c.backoff * 2

		conn, err := c.dial()
		if err != nil {
			if errors.Is(err, ErrClientClosed) {
				return
			}
			log.Printf("Reconnection failed: %v", err)
			continue
		}

		// Reset backoff before the new connection's readPump can start
		// reconnecting on its own
		c.backoff = c.initialBackoff
		log.Println("Reconnected successfully")
		c.start(conn)
		return
	}
}

// reportError sends err to Errors, dropping it if nobody is reading, so an
// undrained Errors channel never stalls reconnection.
func (c *Client) reportError(err error) {
	select {
	case c.errorChan <- err:
	default:
		log.Printf("Dropping LiftingCast error, Errors() is full: %v", err)
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

// DataUpdate returns the channel for receiving meet data updates, each the
// raw JSON of one message. Pass them to Cache.Merge to build the full state.
func (c *Client) DataUpdate() <-chan json.RawMessage {
	return c.dataUpdate
}

// Errors returns the channel for receiving errors
func (c *Client) Errors() <-chan error {
	return c.errorChan
}

// Close closes the WebSocket connection and stops reconnecting. The client
// cannot be reused afterwards.
func (c *Client) Close() {
	c.stopOnce.Do(func() { close(c.stop) })

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
	}
}
