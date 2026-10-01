package liftingcast

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeServer stands in for LiftingCast. Each accepted connection is handed to
// the test on conns, and the test decides what the server does with it.
// While down is set, every connection attempt is refused. Each attempt's time
// is recorded on attempts.
type fakeServer struct {
	url      string
	conns    chan *websocket.Conn
	attempts chan time.Time
	down     atomic.Bool
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	s := &fakeServer{
		conns:    make(chan *websocket.Conn, 16),
		attempts: make(chan time.Time, 256),
	}
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case s.attempts <- time.Now():
		default:
		}
		if s.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		s.conns <- conn
	}))
	t.Cleanup(srv.Close)
	s.url = "ws" + strings.TrimPrefix(srv.URL, "http")
	return s
}

// drainAttempts discards the connection attempts recorded so far.
func (s *fakeServer) drainAttempts() {
	for {
		select {
		case <-s.attempts:
		default:
			return
		}
	}
}

func (s *fakeServer) nextAttempt(t *testing.T, within time.Duration) time.Time {
	t.Helper()
	select {
	case at := <-s.attempts:
		return at
	case <-time.After(within):
		t.Fatalf("client did not try to connect within %v", within)
		return time.Time{}
	}
}

func (s *fakeServer) accept(t *testing.T, within time.Duration) *websocket.Conn {
	t.Helper()
	select {
	case conn := <-s.conns:
		t.Cleanup(func() { conn.Close() })
		return conn
	case <-time.After(within):
		t.Fatalf("client did not connect within %v", within)
		return nil
	}
}

// newIdleTestClient returns a client with shortened timings that has not yet
// started. Tests may adjust its timings before calling Start.
func newIdleTestClient(t *testing.T, url string, ping, timeout time.Duration) *Client {
	t.Helper()
	c := NewClient(url, "meet", "password", "key")
	c.pingInterval = ping
	c.messageTimeout = timeout
	c.initialBackoff = 10 * time.Millisecond
	c.maxBackoff = time.Second
	c.handshakeTimeout = time.Second
	t.Cleanup(c.Close)
	return c
}

func newTestClient(t *testing.T, url string, ping, timeout time.Duration) *Client {
	t.Helper()
	c := newIdleTestClient(t, url, ping, timeout)
	c.Start()
	return c
}

// waitForStatus waits until the client's status satisfies ok.
func waitForStatus(t *testing.T, c *Client, ok func(ClientStatus) bool) ClientStatus {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		status := c.Status()
		if ok(status) {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("status stayed %+v", status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestClientRetriesFailedFirstDial(t *testing.T) {
	srv := newFakeServer(t)
	srv.down.Store(true)
	c := newTestClient(t, srv.url, time.Hour, time.Hour)

	srv.nextAttempt(t, time.Second)
	status := waitForStatus(t, c, func(s ClientStatus) bool { return s.LastError != "" })
	if status.Connected {
		t.Errorf("status = %+v after a failed dial, want disconnected", status)
	}

	srv.down.Store(false)
	srv.accept(t, time.Second)
	status = waitForStatus(t, c, func(s ClientStatus) bool { return s.Connected })
	if status.LastError != "" {
		t.Errorf("status = %+v after connecting, want LastError cleared", status)
	}
}

func TestClientSendsHeartbeatsAfterReconnect(t *testing.T) {
	srv := newFakeServer(t)
	newTestClient(t, srv.url, 20*time.Millisecond, 10*time.Second)

	srv.accept(t, time.Second).Close()
	second := srv.accept(t, time.Second)

	second.SetReadDeadline(time.Now().Add(time.Second))
	for {
		_, msg, err := second.ReadMessage()
		if err != nil {
			t.Fatalf("no heartbeat on the reconnected connection: %v", err)
		}
		if string(msg) == "ping" {
			return
		}
	}
}

func TestClientDetectsTimeoutAfterReconnect(t *testing.T) {
	srv := newFakeServer(t)
	newTestClient(t, srv.url, time.Hour, 50*time.Millisecond)

	// Drop the first connection, then stay silent on every later one. Each
	// silent connection must time out and be replaced.
	srv.accept(t, time.Second).Close()
	for i := 0; i < 3; i++ {
		srv.accept(t, time.Second)
	}
}

func TestClientReconnectBackoffStopsAtCap(t *testing.T) {
	srv := newFakeServer(t)
	c := newIdleTestClient(t, srv.url, time.Hour, time.Hour)
	c.maxBackoff = 40 * time.Millisecond
	c.Start()
	conn := srv.accept(t, time.Second)

	srv.down.Store(true)
	srv.drainAttempts()
	conn.Close()

	// Uncapped, the gaps would reach 10ms·2⁹ ≈ 5s.
	prev := srv.nextAttempt(t, time.Second)
	for i := 0; i < 10; i++ {
		at := srv.nextAttempt(t, time.Second)
		if gap := at.Sub(prev); gap > 3*c.maxBackoff {
			t.Fatalf("gap before attempt %d = %v, want at most about %v", i+2, gap, c.maxBackoff)
		}
		prev = at
	}
}

func TestClientReconnectsPromptlyAfterLongOutage(t *testing.T) {
	srv := newFakeServer(t)
	c := newIdleTestClient(t, srv.url, time.Hour, time.Hour)
	c.maxBackoff = 50 * time.Millisecond
	c.Start()
	conn := srv.accept(t, time.Second)

	srv.down.Store(true)
	conn.Close()
	time.Sleep(time.Second)
	srv.down.Store(false)

	srv.accept(t, c.maxBackoff+150*time.Millisecond)
}

// silentListener accepts TCP connections but never answers the WebSocket
// handshake. Each accepted connection is sent on the returned channel.
func silentListener(t *testing.T) (string, <-chan net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	accepted := make(chan net.Conn, 16)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
			select {
			case accepted <- conn:
			default:
			}
		}
	}()
	return "ws://" + ln.Addr().String(), accepted
}

func TestClientDialFailsAtHandshakeTimeout(t *testing.T) {
	url, _ := silentListener(t)
	c := newIdleTestClient(t, url, time.Hour, time.Hour)
	c.handshakeTimeout = 50 * time.Millisecond
	c.initialBackoff = time.Hour

	start := time.Now()
	c.Start()
	waitForStatus(t, c, func(s ClientStatus) bool { return s.LastError != "" })
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("dial failed after %v, want about %v", elapsed, c.handshakeTimeout)
	}
}

// waitForClosed waits for Messages to close, discarding anything left on it.
func waitForClosed(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-c.Messages():
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("Messages did not close after Close")
		}
	}
}

func TestClientCloseDuringBackoffEndsReconnection(t *testing.T) {
	srv := newFakeServer(t)
	c := newIdleTestClient(t, srv.url, time.Hour, time.Hour)
	c.initialBackoff = 200 * time.Millisecond
	c.Start()
	conn := srv.accept(t, time.Second)
	srv.drainAttempts()

	conn.Close()
	time.Sleep(50 * time.Millisecond) // the client is now waiting out its backoff
	c.Close()
	waitForClosed(t, c)

	select {
	case <-srv.attempts:
		t.Fatal("client tried to reconnect after Close")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestClientCloseDuringDialEndsReconnection(t *testing.T) {
	url, accepted := silentListener(t)
	c := newIdleTestClient(t, url, time.Hour, time.Hour)
	c.handshakeTimeout = time.Hour
	c.Start()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("client did not dial")
	}

	c.Close()
	waitForClosed(t, c)
	if status := c.Status(); status.Connected {
		t.Errorf("status = %+v after Close during a dial, want disconnected", status)
	}
}

func TestClientCloseWhileStreamingClosesMessages(t *testing.T) {
	srv := newFakeServer(t)
	c := newTestClient(t, srv.url, time.Hour, time.Hour)
	conn := srv.accept(t, time.Second)

	// Fill Messages so the client is blocked handing on a state when it is
	// closed.
	go func() {
		for {
			if conn.WriteMessage(websocket.TextMessage, []byte(`{"name": "Test Meet"}`)) != nil {
				return
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	c.Close()
	waitForClosed(t, c)
	if status := c.Status(); status.Connected {
		t.Errorf("status = %+v after Close, want disconnected", status)
	}
}

func TestClientCloseBeforeStartClosesMessages(t *testing.T) {
	srv := newFakeServer(t)
	c := newIdleTestClient(t, srv.url, time.Hour, time.Hour)
	c.Close()
	c.Start()
	waitForClosed(t, c)

	select {
	case <-srv.attempts:
		t.Fatal("client dialled after Close")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestClientStartTwiceDialsOnce(t *testing.T) {
	srv := newFakeServer(t)
	c := newTestClient(t, srv.url, time.Hour, time.Hour)
	c.Start()
	srv.accept(t, time.Second)

	select {
	case <-srv.attempts:
	default:
		t.Fatal("no dial recorded")
	}
	select {
	case <-srv.attempts:
		t.Fatal("second Start dialled again")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestClientShowsDropBeforeQueuedMessagesAreRead(t *testing.T) {
	srv := newFakeServer(t)
	c := newIdleTestClient(t, srv.url, time.Hour, time.Hour)
	c.initialBackoff = time.Hour
	c.Start()
	conn := srv.accept(t, time.Second)
	waitForStatus(t, c, func(s ClientStatus) bool { return s.Connected })

	for i := 0; i < 3; i++ {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"name": "Test Meet"}`)); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	conn.Close()

	status := waitForStatus(t, c, func(s ClientStatus) bool { return !s.Connected })
	if status.LastError == "" {
		t.Errorf("status = %+v after a drop, want LastError set", status)
	}
	if n := len(c.Messages()); n != 3 {
		t.Errorf("%d messages queued, want the 3 sent before the drop still unread", n)
	}
}

func TestClientClearsLastErrorOnReconnect(t *testing.T) {
	srv := newFakeServer(t)
	c := newTestClient(t, srv.url, time.Hour, time.Hour)
	conn := srv.accept(t, time.Second)
	waitForStatus(t, c, func(s ClientStatus) bool { return s.Connected })

	srv.down.Store(true)
	conn.Close()
	waitForStatus(t, c, func(s ClientStatus) bool { return !s.Connected && s.LastError != "" })

	srv.down.Store(false)
	srv.accept(t, time.Second)
	status := waitForStatus(t, c, func(s ClientStatus) bool { return s.Connected })
	if status.LastError != "" {
		t.Errorf("status = %+v after reconnecting, want LastError cleared", status)
	}
}

func TestClientStaysConnectedOnServerError(t *testing.T) {
	srv := newFakeServer(t)
	c := newTestClient(t, srv.url, time.Hour, time.Hour)
	conn := srv.accept(t, time.Second)
	waitForStatus(t, c, func(s ClientStatus) bool { return s.Connected })

	for _, raw := range []string{`null`, `[1]`, `not json`} {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Fatalf("send: %v", err)
		}
		status := waitForStatus(t, c, func(s ClientStatus) bool { return strings.Contains(s.LastError, raw) })
		if !status.Connected {
			t.Errorf("status = %+v after server error %s, want still connected", status, raw)
		}
	}
	select {
	case m := <-c.Messages():
		t.Errorf("Messages got %s, want server errors kept off it", m)
	default:
	}
}
