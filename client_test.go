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
// connected. Tests may adjust its timings before calling Connect.
func newIdleTestClient(t *testing.T, url string, ping, timeout time.Duration) *Client {
	t.Helper()
	c := NewClient(url, "meet", "password", "key")
	c.pingInterval = ping
	c.messageTimeout = timeout
	c.initialBackoff = 10 * time.Millisecond
	c.backoff = c.initialBackoff
	c.maxBackoff = time.Second
	c.handshakeTimeout = time.Second

	stopDraining := make(chan struct{})
	go func() {
		for {
			select {
			case <-c.Errors():
			case <-stopDraining:
				return
			}
		}
	}()
	t.Cleanup(func() {
		c.Close()
		close(stopDraining)
	})
	return c
}

func newTestClient(t *testing.T, url string, ping, timeout time.Duration) *Client {
	t.Helper()
	c := newIdleTestClient(t, url, ping, timeout)
	if err := c.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	return c
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
	if err := c.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
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
	if err := c.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn := srv.accept(t, time.Second)

	srv.down.Store(true)
	conn.Close()
	time.Sleep(time.Second)
	srv.down.Store(false)

	srv.accept(t, c.maxBackoff+150*time.Millisecond)
}

func TestClientDialFailsAtHandshakeTimeout(t *testing.T) {
	// Accepts TCP connections but never answers the WebSocket handshake.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
		}
	}()

	c := newIdleTestClient(t, "ws://"+ln.Addr().String(), time.Hour, time.Hour)
	c.handshakeTimeout = 50 * time.Millisecond

	result := make(chan error, 1)
	start := time.Now()
	go func() { result <- c.Connect() }()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("Connect succeeded against a server that never handshakes")
		}
		if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
			t.Errorf("Connect failed after %v, want about %v", elapsed, c.handshakeTimeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect did not fail within 2s")
	}
}

func TestClientCloseDuringBackoffEndsReconnection(t *testing.T) {
	srv := newFakeServer(t)
	c := newIdleTestClient(t, srv.url, time.Hour, time.Hour)
	c.initialBackoff = 200 * time.Millisecond
	c.backoff = c.initialBackoff
	if err := c.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn := srv.accept(t, time.Second)
	srv.drainAttempts()

	conn.Close()
	time.Sleep(50 * time.Millisecond) // the client is now waiting out its backoff
	c.Close()

	select {
	case <-srv.attempts:
		t.Fatal("client tried to reconnect after Close")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestClientCloseDuringDialEndsReconnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { conn.Close() })
		accepted <- conn
	}()

	c := newIdleTestClient(t, "ws://"+ln.Addr().String(), time.Hour, time.Hour)
	c.handshakeTimeout = time.Hour

	result := make(chan error, 1)
	go func() { result <- c.Connect() }()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("client did not dial")
	}
	c.Close()

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("Connect succeeded after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not end a pending dial")
	}
}

func TestClientReportsNonObjectMessagesAsErrors(t *testing.T) {
	srv := newFakeServer(t)
	c := NewClient(srv.url, "meet", "password", "key")
	t.Cleanup(c.Close)
	if err := c.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	conn := srv.accept(t, time.Second)

	for _, raw := range []string{`null`, `[1]`, `not json`} {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Fatalf("send: %v", err)
		}
		select {
		case <-c.Errors():
		case m := <-c.DataUpdate():
			t.Fatalf("DataUpdate got %s, want it reported on Errors", m)
		case <-time.After(time.Second):
			t.Fatalf("message %s was neither reported nor passed on", raw)
		}
	}
}
