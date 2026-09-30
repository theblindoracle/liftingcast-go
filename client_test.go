package liftingcast

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeServer stands in for LiftingCast. Each accepted connection is handed to
// the test on conns, and the test decides what the server does with it.
type fakeServer struct {
	url   string
	conns chan *websocket.Conn
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	conns := make(chan *websocket.Conn, 16)
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conns <- conn
	}))
	t.Cleanup(srv.Close)
	return &fakeServer{url: "ws" + strings.TrimPrefix(srv.URL, "http"), conns: conns}
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

func newTestClient(t *testing.T, url string, ping, timeout time.Duration) *Client {
	t.Helper()
	c := NewClient(url, "meet", "password", "key")
	c.pingInterval = ping
	c.messageTimeout = timeout
	c.initialBackoff = 10 * time.Millisecond
	c.backoff = c.initialBackoff

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
