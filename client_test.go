package liftingcast

import (
	"bufio"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeServer stands in for LiftingCast. Each accepted connection is handed to
// the test on conns, and the test decides what the server does with it.
// While refuse holds an HTTP status, every handshake is refused with it. Each
// attempt's time is recorded on attempts.
type fakeServer struct {
	url      string
	conns    chan *websocket.Conn
	attempts chan time.Time
	refuse   atomic.Int32
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
		if status := int(s.refuse.Load()); status != 0 {
			http.Error(w, http.StatusText(status), status)
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

// expectNoAttempt fails if the client tries to connect within d, ignoring
// attempts recorded so far.
func (s *fakeServer) expectNoAttempt(t *testing.T, d time.Duration) {
	t.Helper()
	s.drainAttempts()
	select {
	case <-s.attempts:
		t.Fatal("client tried to connect again")
	case <-time.After(d):
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
	srv.refuse.Store(http.StatusServiceUnavailable)
	c := newTestClient(t, srv.url, time.Hour, time.Hour)

	srv.nextAttempt(t, time.Second)
	status := waitForStatus(t, c, func(s ClientStatus) bool { return s.LastError != "" })
	if status.Connected {
		t.Errorf("status = %+v after a failed dial, want disconnected", status)
	}

	srv.refuse.Store(0)
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

	srv.refuse.Store(http.StatusServiceUnavailable)
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

	srv.refuse.Store(http.StatusServiceUnavailable)
	conn.Close()
	time.Sleep(time.Second)
	srv.refuse.Store(0)

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

// drainMessages discards meet state until Messages closes.
func drainMessages(c *Client) {
	for range c.Messages() {
	}
}

// expectDoubledBackoff fails unless the gap before the client's attempt n
// (counting from 0) was at least the backoff doubled once per earlier failure.
func expectDoubledBackoff(t *testing.T, c *Client, n int, gap time.Duration) {
	t.Helper()
	if want := c.initialBackoff << (n - 1); gap < want {
		t.Errorf("gap before connection %d = %v, want at least %v", n+1, gap, want)
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

	srv.refuse.Store(http.StatusServiceUnavailable)
	conn.Close()
	waitForStatus(t, c, func(s ClientStatus) bool { return !s.Connected && s.LastError != "" })

	srv.refuse.Store(0)
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

// recordedEvent is one line of a recording from testdata/: meet state the
// server sent (Msg), a message that isn't JSON (Event "text"), or how the
// connection ended.
type recordedEvent struct {
	Conn  int             `json:"conn"`
	Event string          `json:"event"`
	Text  string          `json:"text"`
	Msg   json.RawMessage `json:"msg"`
}

// loadRecording reads a recording from testdata/ and returns each recorded
// connection's events in order.
func loadRecording(t *testing.T, name string) [][]recordedEvent {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var conns [][]recordedEvent
	scanner := bufio.NewScanner(f)
	scanner.Buffer(nil, 1<<20)
	for line := 1; scanner.Scan(); line++ {
		var ev recordedEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			t.Fatalf("%s line %d: %v", name, line, err)
		}
		for len(conns) < ev.Conn {
			conns = append(conns, nil)
		}
		conns[ev.Conn-1] = append(conns[ev.Conn-1], ev)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return conns
}

// replay plays the server's side of one recorded connection on conn: it
// sends what LiftingCast sent and ends the connection the way LiftingCast
// did. A connection that was never ended is left open.
func replay(t *testing.T, conn *websocket.Conn, events []recordedEvent) {
	t.Helper()
	for _, ev := range events {
		var err error
		switch {
		case ev.Msg != nil:
			err = conn.WriteMessage(websocket.TextMessage, ev.Msg)
		case ev.Event == "text":
			err = conn.WriteMessage(websocket.TextMessage, []byte(ev.Text))
		case ev.Event == "read_error":
			// Dropped without a close frame
			conn.NetConn().Close()
		case ev.Event == "close":
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1000, ev.Text))
			conn.Close()
		}
		if err != nil {
			t.Fatalf("replay %s: %v", ev.Event, err)
		}
	}
}

const (
	badCredentialsError = "Error: unauthorized - meet id or password in auth param are incorrect"
	badAPIKeyError      = "Error: invalid api key"
	takeoverError       = "Another websocket connection was established. This connection is closing. You are only allowed to open one connection at a time."
)

func TestClientRejectedByRecordedPermanentServerError(t *testing.T) {
	for _, tc := range []struct{ recording, message string }{
		{"lc-wrong-meet-id-hosted.jsonl", badCredentialsError},
		{"lc-wrong-meet-id-selfhosted.jsonl", badCredentialsError},
		{"lc-wrong-password-hosted.jsonl", badCredentialsError},
		{"lc-wrong-password-selfhosted.jsonl", badCredentialsError},
		{"lc-wrong-api-key-hosted.jsonl", badAPIKeyError},
	} {
		t.Run(tc.recording, func(t *testing.T) {
			conns := loadRecording(t, tc.recording)
			srv := newFakeServer(t)
			c := newTestClient(t, srv.url, time.Hour, time.Hour)
			replay(t, srv.accept(t, time.Second), conns[0])

			status := waitForStatus(t, c, func(s ClientStatus) bool { return s.Rejected })
			if status.Connected || !strings.Contains(status.LastError, tc.message) {
				t.Errorf("status = %+v, want disconnected with LastError %q", status, tc.message)
			}
			srv.expectNoAttempt(t, 200*time.Millisecond)
			if status := c.Status(); !status.Rejected {
				t.Errorf("status = %+v later, want still rejected", status)
			}
		})
	}
}

func TestClientRejectedByRefusedHandshake(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := newFakeServer(t)
			srv.refuse.Store(int32(code))
			c := newTestClient(t, srv.url, time.Hour, time.Hour)

			status := waitForStatus(t, c, func(s ClientStatus) bool { return s.Rejected })
			if status.Connected || !strings.Contains(status.LastError, strconv.Itoa(code)) {
				t.Errorf("status = %+v, want disconnected with LastError naming %d", status, code)
			}
			srv.expectNoAttempt(t, 200*time.Millisecond)
		})
	}
}

func TestClientClosesRejectedConnectionItself(t *testing.T) {
	srv := newFakeServer(t)
	c := newTestClient(t, srv.url, time.Hour, time.Hour)
	conn := srv.accept(t, time.Second)

	// Send the error but leave the connection open
	if err := conn.WriteMessage(websocket.TextMessage, []byte(badAPIKeyError)); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitForStatus(t, c, func(s ClientStatus) bool { return s.Rejected })
	conn.SetReadDeadline(time.Now().Add(time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatal("client left the rejected connection open")
			}
			return
		}
	}
}

func TestClientKeepsMessagesOpenWhenRejected(t *testing.T) {
	srv := newFakeServer(t)
	c := newTestClient(t, srv.url, time.Hour, time.Hour)
	replay(t, srv.accept(t, time.Second), loadRecording(t, "lc-wrong-api-key-hosted.jsonl")[0])
	waitForStatus(t, c, func(s ClientStatus) bool { return s.Rejected })

	select {
	case _, ok := <-c.Messages():
		if !ok {
			t.Fatal("Messages closed when the client was rejected, want it open until Close")
		}
		t.Fatal("Messages got meet state from a rejected connection")
	case <-time.After(100 * time.Millisecond):
	}
	c.Close()
	waitForClosed(t, c)
}

func TestClientIgnoresWrongAPIKeyOnSelfHosted(t *testing.T) {
	srv := newFakeServer(t)
	c := newTestClient(t, srv.url, time.Hour, time.Hour)
	replay(t, srv.accept(t, time.Second), loadRecording(t, "lc-wrong-api-key-selfhosted.jsonl")[0])

	select {
	case <-c.Messages():
	case <-time.After(time.Second):
		t.Fatal("no meet state")
	}
	if status := c.Status(); !status.Connected || status.Rejected || status.LastError != "" {
		t.Errorf("status = %+v, want connected with no error", status)
	}
}

func TestClientKeepsRetryingAfterRecordedTakeover(t *testing.T) {
	for _, recording := range []string{"lc-two-conns-A-hosted.jsonl", "lc-two-conns-B-hosted.jsonl"} {
		t.Run(recording, func(t *testing.T) {
			conns := loadRecording(t, recording)
			srv := newFakeServer(t)
			c := newIdleTestClient(t, srv.url, time.Hour, time.Hour)
			c.initialBackoff = 20 * time.Millisecond
			c.Start()
			go drainMessages(c)

			// Each connection gets meet state and is then pushed off, so none
			// of them resets the backoff: the gaps keep doubling. The last
			// connection B made was never pushed off.
			var prev time.Time
			for i, events := range conns {
				at := srv.nextAttempt(t, time.Second)
				if i > 0 {
					expectDoubledBackoff(t, c, i, at.Sub(prev))
				}
				prev = at
				conn := srv.accept(t, time.Second)
				waitForStatus(t, c, func(s ClientStatus) bool { return s.Connected })
				replay(t, conn, events)
				if events[len(events)-1].Event != "read_error" {
					time.Sleep(50 * time.Millisecond)
					if status := c.Status(); !status.Connected || status.LastError != "" {
						t.Errorf("status = %+v on the connection that was kept, want connected", status)
					}
					continue
				}
				status := waitForStatus(t, c, func(s ClientStatus) bool { return !s.Connected })
				if status.Rejected || status.LastError != "server error: "+takeoverError {
					t.Errorf("status = %+v after takeover %d, want not rejected and the takeover as LastError", status, i+1)
				}
			}
		})
	}
}

func TestClientResetsBackoffOnlyAfterMeetStateWithoutServerError(t *testing.T) {
	srv := newFakeServer(t)
	c := newIdleTestClient(t, srv.url, time.Hour, time.Hour)
	c.initialBackoff = 20 * time.Millisecond
	c.Start()
	go drainMessages(c)

	// Connections that deliver no meet state don't reset the backoff
	var prev time.Time
	for i := 0; i < 4; i++ {
		at := srv.nextAttempt(t, time.Second)
		if i > 0 {
			expectDoubledBackoff(t, c, i, at.Sub(prev))
		}
		prev = at
		srv.accept(t, time.Second).Close()
	}

	// One that delivers meet state and drops without a server error does
	srv.nextAttempt(t, time.Second)
	conn := srv.accept(t, time.Second)
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"name": "Test Meet"}`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitForStatus(t, c, func(s ClientStatus) bool { return s.Connected })
	time.Sleep(20 * time.Millisecond)
	dropped := time.Now()
	conn.NetConn().Close()
	if gap := srv.nextAttempt(t, time.Second).Sub(dropped); gap > 4*c.initialBackoff {
		t.Errorf("reconnected %v after a connection that delivered meet state dropped, want about %v", gap, c.initialBackoff)
	}
}

func TestClientForgetsServerErrorFollowedByMeetState(t *testing.T) {
	srv := newFakeServer(t)
	c := newIdleTestClient(t, srv.url, time.Hour, time.Hour)
	c.initialBackoff = time.Hour
	c.Start()
	conn := srv.accept(t, time.Second)
	waitForStatus(t, c, func(s ClientStatus) bool { return s.Connected })

	for _, raw := range []string{`not json`, `{"name": "Test Meet"}`} {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	<-c.Messages()
	conn.NetConn().Close()

	status := waitForStatus(t, c, func(s ClientStatus) bool { return !s.Connected })
	if strings.Contains(status.LastError, "not json") {
		t.Errorf("status = %+v after the drop, want the drop as LastError, not a server error meet state followed", status)
	}
}
