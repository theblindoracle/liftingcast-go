package liftingcast

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// memRecording is an in-memory recording writer. failFirst makes the first
// failFirst writes fail, and block (if set) holds every write until closed.
type memRecording struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	writes    int
	failFirst int
	block     chan struct{}
	closed    chan struct{}
	closedAt  int // length of buf when Close was called
}

func newMemRecording() *memRecording {
	return &memRecording{closed: make(chan struct{})}
}

func (m *memRecording) Write(p []byte) (int, error) {
	if m.block != nil {
		<-m.block
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes++
	if m.writes <= m.failFirst {
		return 0, errors.New("disk full")
	}
	return m.buf.Write(p)
}

func (m *memRecording) Close() error {
	m.mu.Lock()
	m.closedAt = m.buf.Len()
	m.mu.Unlock()
	close(m.closed)
	return nil
}

func (m *memRecording) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-m.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("recording writer was not closed")
	}
}

// events returns the recording's lines, decoded.
func (m *memRecording) events(t *testing.T) []map[string]any {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(m.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var e map[string]any
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("recording line is not JSON: %q", line)
		}
		out = append(out, e)
	}
	return out
}

func eventNames(events []map[string]any) []string {
	var names []string
	for _, e := range events {
		name, _ := e["event"].(string)
		if name == "" {
			name = "meet" // meet-state lines have no event, as in lc-tap
		}
		names = append(names, name)
	}
	return names
}

func recorderOption(m *memRecording, infos *[]ClientInfo) Option {
	return WithRecorder(func(info ClientInfo) (io.WriteCloser, error) {
		if infos != nil {
			*infos = append(*infos, info)
		}
		return m, nil
	})
}

func TestRecorderWritesStartThenClosedAndClosesWriter(t *testing.T) {
	rec := newMemRecording()
	var infos []ClientInfo
	c := NewClient(testConfig("ws://127.0.0.1:1"), recorderOption(rec, &infos))
	c.Close()
	rec.waitClosed(t)

	events := rec.events(t)
	if got := eventNames(events); len(got) != 2 || got[0] != "start" || got[1] != "closed" {
		t.Fatalf("events = %v, want [start closed]", got)
	}
	if events[0]["meetId"] != "meet" || events[0]["sequence"] != float64(1) {
		t.Errorf("start = %v, want meet ID and sequence 1", events[0])
	}
	if len(infos) != 1 || infos[0].MeetID != "meet" || infos[0].Sequence != 1 || infos[0].Start.IsZero() {
		t.Errorf("info = %+v", infos)
	}
	if rec.closedAt != rec.buf.Len() {
		t.Errorf("writer closed before the last line was written")
	}
}

func TestRecorderOfStartedClientClosesAfterConnectionEnds(t *testing.T) {
	srv := newFakeServer(t)
	rec := newMemRecording()
	c := NewClient(testConfig(srv.url), recorderOption(rec, nil))
	c.Start()
	srv.accept(t, time.Second)
	c.Close()
	rec.waitClosed(t)
	if got := eventNames(rec.events(t)); got[0] != "start" || got[len(got)-1] != "closed" {
		t.Fatalf("events = %v", got)
	}
}

func TestRecorderNeverWritesCredentialsOrURL(t *testing.T) {
	rec := newMemRecording()
	cfg := Config{BaseURL: "ws://127.0.0.1:1/websocket", MeetID: "meet", Password: "hunter2-secret", APIKey: "api-key-secret"}
	c := NewClient(cfg, recorderOption(rec, nil))
	c.Close()
	rec.waitClosed(t)
	out := rec.buf.String()
	for _, secret := range []string{cfg.Password, cfg.APIKey, cfg.BaseURL, "127.0.0.1"} {
		if strings.Contains(out, secret) {
			t.Errorf("recording contains %q: %s", secret, out)
		}
	}
}

func TestRecorderReportsEventsLostToFailedWrite(t *testing.T) {
	rec := newMemRecording()
	rec.failFirst = 1 // the start line is lost
	c := NewClient(testConfig("ws://127.0.0.1:1"), recorderOption(rec, nil))
	c.Close()
	rec.waitClosed(t)

	events := rec.events(t)
	if got := eventNames(events); len(got) != 2 || got[0] != "recorder_dropped" || got[1] != "closed" {
		t.Fatalf("events = %v, want [recorder_dropped closed]", got)
	}
	if events[0]["count"] != float64(1) {
		t.Errorf("count = %v, want 1", events[0]["count"])
	}
}

func TestStalledRecorderDoesNotStallConnection(t *testing.T) {
	srv := newFakeServer(t)
	rec := newMemRecording()
	rec.block = make(chan struct{})
	c := NewClient(testConfig(srv.url), recorderOption(rec, nil))
	c.initialBackoff = 10 * time.Millisecond
	c.Start()
	server := srv.accept(t, time.Second)
	if err := server.WriteMessage(1, []byte(`{"name":"Meet"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Messages():
	case <-time.After(time.Second):
		t.Fatal("meet state not delivered while the recording is stalled")
	}
	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close stalled on the recording")
	}
	close(rec.block)
	rec.waitClosed(t)
	if got := eventNames(rec.events(t)); got[0] != "start" || got[len(got)-1] != "closed" {
		t.Fatalf("events = %v", got)
	}
}

func TestRecorderFactoryErrorRunsWithoutRecording(t *testing.T) {
	srv := newFakeServer(t)
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	c := NewClient(testConfig(srv.url), WithLogger(logger),
		WithRecorder(func(ClientInfo) (io.WriteCloser, error) { return nil, errors.New("bad path") }))
	t.Cleanup(c.Close)
	c.Start()
	server := srv.accept(t, time.Second)
	if err := server.WriteMessage(1, []byte(`{"name":"Meet"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Messages():
	case <-time.After(time.Second):
		t.Fatal("connection not running without a recording")
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "bad path") {
		t.Errorf("no warning logged: %s", logs.String())
	}
}

// waitForEvent waits until the recording holds an event named name, and
// returns the recording's events so far.
func waitForEvent(t *testing.T, rec *memRecording, name string) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		events := rec.events(t)
		for _, e := range events {
			if e["event"] == name {
				return events
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %q event in %v", name, eventNames(events))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRecorderRecordsConnectionAndMessagesInOrder(t *testing.T) {
	srv := newFakeServer(t)
	rec := newMemRecording()
	c := NewClient(testConfig(srv.url), recorderOption(rec, nil))
	c.pingInterval = 10 * time.Millisecond
	c.Start()
	server := srv.accept(t, time.Second)
	// Answer the client's pings like LiftingCast does; none of it is recorded
	var writeMu sync.Mutex
	go func() {
		for {
			if _, _, err := server.ReadMessage(); err != nil {
				return
			}
			writeMu.Lock()
			server.WriteMessage(1, []byte("pong"))
			writeMu.Unlock()
		}
	}()
	time.Sleep(50 * time.Millisecond)
	writeMu.Lock()
	server.WriteMessage(1, []byte(`{"name":"Meet"}`))
	server.WriteMessage(1, []byte(`Error: something odd`))
	writeMu.Unlock()
	<-c.Messages()
	waitForEvent(t, rec, "text")
	c.Close()
	rec.waitClosed(t)

	events := rec.events(t)
	want := []string{"start", "dial", "handshake", "connected", "meet", "text", "closed"}
	var got []string
	for _, e := range events {
		name, _ := e["event"].(string)
		if name == "" {
			name = "meet"
		}
		got = append(got, name)
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for _, i := range []int{1, 2, 3, 4, 5} {
		if events[i]["conn"] != float64(1) {
			t.Errorf("%s conn = %v, want 1", got[i], events[i]["conn"])
		}
	}
	if events[2]["status"] != float64(101) {
		t.Errorf("handshake status = %v, want 101", events[2]["status"])
	}
	meet := events[4]
	if meet["bytes"] != float64(15) || meet["rx"] == "" {
		t.Errorf("meet message = %v", meet)
	}
	if m, _ := meet["msg"].(map[string]any); m["name"] != "Meet" {
		t.Errorf("meet msg = %v", meet["msg"])
	}
	if text := events[5]; text["text"] != "Error: something odd" || text["bytes"] != float64(20) {
		t.Errorf("text = %v", text)
	}
}

// eventsUntil starts c and returns the recording's event names, with each
// event as decoded, once an event named name has been written.
func eventsUntil(t *testing.T, rec *memRecording, name string) ([]string, []map[string]any) {
	t.Helper()
	events := waitForEvent(t, rec, name)
	return eventNames(events), events
}

func startRecorded(t *testing.T, url string) (*Client, *memRecording) {
	t.Helper()
	rec := newMemRecording()
	c := NewClient(testConfig(url), recorderOption(rec, nil))
	c.initialBackoff = 10 * time.Millisecond
	t.Cleanup(c.Close)
	c.Start()
	return c, rec
}

func wantNames(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) < len(want) || strings.Join(got[:len(want)], ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want to begin %v", got, want)
	}
}

func TestRecorderRecordsRefusedHandshakeAndBackoff(t *testing.T) {
	srv := newFakeServer(t)
	srv.refuse.Store(http.StatusServiceUnavailable)
	_, rec := startRecorded(t, srv.url)
	names, events := eventsUntil(t, rec, "backoff")
	wantNames(t, names, "start", "dial", "handshake", "backoff")
	hs := events[2]
	if hs["status"] != float64(503) || hs["conn"] != float64(1) || !strings.Contains(hs["body"].(string), "Service Unavailable") || hs["error"] == "" {
		t.Errorf("handshake = %v", hs)
	}
	if events[3]["waitMs"] != float64(10) {
		t.Errorf("backoff = %v, want waitMs 10", events[3])
	}
	// Still failing to connect: the next attempt is the same connection number
	waitForEvent(t, rec, "dial")
}

func TestRecorderRecordsRejectedHandshake(t *testing.T) {
	srv := newFakeServer(t)
	srv.refuse.Store(http.StatusUnauthorized)
	_, rec := startRecorded(t, srv.url)
	names, events := eventsUntil(t, rec, "rejected")
	wantNames(t, names, "start", "dial", "handshake", "rejected")
	if events[2]["status"] != float64(401) {
		t.Errorf("handshake = %v", events[2])
	}
	if !strings.Contains(events[3]["error"].(string), "401") {
		t.Errorf("rejected = %v", events[3])
	}
}

func TestRecorderRecordsServerErrorThenDropThenReconnect(t *testing.T) {
	srv := newFakeServer(t)
	_, rec := startRecorded(t, srv.url)
	server := srv.accept(t, time.Second)
	server.WriteMessage(1, []byte("Error: try later"))
	server.UnderlyingConn().Close()
	srv.accept(t, time.Second)
	names, events := eventsUntil(t, rec, "connected")
	for len(events) < 9 { // wait for the second connected
		time.Sleep(5 * time.Millisecond)
		names, events = eventsUntil(t, rec, "connected")
	}
	wantNames(t, names, "start", "dial", "handshake", "connected", "text", "read_error", "backoff", "dial", "handshake", "connected")
	if events[5]["conn"] != float64(1) || events[5]["error"] == "" {
		t.Errorf("read_error = %v", events[5])
	}
	if events[6]["conn"] != float64(1) {
		t.Errorf("backoff = %v, want conn 1", events[6])
	}
	if events[9]["conn"] != float64(2) {
		t.Errorf("connected = %v, want conn 2", events[9])
	}
}

func TestRecorderRecordsCloseFrame(t *testing.T) {
	srv := newFakeServer(t)
	_, rec := startRecorded(t, srv.url)
	server := srv.accept(t, time.Second)
	server.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "bye"))
	names, events := eventsUntil(t, rec, "close")
	wantNames(t, names, "start", "dial", "handshake", "connected", "close", "backoff")
	if events[4]["code"] != float64(1001) || events[4]["text"] != "bye" {
		t.Errorf("close = %v", events[4])
	}
}

func TestRecorderRecordsTimeout(t *testing.T) {
	srv := newFakeServer(t)
	rec := newMemRecording()
	c := NewClient(testConfig(srv.url), recorderOption(rec, nil))
	c.messageTimeout = 50 * time.Millisecond
	c.initialBackoff = 10 * time.Millisecond
	t.Cleanup(c.Close)
	c.Start()
	srv.accept(t, time.Second)
	names, events := eventsUntil(t, rec, "timeout")
	wantNames(t, names, "start", "dial", "handshake", "connected", "timeout")
	if events[4]["conn"] != float64(1) {
		t.Errorf("timeout = %v", events[4])
	}
	// Our own close of the silent connection is not a read error
	waitForEvent(t, rec, "backoff")
	for _, n := range eventNames(rec.events(t)) {
		if n == "read_error" {
			t.Errorf("read_error recorded after a timeout")
		}
	}
}

func TestRecorderRecordsPermanentServerErrorAsRejected(t *testing.T) {
	srv := newFakeServer(t)
	_, rec := startRecorded(t, srv.url)
	server := srv.accept(t, time.Second)
	server.WriteMessage(1, []byte("Error: invalid api key"))
	names, events := eventsUntil(t, rec, "rejected")
	wantNames(t, names, "start", "dial", "handshake", "connected", "text", "rejected")
	if events[5]["conn"] != float64(1) || !strings.Contains(events[5]["error"].(string), "invalid api key") {
		t.Errorf("rejected = %v", events[5])
	}
}

func TestRecorderDialFailureLeavesNoAddressOrCredentials(t *testing.T) {
	cfg := Config{BaseURL: "ws://127.0.0.1:1/websocket", MeetID: "meet", Password: "hunter2-secret", APIKey: "api-key-secret"}
	rec := newMemRecording()
	c := NewClient(cfg, recorderOption(rec, nil))
	t.Cleanup(c.Close)
	c.Start()
	_, events := eventsUntil(t, rec, "handshake")
	if events[2]["error"] == "" || events[2]["status"] != nil {
		t.Errorf("handshake = %v, want an error and no status", events[2])
	}
	c.Close()
	rec.waitClosed(t)
	out := rec.buf.String()
	for _, secret := range []string{cfg.Password, cfg.APIKey, cfg.BaseURL, "127.0.0.1"} {
		if strings.Contains(out, secret) {
			t.Errorf("recording contains %q: %s", secret, out)
		}
	}
}

func TestOverflowingRecorderDropsEventsAndCountsThem(t *testing.T) {
	const messages = 200
	srv := newFakeServer(t)
	rec := newMemRecording()
	rec.block = make(chan struct{})
	c := NewClient(testConfig(srv.url), recorderOption(rec, nil))
	t.Cleanup(c.Close)
	c.Start()
	server := srv.accept(t, time.Second)
	go func() {
		for i := 0; i < messages; i++ {
			if server.WriteMessage(1, []byte(`{"name":"Meet"}`)) != nil {
				return
			}
		}
	}()
	// The live connection delivers every message while the recording is stuck
	for i := 0; i < messages; i++ {
		select {
		case <-c.Messages():
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d messages delivered", i, messages)
		}
	}
	c.Close()
	close(rec.block)
	rec.waitClosed(t)

	// start, dial, handshake, connected, every message, and closed
	const attempted = 4 + messages + 1
	events := rec.events(t)
	var written, dropped int
	for _, e := range events {
		if e["event"] == "recorder_dropped" {
			dropped += int(e["count"].(float64))
		} else {
			written++
		}
	}
	if dropped == 0 {
		t.Fatalf("no events reported dropped, though %d were attempted into a queue of %d", attempted, recorderBuffer)
	}
	if written+dropped != attempted {
		t.Errorf("written %d + dropped %d = %d, want %d", written, dropped, written+dropped, attempted)
	}
	// The queue, the one event the stuck writer holds, and the closed event,
	// which may find room once the writer is released
	if written > recorderBuffer+2 {
		t.Errorf("wrote %d events from a stalled writer, more than the queue of %d plus two", written, recorderBuffer)
	}
	if events[0]["event"] != "start" {
		t.Errorf("first event = %v, want start", events[0])
	}
}

func TestFinalEventSurvivesFullQueue(t *testing.T) {
	srv := newFakeServer(t)
	rec := newMemRecording()
	rec.block = make(chan struct{})
	c := NewClient(testConfig(srv.url), recorderOption(rec, nil))
	c.Start()
	server := srv.accept(t, time.Second)
	for i := 0; i < recorderBuffer*2; i++ {
		if err := server.WriteMessage(1, []byte(`{"name":"Meet"}`)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-c.Messages():
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d messages delivered", i)
		}
	}
	done := make(chan struct{})
	go func() { c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close stalled on the recording")
	}
	close(rec.block)
	rec.waitClosed(t)
	events := rec.events(t)
	if last := events[len(events)-1]; last["event"] != "closed" {
		t.Fatalf("last event = %v, want closed", last)
	}
}

func TestRecordAfterFinishIsIgnored(t *testing.T) {
	m := newMemRecording()
	r, err := newRecorder(func(ClientInfo) (io.WriteCloser, error) { return m, nil }, ClientInfo{})
	if err != nil {
		t.Fatal(err)
	}
	r.finish(eventClosed)
	r.record(recordEvent{Event: eventText}) // must not panic
	r.finish(eventReplaced)
	m.waitClosed(t)
	if got := eventNames(m.events(t)); len(got) != 2 || got[1] != "closed" {
		t.Fatalf("events = %v", got)
	}
}

func TestRecorderRejectedHandshakeSharesConnectionNumber(t *testing.T) {
	srv := newFakeServer(t)
	srv.refuse.Store(http.StatusUnauthorized)
	_, rec := startRecorded(t, srv.url)
	_, events := eventsUntil(t, rec, "rejected")
	for _, e := range events[1:] {
		if e["conn"] != float64(1) {
			t.Errorf("%v: conn = %v, want 1", e["event"], e["conn"])
		}
	}
}

// recordingSet is a RecorderFactory's in-memory recordings, one per Client.
type recordingSet struct {
	mu   sync.Mutex
	recs []*memRecording
}

func (s *recordingSet) option() Option {
	return WithRecorder(func(ClientInfo) (io.WriteCloser, error) {
		m := newMemRecording()
		s.mu.Lock()
		s.recs = append(s.recs, m)
		s.mu.Unlock()
		return m, nil
	})
}

func (s *recordingSet) get(t *testing.T, i int) *memRecording {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.recs) <= i {
		t.Fatalf("got %d recordings, want more than %d", len(s.recs), i)
	}
	return s.recs[i]
}

func (s *recordingSet) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.recs)
}

func TestHubRecordsEachClientAndMarksReplacement(t *testing.T) {
	srv := newFakeServer(t)
	set := &recordingSet{}
	hub := NewHub(set.option())
	t.Cleanup(hub.Close)

	hub.Connect(Config{BaseURL: srv.url, MeetID: "a"})
	srv.accept(t, time.Second)
	hub.Connect(Config{BaseURL: srv.url, MeetID: "b"})
	srv.accept(t, time.Second)

	first, second := set.get(t, 0), set.get(t, 1)
	first.waitClosed(t)
	ev := first.events(t)
	if last := ev[len(ev)-1]["event"]; last != "replaced" {
		t.Errorf("old recording's last event = %v, want replaced", last)
	}
	if first.closedAt != first.buf.Len() {
		t.Errorf("old writer was written to after Close")
	}
	if ev[0]["event"] != "start" || ev[0]["sequence"] != float64(1) || ev[0]["replaces"] != nil {
		t.Errorf("first start = %v, want sequence 1 and no replaces", ev[0])
	}
	start := second.events(t)[0]
	if start["event"] != "start" || start["meetId"] != "b" || start["sequence"] != float64(2) || start["replaces"] != float64(1) {
		t.Errorf("second start = %v, want meet b, sequence 2, replaces 1", start)
	}

	hub.Disconnect()
	second.waitClosed(t)
	ev = second.events(t)
	if last := ev[len(ev)-1]["event"]; last != "closed" {
		t.Errorf("disconnected recording's last event = %v, want closed", last)
	}

	// A Connect after Disconnect replaces nothing
	hub.Connect(Config{BaseURL: srv.url, MeetID: "c"})
	srv.accept(t, time.Second)
	start = set.get(t, 2).events(t)[0]
	if start["sequence"] != float64(3) || start["replaces"] != nil {
		t.Errorf("third start = %v, want sequence 3 and no replaces", start)
	}
}
