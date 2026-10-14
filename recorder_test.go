package liftingcast

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
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
		names = append(names, e["event"].(string))
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
