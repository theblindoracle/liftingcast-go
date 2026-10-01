package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// record runs tap against a server whose handler sees each connection in
// turn, numbered from 1, and returns the recorded lines.
func record(t *testing.T, reconnects int, handle func(n int, w http.ResponseWriter, r *http.Request)) []entry {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handle(int(n.Add(1)), w, r)
	}))
	defer srv.Close()

	cfg := config{
		wsURL:      "ws" + strings.TrimPrefix(srv.URL, "http"),
		meetID:     "m1",
		password:   "hunter22",
		apiKey:     "key-secret",
		reconnects: reconnects,
		wait:       10 * time.Millisecond,
	}
	var out bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tap(ctx, cfg, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "hunter22") || strings.Contains(out.String(), "key-secret") {
		t.Errorf("recording contains a credential:\n%s", out.String())
	}

	var entries []entry
	sc := bufio.NewScanner(&out)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var e entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("line is not JSON: %s", sc.Text())
		}
		if e.Rx == "" {
			t.Errorf("line has no receive time: %s", sc.Text())
		}
		entries = append(entries, e)
	}
	return entries
}

func TestRecordsRefusedHandshake(t *testing.T) {
	got := record(t, 0, func(_ int, w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad apiKey "+r.URL.Query().Get("apiKey"), http.StatusUnauthorized)
	})
	if len(got) != 1 {
		t.Fatalf("got %d lines, want 1: %+v", len(got), got)
	}
	e := got[0]
	if e.Event != "handshake" || e.Status != http.StatusUnauthorized || e.Error == "" {
		t.Errorf("got %+v, want a refused handshake with status 401", e)
	}
	if !strings.Contains(e.Body, "bad apiKey REDACTED") {
		t.Errorf("body %q, want the server's text with the key redacted", e.Body)
	}
}

func TestRecordsServerErrorThenCloseThenReconnect(t *testing.T) {
	upgrader := websocket.Upgrader{}
	got := record(t, 1, func(n int, w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if n == 1 {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"name":"Test Meet"}`))
			conn.WriteMessage(websocket.TextMessage, []byte("Another connection was opened"))
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "replaced"))
		} else {
			conn.WriteMessage(websocket.TextMessage, []byte("Invalid password"))
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		}
		conn.ReadMessage() // until the client goes
	})

	want := []entry{
		{Conn: 1, Event: "handshake", Status: http.StatusSwitchingProtocols},
		{Conn: 1, Bytes: 20, Msg: json.RawMessage(`{"name":"Test Meet"}`)},
		{Conn: 1, Event: "text", Bytes: 29, Text: "Another connection was opened"},
		{Conn: 1, Event: "close", Code: websocket.ClosePolicyViolation, Text: "replaced"},
		{Conn: 2, Event: "handshake", Status: http.StatusSwitchingProtocols},
		{Conn: 2, Event: "text", Bytes: 16, Text: "Invalid password"},
		{Conn: 2, Event: "close", Code: websocket.CloseNormalClosure},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d lines, want %d:\n%+v", len(got), len(want), got)
	}
	for i := range want {
		g := got[i]
		g.Rx = ""
		if g.Event != want[i].Event || g.Conn != want[i].Conn || g.Status != want[i].Status ||
			g.Code != want[i].Code || g.Bytes != want[i].Bytes || g.Text != want[i].Text ||
			string(g.Msg) != string(want[i].Msg) {
			t.Errorf("line %d: got %+v, want %+v", i, g, want[i])
		}
	}
}
