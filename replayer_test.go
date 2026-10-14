package liftingcast

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// replayAll reads every update from a recording.
func replayAll(t *testing.T, recording string) []Update {
	t.Helper()
	var updates []Update
	for u, err := range NewReplayer(strings.NewReader(recording)).All() {
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		updates = append(updates, u)
	}
	return updates
}

func TestReplayerReproducesALiveRecordedRun(t *testing.T) {
	srv := newFakeServer(t)
	rec := newMemRecording()
	hub := NewHub(WithMaxBackoff(20*time.Millisecond), recorderOption(rec, nil))
	t.Cleanup(hub.Close)

	got := make(chan Update, 10)
	hub.Listen(func(u Update) { got <- u })
	hub.Connect(testConfig(srv.url))

	send := func(conn *websocket.Conn, raw string) {
		t.Helper()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	next := func() Update {
		t.Helper()
		select {
		case u := <-got:
			return u
		case <-time.After(2 * time.Second):
			t.Fatal("no update")
			return Update{}
		}
	}

	var live []Update
	conn := srv.accept(t, time.Second)
	send(conn, `{"name":"Meet","lifters":{"l1":{"id":"l1","name":"Alice"}}}`)
	live = append(live, next())
	send(conn, `{"name":"Meet","platforms":{"p1":{"id":"p1","name":"One"}}}`)
	live = append(live, next())
	send(conn, "Error: something temporary") // not meet state
	conn.NetConn().Close()                   // drop; the client redials
	conn = srv.accept(t, time.Second)
	send(conn, `{"name":"Meet 2","lifters":{"l2":{"id":"l2","name":"Bob"}}}`)
	live = append(live, next())

	hub.Close()
	rec.waitClosed(t)

	replayed := replayAll(t, rec.buf.String())
	if len(replayed) != len(live) {
		t.Fatalf("replayed %d states, want %d", len(replayed), len(live))
	}
	for i := range live {
		if !reflect.DeepEqual(replayed[i].State, live[i].State) {
			t.Errorf("state %d = %+v, want %+v", i, replayed[i].State, live[i].State)
		}
		if replayed[i].Connection != live[i].Connection {
			t.Errorf("state %d connection = %d, want %d", i, replayed[i].Connection, live[i].Connection)
		}
		if !replayed[i].ReceivedAt.Equal(live[i].ReceivedAt) {
			t.Errorf("state %d received at %v, want %v", i, replayed[i].ReceivedAt, live[i].ReceivedAt)
		}
	}
	if live[0].Connection == live[2].Connection {
		t.Errorf("test run never reconnected: connections %d and %d", live[0].Connection, live[2].Connection)
	}
}

func TestReplayerToleratesTruncatedLastLineAndMissingFinalEvent(t *testing.T) {
	recording := `{"rx":"2026-10-01T10:00:00Z","event":"start","sequence":1}` + "\n" +
		`{"rx":"2026-10-01T10:00:01Z","conn":1,"msg":{"name":"A"}}` + "\n" +
		`{"rx":"2026-10-01T10:00:02Z","conn":1,"msg":{"name":"B","li`
	updates := replayAll(t, recording)
	if len(updates) != 1 || updates[0].State.Name != "A" {
		t.Fatalf("updates = %+v, want only the complete message", updates)
	}
	want := time.Date(2026, 10, 1, 10, 0, 1, 0, time.UTC)
	if updates[0].Connection != 1 || !updates[0].ReceivedAt.Equal(want) {
		t.Errorf("update = %+v, want connection 1 received at %v", updates[0], want)
	}
}

func TestReplayerIgnoresUnknownEventsAndFields(t *testing.T) {
	recording := `{"rx":"2026-10-01T10:00:00Z","conn":1,"event":"from_the_future","novel":1}` + "\n" +
		`{"rx":"2026-10-01T10:00:01Z","conn":1,"extra":[1],"msg":{"name":"A"}}` + "\n" +
		`{"rx":"2026-10-01T10:00:02Z","conn":1,"event":"text","text":"Error: x"}` + "\n" +
		`{"rx":"2026-10-01T10:00:03Z","conn":1,"event":"closed"}` + "\n"
	if updates := replayAll(t, recording); len(updates) != 1 {
		t.Fatalf("got %d updates, want 1", len(updates))
	}
}

func TestReplayerReportsCorruptLineBeforeTheEnd(t *testing.T) {
	r := NewReplayer(strings.NewReader("{oops\n" + `{"rx":"2026-10-01T10:00:01Z","conn":1,"msg":{"name":"A"}}` + "\n"))
	if _, err := r.Next(); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("Next error = %v, want a decode error", err)
	}
}

func TestReplayerReportsUnparseableReceiveTimeWithLine(t *testing.T) {
	r := NewReplayer(strings.NewReader(`{"rx":"2026-10-01T10:00:01Z","conn":1,"msg":{"name":"A"}}` + "\n" + `{"rx":"yesterday","conn":1,"msg":{"name":"B"}}` + "\n"))
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("Next error = %v, want one naming line 2", err)
	}
}

func TestReplayerReplaysTestdataRecordings(t *testing.T) {
	files, err := filepath.Glob("testdata/*.jsonl")
	if err != nil || len(files) == 0 {
		t.Fatalf("no recordings: %v", err)
	}
	for _, file := range files {
		t.Run(filepath.Base(file), func(t *testing.T) {
			f, err := os.Open(file)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			want := 0
			scanner := bufio.NewScanner(f)
			scanner.Buffer(nil, 1<<22)
			for scanner.Scan() {
				var ev struct {
					Msg json.RawMessage `json:"msg"`
				}
				if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
					t.Fatal(err)
				}
				if ev.Msg != nil {
					want++
				}
			}
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, err := range NewReplayer(f).All() {
				if err != nil {
					t.Fatal(err)
				}
				n++
			}
			if n != want {
				t.Errorf("replayed %d states, want %d", n, want)
			}
		})
	}
}

func TestReplayerMatchesCacheMergeOfTestdata(t *testing.T) {
	merged, _ := replayRecording(t, "lc-traffic-test-meet-2026-09-29.jsonl")
	f, err := os.Open("testdata/lc-traffic-test-meet-2026-09-29.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var last *MeetState
	for u, err := range NewReplayer(f).All() {
		if err != nil {
			t.Fatal(err)
		}
		last = u.State
	}
	if !reflect.DeepEqual(last, merged) {
		t.Error("final replayed state differs from merging the recording directly")
	}
}
