package liftingcast

import (
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const replaySourceRecording = `{"rx":"2026-10-01T10:00:00Z","conn":1,"msg":{"name":"A","lifters":{"l1":{"id":"l1","name":"One"}}}}` + "\n" +
	`{"rx":"2026-10-01T10:00:10Z","conn":1,"msg":{"name":"B"}}` + "\n" +
	`{"rx":"2026-10-01T10:00:40Z","conn":2,"msg":{"name":"C"}}` + "\n"

// collect registers a listener and returns a func that waits for n updates.
func collect(src Source, n int) func(t *testing.T) []Update {
	var mu sync.Mutex
	var got []Update
	full := make(chan struct{})
	src.Listen(func(u Update) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, u)
		if len(got) == n {
			close(full)
		}
	})
	return func(t *testing.T) []Update {
		t.Helper()
		select {
		case <-full:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %d updates", n)
		}
		mu.Lock()
		defer mu.Unlock()
		return append([]Update(nil), got...)
	}
}

func TestReplaySourceDeliversWhatTheReplayerYields(t *testing.T) {
	data, err := os.ReadFile("testdata/lc-traffic-test-meet-2026-09-29.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	want := replayAll(t, string(data))
	if len(want) == 0 {
		t.Fatal("no states in recording")
	}

	src := NewReplaySource(strings.NewReader(string(data)), ReplayOptions{})
	t.Cleanup(src.Close)
	first := collect(src, len(want))
	second := collect(src, len(want))

	for i, got := range [][]Update{first(t), second(t)} {
		if !reflect.DeepEqual(got, want) {
			t.Errorf("listener %d saw different updates than the Replayer yields", i)
		}
	}
	if err := src.Err(); err != nil {
		t.Errorf("Err = %v", err)
	}
}

func TestReplaySourceWaitsTheRecordedGapsAtTheSpeed(t *testing.T) {
	for _, tc := range []struct {
		speed float64
		want  []time.Duration
	}{
		{1, []time.Duration{10 * time.Second, 30 * time.Second}},
		{2, []time.Duration{5 * time.Second, 15 * time.Second}},
		{0.5, []time.Duration{20 * time.Second, 60 * time.Second}},
		{0, nil},
	} {
		src := NewReplaySource(strings.NewReader(replaySourceRecording), ReplayOptions{Speed: tc.speed})
		var waits []time.Duration
		src.wait = func(d time.Duration, _ <-chan struct{}) bool {
			waits = append(waits, d)
			return true
		}
		got := collect(src, 3)
		got(t)
		src.Close()

		if !reflect.DeepEqual(waits, tc.want) {
			t.Errorf("speed %v: waits = %v, want %v", tc.speed, waits, tc.want)
		}
	}
}

func TestReplaySourceLoopsFromAnEmptyMeetState(t *testing.T) {
	src := NewReplaySource(strings.NewReader(replaySourceRecording), ReplayOptions{Loop: true})
	t.Cleanup(src.Close)

	got := collect(src, 6)(t)[:6]
	if !reflect.DeepEqual(got[:3], got[3:]) {
		t.Errorf("second pass differs from the first")
	}
	// The first state of the second pass has no lifters from the first pass
	if got[0].State.Name != "A" || len(got[0].State.Lifters) != 1 {
		t.Fatalf("first state = %+v", got[0].State)
	}
	if got[4].State.Name != "B" || len(got[4].State.Lifters) != 1 {
		t.Fatalf("second state of second pass = %+v", got[4].State)
	}
}

func TestReplaySourceLoopStartsWithoutLeftovers(t *testing.T) {
	// The second pass's first message carries no lifters section, so any
	// lifters in its state would have leaked from the first pass.
	rec := `{"rx":"2026-10-01T10:00:00Z","conn":1,"msg":{"name":"A"}}` + "\n" +
		`{"rx":"2026-10-01T10:00:01Z","conn":1,"msg":{"lifters":{"l1":{"id":"l1"}}}}` + "\n"
	src := NewReplaySource(strings.NewReader(rec), ReplayOptions{Loop: true})
	t.Cleanup(src.Close)

	got := collect(src, 4)(t)[:4]
	if len(got[0].State.Lifters) != 0 || len(got[2].State.Lifters) != 0 {
		t.Errorf("lifters leaked into a pass: %+v, %+v", got[0].State.Lifters, got[2].State.Lifters)
	}
}

func TestReplaySourceDoesNotLoopAnEmptyRecording(t *testing.T) {
	src := NewReplaySource(strings.NewReader(`{"event":"start"}`+"\n"), ReplayOptions{Loop: true})
	src.Listen(func(Update) {})

	select {
	case <-src.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("replay of a recording with no states did not end")
	}
	src.Close()
}

func TestReplaySourceCloseStopsDeliveryMidReplay(t *testing.T) {
	// Real time, with an hour's gap after the first state
	rec := `{"rx":"2026-10-01T10:00:00Z","conn":1,"msg":{"name":"A"}}` + "\n" +
		`{"rx":"2026-10-01T11:00:00Z","conn":1,"msg":{"name":"B"}}` + "\n"
	src := NewReplaySource(strings.NewReader(rec), ReplayOptions{Speed: 1})

	var mu sync.Mutex
	var got []Update
	first := make(chan struct{})
	src.Listen(func(u Update) {
		mu.Lock()
		got = append(got, u)
		n := len(got)
		mu.Unlock()
		if n == 1 {
			close(first)
		}
	})
	<-first

	closed := make(chan struct{})
	go func() { src.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return while the replay was waiting")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Errorf("got %d updates, want 1", len(got))
	}
	if src.Status() != (HubStatus{}) {
		t.Errorf("Status after Close = %+v", src.Status())
	}
	src.Listen(func(Update) { t.Error("handler called after Close") })()
}

func TestReplaySourceLateListenerGetsTheCurrentStateFirst(t *testing.T) {
	src := NewReplaySource(strings.NewReader(replaySourceRecording), ReplayOptions{})
	t.Cleanup(src.Close)
	collect(src, 3)(t)

	late := collect(src, 1)(t)
	if len(late) != 1 || late[0].State.Name != "C" || late[0].Connection != 2 {
		t.Errorf("late listener got %+v", late)
	}
}

func TestReplaySourceStatus(t *testing.T) {
	src := NewReplaySource(strings.NewReader(replaySourceRecording), ReplayOptions{Loop: true})
	t.Cleanup(src.Close)

	want := HubStatus{Connected: true, MeetID: "replay"}
	if got := src.Status(); got != want {
		t.Errorf("before start: %+v, want %+v", got, want)
	}

	var mu sync.Mutex
	statuses := map[string]bool{}
	var seen []Update
	done := make(chan struct{})
	src.Listen(func(u Update) {
		st := src.Status()
		mu.Lock()
		defer mu.Unlock()
		if !st.Connected || st.Rejected || st.MeetID != "replay" {
			t.Errorf("status = %+v", st)
		}
		statuses[st.MeetName] = true
		seen = append(seen, u)
		if len(seen) == 7 {
			close(done)
		}
	})
	<-done

	got := src.Status()
	if !got.Connected || got.MeetID != "replay" || got.MeetName == "" || got.LastMeetStateAt.IsZero() {
		t.Errorf("after replay: %+v", got)
	}
}

func TestReplaySourceReportsAReadError(t *testing.T) {
	src := NewReplaySource(strings.NewReader("{oops\n"), ReplayOptions{})
	src.Listen(func(Update) {})
	select {
	case <-src.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("replay did not stop")
	}
	if src.Err() == nil {
		t.Error("Err = nil")
	}
	src.Close()
}
