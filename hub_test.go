package liftingcast

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHubHandsMergedStateToListeners(t *testing.T) {
	srv := newFakeServer(t)
	hub := NewHub(srv.url, "meet", "password", "key")
	go hub.Run()
	t.Cleanup(hub.Disconnect)

	states := make(chan *MeetApiResponse, 10)
	hub.RegisterBackendListener() <- NewBackendListener("test", func(m *MeetApiResponse) { states <- m })

	conn := srv.accept(t, time.Second)
	send := func(raw string) {
		t.Helper()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	send(`{"name": "Test Meet", "federation": "USAPL", "lifters": {"l1": {"id": "l1", "name": "Alice"}},
		"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "initial"}}}`)
	send(`{"name": "Test Meet", "federation": "USAPL",
		"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "started"}}}`)

	deadline := time.After(time.Second)
	for {
		select {
		case m := <-states:
			p1 := (*m.Platforms)["p1"]
			if p1.ClockState == nil || *p1.ClockState != ClockStarted {
				continue
			}
			if m.Lifters == nil || (*m.Lifters)["l1"].ID != "l1" {
				t.Fatalf("merged state lost lifters: %v", m.Lifters)
			}
			if !hub.GetStatus().Connected {
				t.Error("status not connected")
			}
			return
		case <-deadline:
			t.Fatal("listener never received the merged platforms update")
		}
	}
}

// startHub returns a running hub connected to a fake server, and a function
// that sends the server's connection one message per lot number, each
// setting lifter l1's lot so listeners can tell the messages apart.
func startHub(t *testing.T) (*Hub, func(lots ...int)) {
	t.Helper()
	srv := newFakeServer(t)
	hub := NewHub(srv.url, "meet", "password", "key")
	go hub.Run()
	t.Cleanup(hub.Disconnect)

	conn := srv.accept(t, time.Second)
	send := func(lots ...int) {
		t.Helper()
		for _, lot := range lots {
			raw := fmt.Sprintf(`{"name": "Test Meet", "lifters": {"l1": {"id": "l1", "lot": %d}}}`, lot)
			if err := conn.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
				t.Fatalf("send: %v", err)
			}
		}
	}
	return hub, send
}

func lotsUpTo(from, to int) []int {
	var lots []int
	for i := from; i <= to; i++ {
		lots = append(lots, i)
	}
	return lots
}

// lotRecorder is a listener handler that records the lot of each state.
type lotRecorder struct {
	mu   sync.Mutex
	lots []int
	seen chan int
}

func newLotRecorder() *lotRecorder {
	return &lotRecorder{seen: make(chan int, 1024)}
}

func (r *lotRecorder) handle(m *MeetApiResponse) {
	lot := *(*m.Lifters)["l1"].Lot
	r.mu.Lock()
	r.lots = append(r.lots, lot)
	r.mu.Unlock()
	r.seen <- lot
}

func (r *lotRecorder) recorded() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.lots...)
}

// waitFor waits until the recorder has seen lot.
func (r *lotRecorder) waitFor(t *testing.T, lot int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-r.seen:
			if got == lot {
				return
			}
		case <-deadline:
			t.Fatalf("listener never received lot %d; got %v", lot, r.recorded())
		}
	}
}

func TestHubDeliversStatesToListenerInOrder(t *testing.T) {
	hub, send := startHub(t)
	rec := newLotRecorder()
	hub.RegisterBackendListener() <- NewBackendListener("test", rec.handle)

	send(lotsUpTo(0, 199)...)
	rec.waitFor(t, 199)

	if got, want := rec.recorded(), lotsUpTo(0, 199); !reflect.DeepEqual(got, want) {
		t.Errorf("listener saw lots %v, want 0..199 in order", got)
	}
}

func TestHubSendsCachedStateBeforeLaterStates(t *testing.T) {
	hub, send := startHub(t)
	first := newLotRecorder()
	hub.RegisterBackendListener() <- NewBackendListener("first", first.handle)
	send(lotsUpTo(0, 49)...)
	first.waitFor(t, 49)

	go send(lotsUpTo(50, 199)...)
	late := newLotRecorder()
	hub.RegisterBackendListener() <- NewBackendListener("late", late.handle)
	late.waitFor(t, 199)

	got := late.recorded()
	if got[0] < 49 {
		t.Fatalf("late listener's first state has lot %d, want the cached state (lot >= 49)", got[0])
	}
	if want := lotsUpTo(got[0], 199); !reflect.DeepEqual(got, want) {
		t.Errorf("late listener saw lots %v, want %d..199 in order", got, got[0])
	}
}

func TestHubKeepsDeliveringWhileOneListenerBlocks(t *testing.T) {
	hub, send := startHub(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	hub.RegisterBackendListener() <- NewBackendListener("stuck", func(*MeetApiResponse) { <-release })
	rec := newLotRecorder()
	hub.RegisterBackendListener() <- NewBackendListener("free", rec.handle)

	send(lotsUpTo(0, 199)...)
	rec.waitFor(t, 199)

	if got, want := rec.recorded(), lotsUpTo(0, 199); !reflect.DeepEqual(got, want) {
		t.Errorf("free listener saw lots %v, want 0..199 in order", got)
	}
}

func TestHubStopsDeliveringToUnregisteredListener(t *testing.T) {
	hub, send := startHub(t)
	control := newLotRecorder()
	hub.RegisterBackendListener() <- NewBackendListener("control", control.handle)

	// The listener blocks in its first call, so later states queue up behind it.
	release := make(chan struct{})
	rec := newLotRecorder()
	listener := NewBackendListener("leaving", func(m *MeetApiResponse) {
		rec.handle(m)
		<-release
	})
	hub.RegisterBackendListener() <- listener

	send(lotsUpTo(0, 9)...)
	rec.waitFor(t, 0)
	control.waitFor(t, 9)

	hub.UnregisterBackendListener() <- listener
	<-listener.Done
	close(release)
	select {
	case <-listener.exited:
	case <-time.After(time.Second):
		t.Fatal("listener goroutine did not exit after unregistering")
	}

	send(10)
	control.waitFor(t, 10)
	if got := rec.recorded(); !reflect.DeepEqual(got, []int{0}) {
		t.Errorf("unregistered listener saw lots %v, want only 0", got)
	}
}
