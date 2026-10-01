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
	hub := newTestHub(srv.url)
	t.Cleanup(hub.Close)

	states := make(chan *MeetState, 10)
	hub.Listen(func(m *MeetState) { states <- m })

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
			p1 := m.Platforms["p1"]
			if p1.ClockState == nil || *p1.ClockState != ClockStarted {
				continue
			}
			if m.Lifters == nil || m.Lifters["l1"].ID != "l1" {
				t.Fatalf("merged state lost lifters: %v", m.Lifters)
			}
			if !hub.Status().Connected {
				t.Error("status not connected")
			}
			return
		case <-deadline:
			t.Fatal("listener never received the merged platforms update")
		}
	}
}

func TestHubConnectReplacesTheUpstreamConnection(t *testing.T) {
	srv := newFakeServer(t)
	hub := NewHub()
	t.Cleanup(hub.Close)
	states := make(chan *MeetState, 10)
	hub.Listen(func(m *MeetState) { states <- m })
	next := func() *MeetState {
		t.Helper()
		select {
		case m := <-states:
			return m
		case <-time.After(time.Second):
			t.Fatal("listener received no meet state")
			return nil
		}
	}

	hub.Connect(Config{BaseURL: srv.url, MeetID: "a"})
	connA := srv.accept(t, time.Second)
	if err := connA.WriteMessage(websocket.TextMessage, []byte(`{"name": "Meet A", "lifters": {"l1": {"id": "l1"}}}`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	if m := next(); m.Name != "Meet A" {
		t.Fatalf("name = %q, want Meet A", m.Name)
	}

	hub.Connect(Config{BaseURL: srv.url, MeetID: "b"})
	connB := srv.accept(t, time.Second)
	if err := connB.WriteMessage(websocket.TextMessage, []byte(`{"name": "Meet B"}`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	m := next()
	if m.Name != "Meet B" {
		t.Fatalf("name = %q, want Meet B", m.Name)
	}
	if m.Lifters != nil {
		t.Errorf("lifters = %v, want none carried over from Meet A", m.Lifters)
	}
	if status := hub.Status(); status.MeetID != "b" || status.MeetName != "Meet B" {
		t.Errorf("status = %+v, want meet b", status)
	}

	hub.Disconnect()
	if status := hub.Status(); status != (HubStatus{}) {
		t.Errorf("status = %+v after Disconnect, want the zero status", status)
	}
}

// startHub returns a hub connected to a fake server, and a function that
// sends the server's connection one message per lot number, each setting
// lifter l1's lot so listeners can tell the messages apart.
func startHub(t *testing.T) (*Hub, func(lots ...int)) {
	t.Helper()
	hub, send, _ := startHubConn(t)
	return hub, send
}

// startHubConn is startHub that also returns the fake server and the hub's
// upstream connection to it.
func startHubConn(t *testing.T) (*Hub, func(lots ...int), *websocket.Conn) {
	t.Helper()
	srv := newFakeServer(t)
	hub := newTestHub(srv.url)
	t.Cleanup(hub.Close)

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
	return hub, send, conn
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

func (r *lotRecorder) handle(m *MeetState) {
	lot := *m.Lifters["l1"].Lot
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

// blockingRecorder returns a handler that records each state with rec and
// then blocks in its first call until release is closed, so later states
// queue up behind it.
func blockingRecorder(rec *lotRecorder, release <-chan struct{}) func(*MeetState) {
	var once sync.Once
	return func(m *MeetState) {
		rec.handle(m)
		once.Do(func() { <-release })
	}
}

// expectOnly fails unless rec has seen exactly lots once the states the
// control listener has seen up to lot have had time to reach rec too.
func expectOnly(t *testing.T, rec, control *lotRecorder, lot int, lots []int) {
	t.Helper()
	control.waitFor(t, lot)
	time.Sleep(20 * time.Millisecond)
	if got := rec.recorded(); !reflect.DeepEqual(got, lots) {
		t.Errorf("stopped listener saw lots %v, want only %v", got, lots)
	}
}

// inBackground calls f in a new goroutine and returns a channel closed once
// it returns.
func inBackground(f func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	return done
}

// expectBlocked fails with msg if done closes within a short while.
func expectBlocked(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(msg)
	case <-time.After(50 * time.Millisecond):
	}
}

// expectDone fails with msg unless done closes within a second.
func expectDone(t *testing.T, done <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal(msg)
	}
}

func TestHubDeliversStatesToListenerInOrder(t *testing.T) {
	hub, send := startHub(t)
	rec := newLotRecorder()
	hub.Listen(rec.handle)

	send(lotsUpTo(0, 199)...)
	rec.waitFor(t, 199)

	if got, want := rec.recorded(), lotsUpTo(0, 199); !reflect.DeepEqual(got, want) {
		t.Errorf("listener saw lots %v, want 0..199 in order", got)
	}
}

func TestHubSendsCachedStateBeforeLaterStates(t *testing.T) {
	hub, send := startHub(t)
	first := newLotRecorder()
	hub.Listen(first.handle)
	send(lotsUpTo(0, 49)...)
	first.waitFor(t, 49)

	go send(lotsUpTo(50, 199)...)
	late := newLotRecorder()
	hub.Listen(late.handle)
	late.waitFor(t, 199)

	got := late.recorded()
	if got[0] < 49 {
		t.Fatalf("late listener's first state has lot %d, want the cached state (lot >= 49)", got[0])
	}
	if want := lotsUpTo(got[0], 199); !reflect.DeepEqual(got, want) {
		t.Errorf("late listener saw lots %v, want %d..199 in order", got, got[0])
	}
}

func TestHubSendsExactlyCachedStateThenLaterStates(t *testing.T) {
	hub, send := startHub(t)
	first := newLotRecorder()
	hub.Listen(first.handle)
	send(lotsUpTo(0, 49)...)
	first.waitFor(t, 49)

	// Listen has registered the listener by the time it returns, so the
	// cached 49 is all it can have been handed before 50 is sent.
	late := newLotRecorder()
	hub.Listen(late.handle)
	send(lotsUpTo(50, 99)...)
	late.waitFor(t, 99)

	if got, want := late.recorded(), lotsUpTo(49, 99); !reflect.DeepEqual(got, want) {
		t.Errorf("late listener saw lots %v, want the cached 49 then 50..99 in order", got)
	}
}

func TestHubKeepsDeliveringWhileOneListenerBlocks(t *testing.T) {
	hub, send := startHub(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	hub.Listen(func(*MeetState) { <-release })
	rec := newLotRecorder()
	hub.Listen(rec.handle)

	send(lotsUpTo(0, 199)...)
	rec.waitFor(t, 199)

	if got, want := rec.recorded(), lotsUpTo(0, 199); !reflect.DeepEqual(got, want) {
		t.Errorf("free listener saw lots %v, want 0..199 in order", got)
	}
}

func TestHubStopsDeliveringToStoppedListener(t *testing.T) {
	hub, send := startHub(t)
	control := newLotRecorder()
	hub.Listen(control.handle)
	release := make(chan struct{})
	rec := newLotRecorder()
	stop := hub.Listen(blockingRecorder(rec, release))

	send(lotsUpTo(0, 9)...)
	rec.waitFor(t, 0)
	control.waitFor(t, 9)

	// stop waits for the call already in progress, and none of the queued
	// states is handed over after it returns.
	stopped := inBackground(stop)
	expectBlocked(t, stopped, "stop returned while the handler was still running")
	close(release)
	expectDone(t, stopped, "stop did not return once the handler finished")
	send(10)
	expectOnly(t, rec, control, 10, []int{0})
}

func TestHubStopTwiceIsHarmless(t *testing.T) {
	hub, send := startHub(t)
	control := newLotRecorder()
	hub.Listen(control.handle)
	rec := newLotRecorder()
	stop := hub.Listen(rec.handle)

	send(0)
	rec.waitFor(t, 0)
	stop()
	stop()

	send(1)
	expectOnly(t, rec, control, 1, []int{0})
}

func TestHubStopFromInsideHandler(t *testing.T) {
	hub, send := startHub(t)
	control := newLotRecorder()
	hub.Listen(control.handle)

	rec := newLotRecorder()
	stops := make(chan func(), 1)
	returned := make(chan struct{})
	var once sync.Once
	stops <- hub.Listen(func(m *MeetState) {
		rec.handle(m)
		once.Do(func() {
			(<-stops)()
			close(returned)
		})
	})

	send(lotsUpTo(0, 9)...)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("stop called from inside the handler did not return")
	}
	expectOnly(t, rec, control, 9, []int{0})
}

// A listener added while states are still on their way to the Hub's loop
// must not be handed a cached state newer than the states queued behind it.
func TestHubLateListenerNeverSeesStatesGoBackwards(t *testing.T) {
	// Whether the states or the new listener reach the loop first varies,
	// so try several times.
	for round := 0; round < 10; round++ {
		hub, send := startHub(t)
		send(lotsUpTo(0, 5)...)

		rec := newLotRecorder()
		hub.Listen(rec.handle)
		rec.waitFor(t, 5)

		got := rec.recorded()
		if want := lotsUpTo(got[0], 5); !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d: listener saw lots %v, want %d..5 in order", round, got, got[0])
		}
	}
}

func TestHubReportsDisconnectedAfterDropEvenWithStatesQueued(t *testing.T) {
	hub, send, conn := startHubConn(t)

	// The listener blocks in its first call, so the later states are still
	// waiting to be handed to it when the connection drops.
	release := make(chan struct{})
	rec := newLotRecorder()
	hub.Listen(blockingRecorder(rec, release))
	send(lotsUpTo(0, 5)...)
	rec.waitFor(t, 0)
	time.Sleep(50 * time.Millisecond)
	conn.Close()

	waitForHubStatus(t, hub, func(s HubStatus) bool { return !s.Connected })
	close(release)
	rec.waitFor(t, 5)

	if status := hub.Status(); status.Connected {
		t.Errorf("status = %+v after the drop, want disconnected until the client reconnects", status)
	}
}

func TestHubReportsRejection(t *testing.T) {
	srv := newFakeServer(t)
	hub := newTestHub(srv.url)
	t.Cleanup(hub.Close)
	replay(t, srv.accept(t, time.Second), loadRecording(t, "lc-wrong-password-hosted.jsonl")[0])

	status := waitForHubStatus(t, hub, func(s HubStatus) bool { return s.Rejected })
	if status.Connected || status.LastError != "server error: "+badCredentialsError {
		t.Errorf("status = %+v, want disconnected with the server error", status)
	}
}

func TestHubCloseStopsEveryListener(t *testing.T) {
	hub, send, conn := startHubConn(t)
	idle := newLotRecorder()
	hub.Listen(idle.handle)
	release := make(chan struct{})
	busy := newLotRecorder()
	hub.Listen(blockingRecorder(busy, release))

	send(lotsUpTo(0, 9)...)
	busy.waitFor(t, 0)
	idle.waitFor(t, 9)

	// Close waits for the call already in progress, and none of the queued
	// states is handed over after it returns.
	closed := inBackground(hub.Close)
	expectBlocked(t, closed, "Close returned while a handler was still running")
	close(release)
	expectDone(t, closed, "Close did not return once the handler finished")
	time.Sleep(20 * time.Millisecond)
	if got := busy.recorded(); !reflect.DeepEqual(got, []int{0}) {
		t.Errorf("listener saw lots %v after Close, want only 0", got)
	}

	// Close also closes the upstream connection.
	conn.SetReadDeadline(time.Now().Add(time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
				t.Fatal("upstream connection still open after Close")
			}
			break
		}
	}
	if got := idle.recorded(); !reflect.DeepEqual(got, lotsUpTo(0, 9)) {
		t.Errorf("idle listener saw lots %v, want 0..9", got)
	}
}

func TestHubCloseFromInsideHandler(t *testing.T) {
	hub, send := startHub(t)
	returned := make(chan struct{})
	var once sync.Once
	hub.Listen(func(*MeetState) {
		once.Do(func() {
			hub.Close()
			close(returned)
		})
	})

	send(0)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Close called from inside a handler did not return")
	}
	if status := hub.Status(); status != (HubStatus{}) {
		t.Errorf("status = %+v after Close, want the zero status", status)
	}
}

// Handlers that stop each other's listeners at the same time must not each
// wait for the other to finish.
func TestHubHandlersStoppingEachOtherDontDeadlock(t *testing.T) {
	hub, send := startHub(t)
	var stopA, stopB func()
	ready := make(chan struct{})
	bothIn := sync.WaitGroup{}
	bothIn.Add(2)
	returned := make(chan struct{}, 2)
	crossStop := func(other *func()) func(*MeetState) {
		var once sync.Once
		return func(*MeetState) {
			once.Do(func() {
				<-ready
				bothIn.Done()
				bothIn.Wait()
				(*other)()
				returned <- struct{}{}
			})
		}
	}
	stopA = hub.Listen(crossStop(&stopB))
	stopB = hub.Listen(crossStop(&stopA))
	close(ready)

	send(0)
	for i := 0; i < 2; i++ {
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Fatal("handlers stopping each other deadlocked")
		}
	}
}

// Two handlers closing the Hub at the same time must not each wait for the
// other to finish.
func TestHubHandlersClosingTogetherDontDeadlock(t *testing.T) {
	hub, send := startHub(t)
	var bothIn sync.WaitGroup
	bothIn.Add(2)
	returned := make(chan struct{}, 2)
	closeOnce := func() func(*MeetState) {
		var once sync.Once
		return func(*MeetState) {
			once.Do(func() {
				bothIn.Done()
				bothIn.Wait()
				hub.Close()
				returned <- struct{}{}
			})
		}
	}
	hub.Listen(closeOnce())
	hub.Listen(closeOnce())

	send(0)
	for i := 0; i < 2; i++ {
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Fatal("handlers closing the hub together deadlocked")
		}
	}
}

func TestHubDoesNothingAfterClose(t *testing.T) {
	srv := newFakeServer(t)
	hub := newTestHub(srv.url)
	t.Cleanup(hub.Close)
	conn := srv.accept(t, time.Second)
	first := newLotRecorder()
	hub.Listen(first.handle)
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"name": "Test Meet", "lifters": {"l1": {"id": "l1", "lot": 0}}}`)); err != nil {
		t.Fatalf("send: %v", err)
	}
	first.waitFor(t, 0)
	hub.Close()

	called := make(chan struct{}, 1)
	stop := hub.Listen(func(*MeetState) { called <- struct{}{} })
	hub.Connect(testConfig(srv.url))
	srv.expectNoAttempt(t, 100*time.Millisecond)
	hub.Disconnect()
	stop()
	stop()

	select {
	case <-called:
		t.Error("listener added after Close was called")
	default:
	}
	if status := hub.Status(); status != (HubStatus{}) {
		t.Errorf("status = %+v after Close, want the zero status", status)
	}
}

func TestHubCloseTwiceIsHarmless(t *testing.T) {
	hub := NewHub()
	hub.Close()
	hub.Close()
}

// newTestHub returns a hub connected to url
func newTestHub(url string) *Hub {
	hub := NewHub()
	hub.Connect(testConfig(url))
	return hub
}

// waitForHubStatus waits until the hub's status satisfies ok.
func waitForHubStatus(t *testing.T, hub *Hub, ok func(HubStatus) bool) HubStatus {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		status := hub.Status()
		if ok(status) {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("status stayed %+v", status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
