package liftingcast

import (
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
