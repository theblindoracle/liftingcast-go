package liftingcast

import (
	"encoding/json"
	"testing"
)

func mustMeet(t *testing.T, raw string) *MeetApiResponse {
	t.Helper()
	var m MeetApiResponse
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return &m
}

func TestCacheMergeOfPlatformsOnlyMessageKeepsLifters(t *testing.T) {
	cache := NewCache()

	full := mustMeet(t, `{
		"name": "Test Meet",
		"federation": "USAPL",
		"lifters": {
			"l1": {"id": "l1", "name": "Alice", "lifts": {"squat": {"1": {"id": "a1", "weight": 100, "result": null}}}},
			"l2": {"id": "l2", "name": "Bea"}
		},
		"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "initial"}}
	}`)
	if _, err := cache.Merge(full); err != nil {
		t.Fatalf("merge full: %v", err)
	}

	// Shaped like real LiftingCast traffic: meet fields plus whole platforms.
	platformsOnly := mustMeet(t, `{
		"name": "Test Meet",
		"federation": "USAPL",
		"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "started", "currentAttempt": {"id": "a1", "liftName": "squat", "attemptNumber": "1", "lifter": {"id": "l1"}}}}
	}`)
	merged, err := cache.Merge(platformsOnly)
	if err != nil {
		t.Fatalf("merge platforms-only: %v", err)
	}

	if merged.Lifters == nil || len(*merged.Lifters) != 2 {
		t.Fatalf("lifters = %v, want both lifters kept", merged.Lifters)
	}
	alice := (*merged.Lifters)["l1"]
	if alice.Name == nil || *alice.Name != "Alice" {
		t.Errorf("lifter l1 name = %v, want Alice", alice.Name)
	}
	if w := alice.Lifts[LiftSquat][Attempt1].Weight.Value; w == nil || *w != 100 {
		t.Errorf("lifter l1 squat 1 weight = %v, want 100", w)
	}

	p1 := (*merged.Platforms)["p1"]
	if p1.ClockState == nil || *p1.ClockState != ClockStarted {
		t.Errorf("platform clockState = %v, want started", p1.ClockState)
	}
	if p1.CurrentAttempt == nil || p1.CurrentAttempt.ID != "a1" {
		t.Errorf("platform currentAttempt = %v, want a1", p1.CurrentAttempt)
	}

	if got := cache.Get(); got.Lifters == nil || len(*got.Lifters) != 2 {
		t.Errorf("Get() lifters = %v, want both lifters kept", got.Lifters)
	}
}
