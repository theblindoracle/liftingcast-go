package liftingcast

import (
	"reflect"
	"testing"
)

func mustMerge(t *testing.T, cache *Cache, raw string) *MeetApiResponse {
	t.Helper()
	merged, err := cache.Merge([]byte(raw))
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	return merged
}

const fullMeet = `{
	"name": "Test Meet",
	"federation": "USAPL",
	"units": "KG",
	"lifters": {
		"l1": {"id": "l1", "name": "Alice", "lifts": {"squat": {"1": {"id": "a1", "weight": 100, "result": null}}}},
		"l2": {"id": "l2", "name": "Bea"}
	},
	"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "initial",
		"currentAttempt": {"id": "a1", "liftName": "squat", "attemptNumber": "1", "lifter": {"id": "l1"}}}}
}`

func TestCacheMergeOfPlatformsOnlyMessageKeepsLifters(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, fullMeet)

	// Shaped like real LiftingCast traffic: meet fields plus whole platforms.
	merged := mustMerge(t, cache, `{
		"name": "Test Meet",
		"federation": "USAPL",
		"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "started", "currentAttempt": {"id": "a1", "liftName": "squat", "attemptNumber": "1", "lifter": {"id": "l1"}}}}
	}`)

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

func TestCacheMergeOfPartialPlatformKeepsOmittedFields(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, fullMeet)

	merged := mustMerge(t, cache, `{"platforms": {"p1": {"id": "p1", "clockState": "started"}}}`)

	p1 := (*merged.Platforms)["p1"]
	if p1.ClockState == nil || *p1.ClockState != ClockStarted {
		t.Errorf("platform clockState = %v, want started", p1.ClockState)
	}
	if p1.Name == nil || *p1.Name != "Platform 1" {
		t.Errorf("platform name = %v, want Platform 1", p1.Name)
	}
	if p1.CurrentAttempt == nil || p1.CurrentAttempt.ID != "a1" {
		t.Errorf("platform currentAttempt = %v, want a1", p1.CurrentAttempt)
	}
}

func TestCacheMergeWithoutMeetFieldsKeepsThem(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, fullMeet)

	merged := mustMerge(t, cache, `{"lifters": {"l2": {"id": "l2", "name": "Bea B"}}}`)

	if merged.Name != "Test Meet" {
		t.Errorf("name = %q, want Test Meet", merged.Name)
	}
	if merged.Units == nil || *merged.Units != "KG" {
		t.Errorf("units = %v, want KG", merged.Units)
	}
	if merged.Federation != "USAPL" {
		t.Errorf("federation = %q, want USAPL", merged.Federation)
	}
}

func TestCacheMergeReturnsCopy(t *testing.T) {
	cache := NewCache()
	for _, raw := range []string{fullMeet, `{"name": "Renamed"}`} {
		merged := mustMerge(t, cache, raw)
		want := merged.Name
		merged.Name = "Mutated"
		(*merged.Lifters)["l1"] = Lifter{ID: "mutated"}

		got := cache.Get()
		if got.Name != want {
			t.Errorf("Get().Name = %q after mutating Merge's result, want %q", got.Name, want)
		}
		if (*got.Lifters)["l1"].ID != "l1" {
			t.Errorf("Get() lifter l1 = %v after mutating Merge's result", (*got.Lifters)["l1"])
		}
	}
}

func TestCacheMergeReplacesIfSuccessfulMapsWhole(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, `{"platforms": {"p1": {"id": "p1", "currentAttempt": {"id": "a1",
		"ifSuccessfulScores": {"l1": 300, "l2": 250},
		"ifSuccessfulPlaces": {"l1": 1, "l2": 2}}}}}`)

	merged := mustMerge(t, cache, `{"platforms": {"p1": {"id": "p1", "currentAttempt": {"id": "a1",
		"ifSuccessfulScores": {"l3": 200},
		"ifSuccessfulPlaces": {"l3": 3}}}}}`)

	ca := (*merged.Platforms)["p1"].CurrentAttempt
	if ca == nil {
		t.Fatal("currentAttempt = nil")
	}
	if want := map[string]float64{"l3": 200}; !reflect.DeepEqual(ca.IfSuccessfulScores, want) {
		t.Errorf("ifSuccessfulScores = %v, want %v", ca.IfSuccessfulScores, want)
	}
	if want := map[string]int{"l3": 3}; !reflect.DeepEqual(ca.IfSuccessfulPlaces, want) {
		t.Errorf("ifSuccessfulPlaces = %v, want %v", ca.IfSuccessfulPlaces, want)
	}
}

func TestCacheMergeOfEmptyMapReplaces(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, fullMeet)

	merged := mustMerge(t, cache, `{"lifters": {}}`)

	if merged.Lifters == nil || len(*merged.Lifters) != 0 {
		t.Errorf("lifters = %v, want empty", merged.Lifters)
	}
}

func TestCacheMergeRejectsInvalidJSON(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, fullMeet)

	if _, err := cache.Merge([]byte(`not json`)); err == nil {
		t.Error("Merge(invalid) err = nil, want error")
	}
	if got := cache.Get(); got.Name != "Test Meet" {
		t.Errorf("Get().Name = %q after rejected merge, want Test Meet", got.Name)
	}
}
