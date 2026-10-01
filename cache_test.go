package liftingcast

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func mustMerge(t *testing.T, cache *Cache, raw string) *MeetState {
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

	if merged.Lifters == nil || len(merged.Lifters) != 2 {
		t.Fatalf("lifters = %v, want both lifters kept", merged.Lifters)
	}
	alice := merged.Lifters["l1"]
	if alice.Name == nil || *alice.Name != "Alice" {
		t.Errorf("lifter l1 name = %v, want Alice", alice.Name)
	}
	if w := alice.Lifts[LiftSquat][Attempt1].Weight.Value; w == nil || *w != 100 {
		t.Errorf("lifter l1 squat 1 weight = %v, want 100", w)
	}

	p1 := merged.Platforms["p1"]
	if p1.ClockState == nil || *p1.ClockState != ClockStarted {
		t.Errorf("platform clockState = %v, want started", p1.ClockState)
	}
	if p1.CurrentAttempt == nil || p1.CurrentAttempt.ID != "a1" {
		t.Errorf("platform currentAttempt = %v, want a1", p1.CurrentAttempt)
	}

	if got := cache.Get(); got.Lifters == nil || len(got.Lifters) != 2 {
		t.Errorf("Get() lifters = %v, want both lifters kept", got.Lifters)
	}
}

func TestCacheMergeReplacesSectionWhole(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, fullMeet)

	merged := mustMerge(t, cache, `{"platforms": {"p1": {"id": "p1", "clockState": "started"}}}`)

	p1 := merged.Platforms["p1"]
	if p1.ClockState == nil || *p1.ClockState != ClockStarted {
		t.Errorf("platform clockState = %v, want started", p1.ClockState)
	}
	if p1.Name != nil {
		t.Errorf("platform name = %q, want it gone with the section it came in", *p1.Name)
	}
}

func TestCacheMergeDoesNotCarryRefereeCardsOver(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, `{"platforms": {"p1": {"id": "p1",
		"refLights": {"left": {"decision": "bad", "cards": {"red": true}}}}}}`)

	merged := mustMerge(t, cache, `{"platforms": {"p1": {"id": "p1",
		"refLights": {"left": {"decision": "bad", "cards": {"blue": true}}}}}}`)

	cards := merged.Platforms["p1"].RefLights.Left.Cards
	if cards == nil || cards.Red != nil || cards.Blue == nil || !*cards.Blue {
		t.Errorf("left cards = %+v, want only blue", cards)
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
		merged.Lifters["l1"] = Lifter{ID: "mutated"}

		got := cache.Get()
		if got.Name != want {
			t.Errorf("Get().Name = %q after mutating Merge's result, want %q", got.Name, want)
		}
		if got.Lifters["l1"].ID != "l1" {
			t.Errorf("Get() lifter l1 = %v after mutating Merge's result", got.Lifters["l1"])
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

	ca := merged.Platforms["p1"].CurrentAttempt
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

	if merged.Lifters == nil || len(merged.Lifters) != 0 {
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

// meetWithDivisions has two of everything that LiftingCast can delete:
// lifters, divisions, and weight classes within a division.
const meetWithDivisions = `{
	"name": "Test Meet",
	"federation": "USAPL",
	"units": "KG",
	"lifters": {
		"l1": {"id": "l1", "name": "Alice", "divisions": [{"divisionId": "d1", "weightClassId": "w1"}]},
		"l2": {"id": "l2", "name": "Bea", "divisions": [{"divisionId": "d1", "weightClassId": "w2"}]}
	},
	"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "initial"}},
	"divisions": {
		"d1": {"id": "d1", "name": "Open", "scoreBy": "TOTAL", "weightClasses": {
			"w1": {"name": "63", "maxWeight": 63},
			"w2": {"name": "69", "maxWeight": 69}}},
		"d2": {"id": "d2", "name": "Junior", "scoreBy": "TOTAL", "weightClasses": {
			"w3": {"name": "63", "maxWeight": 63}}}
	}
}`

// The updates below are shaped like real LiftingCast traffic: each section
// a message carries is complete, so an entry missing from it was deleted.

func TestCacheMergeDropsLifterDeletedUpstream(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, meetWithDivisions)

	merged := mustMerge(t, cache, `{
		"name": "Test Meet",
		"federation": "USAPL",
		"units": "KG",
		"lifters": {
			"l1": {"id": "l1", "name": "Alice", "divisions": [{"divisionId": "d1", "weightClassId": "w1"}]}
		},
		"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "initial"}}
	}`)

	if _, ok := merged.Lifters["l2"]; ok {
		t.Errorf("lifter l2 still in meet state after LiftingCast deleted it")
	}
	if _, ok := merged.Lifters["l1"]; !ok {
		t.Errorf("lifter l1 missing, want it kept")
	}
}

func TestCacheMergeDropsDivisionDeletedUpstream(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, meetWithDivisions)

	merged := mustMerge(t, cache, `{
		"name": "Test Meet",
		"federation": "USAPL",
		"units": "KG",
		"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "initial"}},
		"divisions": {
			"d1": {"id": "d1", "name": "Open", "scoreBy": "TOTAL", "weightClasses": {
				"w1": {"name": "63", "maxWeight": 63},
				"w2": {"name": "69", "maxWeight": 69}}}
		}
	}`)

	if _, ok := merged.Divisions["d2"]; ok {
		t.Errorf("division d2 still in meet state after LiftingCast deleted it")
	}
	if _, ok := merged.Divisions["d1"]; !ok {
		t.Errorf("division d1 missing, want it kept")
	}
}

func TestCacheMergeDropsWeightClassDeletedUpstream(t *testing.T) {
	cache := NewCache()
	mustMerge(t, cache, meetWithDivisions)

	merged := mustMerge(t, cache, `{
		"name": "Test Meet",
		"federation": "USAPL",
		"units": "KG",
		"platforms": {"p1": {"id": "p1", "name": "Platform 1", "clockState": "initial"}},
		"divisions": {
			"d1": {"id": "d1", "name": "Open", "scoreBy": "TOTAL", "weightClasses": {
				"w1": {"name": "63", "maxWeight": 63}}},
			"d2": {"id": "d2", "name": "Junior", "scoreBy": "TOTAL", "weightClasses": {
				"w3": {"name": "63", "maxWeight": 63}}}
		}
	}`)

	wcs := merged.Divisions["d1"].WeightClasses
	if _, ok := wcs["w2"]; ok {
		t.Errorf("weight class w2 still in division d1 after LiftingCast deleted it")
	}
	if _, ok := wcs["w1"]; !ok {
		t.Errorf("weight class w1 missing, want it kept")
	}
}

// replayRecording merges every message of a recording from testdata/ in
// order. It returns the meet state after the last one, and that state as
// LiftingCast last sent it: the latest copy of each section.
func replayRecording(t *testing.T, name string) (merged, latest *MeetState) {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cache := NewCache()
	sections := map[string]json.RawMessage{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(nil, 1<<20)
	for line := 1; scanner.Scan(); line++ {
		var rec struct {
			Msg json.RawMessage `json:"msg"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			t.Fatalf("%s line %d: %v", name, line, err)
		}
		if merged, err = cache.Merge(rec.Msg); err != nil {
			t.Fatalf("%s line %d: merge: %v", name, line, err)
		}
		if err := json.Unmarshal(rec.Msg, &sections); err != nil {
			t.Fatalf("%s line %d: %v", name, line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	data, err := json.Marshal(sections)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &latest); err != nil {
		t.Fatal(err)
	}
	return merged, latest
}

func TestCacheReplayOfRecordedDeletesDropsThem(t *testing.T) {
	merged, latest := replayRecording(t, "lc-traffic-scenarios-2026-10-01.jsonl")

	if !reflect.DeepEqual(merged, latest) {
		t.Error("meet state differs from the latest copy of each section")
	}
	// See the recording's .md for the scenarios behind these IDs.
	if _, ok := merged.Lifters["lvoirlq9n8to"]; ok {
		t.Error("deleted Lifter 53 still in meet state")
	}
	if _, ok := merged.Divisions["dzczfssal7y5"]; ok {
		t.Error("deleted division Men's Masters still in meet state")
	}
	if _, ok := merged.Lifters["lx122y85a58z"]; !ok {
		t.Error("Lifter 55 missing, want them kept")
	}
}

func TestCacheReplayOfRecordedMeetMerges(t *testing.T) {
	merged, latest := replayRecording(t, "lc-traffic-test-meet-2026-09-29.jsonl")

	if !reflect.DeepEqual(merged, latest) {
		t.Error("meet state differs from the latest copy of each section")
	}
	if merged.Platforms == nil || len(merged.Platforms) != 2 {
		t.Errorf("platforms = %v, want two", merged.Platforms)
	}
	if merged.Teams == nil || len(merged.Teams) == 0 {
		t.Error("no teams after replaying the recording")
	}
}

func TestCacheMergeDecodesTeams(t *testing.T) {
	cache := NewCache()

	merged := mustMerge(t, cache, `{"teams": {"Men-Beltless Hero Academia": {"place": 1, "points": 9}}}`)

	want := Teams{"Men-Beltless Hero Academia": {Place: 1, Points: 9}}
	if merged.Teams == nil || !reflect.DeepEqual(merged.Teams, want) {
		t.Errorf("teams = %v, want %v", merged.Teams, want)
	}
}
