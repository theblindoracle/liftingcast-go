package liftingcast

import (
	"encoding/json"
	"errors"
	"maps"
	"sync"
)

// Cache provides thread-safe storage of one meet state, built up from the
// messages LiftingCast sends.
//
// Merge applies each message section by section: every section a message
// carries replaces the cached copy whole, and sections it leaves out keep
// their cached copy. LiftingCast sends each section complete, so an
// entry missing from a section it sends was deleted upstream and disappears
// from the cache (see docs/adr/0002-sections-replace-whole.md). Entries can
// still refer to ones the meet state lacks: LiftingCast sends divisions only
// when one is edited, so a lifter can name a division not yet in Divisions.
type Cache struct {
	mu sync.RWMutex

	// state is the meet state as raw JSON per section; data is the same state
	// encoded, which Get and Merge decode into fresh MeetStates.
	state map[string]json.RawMessage
	data  []byte
}

// NewCache creates a new cache instance
func NewCache() *Cache {
	return &Cache{}
}

// Merge replaces each section a raw JSON message carries in the cached state
// and returns a copy of the result, which the caller may modify. A message
// that is not a JSON object, or that would leave a state not decodable as
// MeetState, is rejected and leaves the cache unchanged.
func (c *Cache) Merge(update []byte) (*MeetState, error) {
	var sections map[string]json.RawMessage
	if err := json.Unmarshal(update, &sections); err != nil {
		return nil, err
	}
	if sections == nil {
		return nil, errors.New("liftingcast: meet update is null")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	merged := make(map[string]json.RawMessage, len(c.state)+len(sections))
	maps.Copy(merged, c.state)
	maps.Copy(merged, sections)

	data, err := json.Marshal(merged)
	if err != nil {
		return nil, err
	}
	result, err := decodeMeetData(data)
	if err != nil {
		return nil, err
	}

	c.state = merged
	c.data = data
	return result, nil
}

// Get returns a copy of the current cached state
func (c *Cache) Get() *MeetState {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.data == nil {
		return nil
	}

	// The cached state was decodable when Merge stored it
	result, _ := decodeMeetData(c.data)
	return result
}

// Clear removes all cached data
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = nil
	c.data = nil
}

func decodeMeetData(data []byte) (*MeetState, error) {
	var result MeetState
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
