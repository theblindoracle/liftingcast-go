package liftingcast

import (
	"encoding/json"
	"errors"
	"sync"
)

// Cache provides thread-safe storage and merging of meet data.
//
// Merge deep-merges each update as it arrived on the wire, so only the keys a
// message sends change the cached state. Nested maps merge rather than
// replace, so a lifter or attempt deleted upstream stays in the cache until
// Clear.
type Cache struct {
	mu sync.RWMutex

	// state is the merged meet state as decoded JSON; data is the same state
	// encoded, which Get and Merge decode into fresh MeetApiResponses.
	state map[string]interface{}
	data  []byte
}

// NewCache creates a new cache instance
func NewCache() *Cache {
	return &Cache{}
}

// Merge deep-merges a raw JSON message into the cached state and returns a
// copy of the result, which the caller may modify. A message that is not a
// JSON object, or that would leave a state not decodable as MeetApiResponse,
// is rejected and leaves the cache unchanged.
func (c *Cache) Merge(update []byte) (*MeetApiResponse, error) {
	var updateMap map[string]interface{}
	if err := json.Unmarshal(update, &updateMap); err != nil {
		return nil, err
	}
	if updateMap == nil {
		return nil, errors.New("liftingcast: meet update is null")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	merged := updateMap
	if c.state != nil {
		merged = mergeMaps(c.state, updateMap)
	}

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
func (c *Cache) Get() *MeetApiResponse {
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

func decodeMeetData(data []byte) (*MeetApiResponse, error) {
	var result MeetApiResponse
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// mergeMaps recursively merges two maps
func mergeMaps(base, update map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Copy all base values
	for k, v := range base {
		result[k] = v
	}

	// Merge/override with update values
	for k, updateValue := range update {
		if baseValue, exists := result[k]; exists {
			// If both are maps, merge them recursively
			// baseValue.(map[string]interface{}) is a type assertion
			if baseMap, baseIsMap := baseValue.(map[string]interface{}); baseIsMap {
				if updateMap, updateIsMap := updateValue.(map[string]interface{}); updateIsMap {
					// If update map is empty, overwrite base with empty map
					if len(updateMap) == 0 {
						result[k] = updateMap
						continue
					}
					if k == "ifSuccessfulScores" || k == "ifSuccessfulPlaces" {
						result[k] = updateMap
						continue
					}
					result[k] = mergeMaps(baseMap, updateMap)
					continue
				}
			}
		}
		// Otherwise, update value overrides base value
		result[k] = updateValue
	}

	return result
}
