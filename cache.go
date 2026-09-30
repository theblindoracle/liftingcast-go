package liftingcast

import (
	"encoding/json"
	"sync"
)

// Cache provides thread-safe storage and merging of meet data.
//
// Merge deep-merges nested maps rather than replacing each top-level key, so
// a lifter or attempt deleted upstream stays in the cache until Clear.
//
// Updates are decoded into MeetApiResponse before merging, so a field left
// out of an object the update does send is reset to its zero value. That is
// harmless for LiftingCast, which sends each platform whole along with the
// meet's name, federation and units.
type Cache struct {
	mu   sync.RWMutex
	data *MeetApiResponse
}

// NewCache creates a new cache instance
func NewCache() *Cache {
	return &Cache{}
}

// Merge merges partial update into the cached state
// This mimics the frontend's {...prevData, ...newData} pattern
func (c *Cache) Merge(update *MeetApiResponse) (*MeetApiResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If no existing data, this is the initial state
	if c.data == nil {
		c.data = update
		return c.data, nil
	}

	// Deep merge the update into existing data
	merged, err := deepMergeMeetData(c.data, update)
	if err != nil {
		return nil, err
	}

	c.data = merged
	return c.data, nil
}

// Get returns a copy of the current cached state
func (c *Cache) Get() *MeetApiResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.data == nil {
		return nil
	}

	// Return a deep copy to prevent external modifications
	return copyMeetData(c.data)
}

// Clear removes all cached data
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = nil
}

// deepMergeMeetData performs a deep merge of two MeetApiResponse objects
// The update object's fields override the base object's fields
func deepMergeMeetData(base, update *MeetApiResponse) (*MeetApiResponse, error) {
	// Use JSON marshaling/unmarshaling for deep merge
	// This is the most reliable way to handle complex nested structures in Go

	// Marshal both objects to JSON
	baseJSON, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}

	updateJSON, err := json.Marshal(update)
	if err != nil {
		return nil, err
	}

	// Unmarshal into map[string]interface{} for flexible merging
	var baseMap map[string]interface{}
	if err := json.Unmarshal(baseJSON, &baseMap); err != nil {
		return nil, err
	}

	var updateMap map[string]interface{}
	if err := json.Unmarshal(updateJSON, &updateMap); err != nil {
		return nil, err
	}

	// Perform deep merge
	merged := mergeMaps(baseMap, updateMap)

	// Marshal back to JSON
	mergedJSON, err := json.Marshal(merged)
	if err != nil {
		return nil, err
	}

	// Unmarshal into MeetApiResponse struct
	var result MeetApiResponse
	if err := json.Unmarshal(mergedJSON, &result); err != nil {
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

// copyMeetData creates a deep copy of MeetApiResponse
func copyMeetData(data *MeetApiResponse) *MeetApiResponse {
	// Use JSON marshaling/unmarshaling for deep copy
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil
	}

	var copy MeetApiResponse
	if err := json.Unmarshal(jsonData, &copy); err != nil {
		return nil
	}

	return &copy
}
