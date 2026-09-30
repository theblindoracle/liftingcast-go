package liftingcast

import (
	"encoding/json"
	"strconv"
)

// NullableFloat64 handles JSON unmarshalling of float64 values that may be
// null, numeric values, or empty strings. Empty strings are treated as nil.
type NullableFloat64 struct {
	Value *float64
}

// UnmarshalJSON implements custom unmarshalling logic to handle empty strings,
// null values, and numeric values (both quoted and unquoted).
func (nf *NullableFloat64) UnmarshalJSON(data []byte) error {
	// Handle null
	if string(data) == "null" {
		nf.Value = nil
		return nil
	}

	// Handle empty string - treat as nil
	if string(data) == `""` || string(data) == `''` {
		nf.Value = nil
		return nil
	}

	// Handle numeric value (quoted or unquoted)
	var raw interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	switch v := raw.(type) {
	case float64:
		nf.Value = &v
		return nil
	case string:
		// Try to parse string as float
		if v == "" {
			nf.Value = nil
			return nil
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			// If it's not a valid number, treat as nil
			nf.Value = nil
			return nil
		}
		nf.Value = &f
		return nil
	default:
		nf.Value = nil
		return nil
	}
}

// MarshalJSON implements custom marshalling - preserves nil as null
func (nf NullableFloat64) MarshalJSON() ([]byte, error) {
	if nf.Value == nil {
		return []byte("null"), nil
	}
	return json.Marshal(*nf.Value)
}

// Float64 returns the underlying *float64 value
func (nf NullableFloat64) Float64() *float64 {
	return nf.Value
}

// ToFloat64Ptr converts NullableFloat64 to *float64 for convenience
func (nf NullableFloat64) ToFloat64Ptr() *float64 {
	return nf.Value
}
