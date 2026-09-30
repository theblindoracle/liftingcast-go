package liftingcast

import (
	"encoding/json"
	"testing"
)

func TestNullableFloat64_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *float64
		wantErr  bool
	}{
		{
			name:     "null value",
			input:    `{"value": null}`,
			expected: nil,
		},
		{
			name:     "empty string",
			input:    `{"value": ""}`,
			expected: nil,
		},
		{
			name:     "numeric value",
			input:    `{"value": 123.45}`,
			expected: float64Ptr(123.45),
		},
		{
			name:     "string numeric value",
			input:    `{"value": "123.45"}`,
			expected: float64Ptr(123.45),
		},
		{
			name:     "zero value",
			input:    `{"value": 0}`,
			expected: float64Ptr(0),
		},
		{
			name:     "negative value",
			input:    `{"value": -10.5}`,
			expected: float64Ptr(-10.5),
		},
		{
			name:     "large value",
			input:    `{"value": 999.999}`,
			expected: float64Ptr(999.999),
		},
		{
			name:     "string zero",
			input:    `{"value": "0"}`,
			expected: float64Ptr(0),
		},
		{
			name:     "invalid string becomes nil",
			input:    `{"value": "not-a-number"}`,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result struct {
				Value NullableFloat64 `json:"value"`
			}

			err := json.Unmarshal([]byte(tt.input), &result)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.expected == nil && result.Value.Value != nil {
				t.Errorf("Expected nil, got %v", *result.Value.Value)
			} else if tt.expected != nil && result.Value.Value == nil {
				t.Errorf("Expected %v, got nil", *tt.expected)
			} else if tt.expected != nil && *result.Value.Value != *tt.expected {
				t.Errorf("Expected %v, got %v", *tt.expected, *result.Value.Value)
			}
		})
	}
}

func TestNullableFloat64_MarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		value    *float64
		expected string
	}{
		{
			name:     "nil value",
			value:    nil,
			expected: `{"value":null}`,
		},
		{
			name:     "numeric value",
			value:    float64Ptr(123.45),
			expected: `{"value":123.45}`,
		},
		{
			name:     "zero value",
			value:    float64Ptr(0),
			expected: `{"value":0}`,
		},
		{
			name:     "negative value",
			value:    float64Ptr(-10.5),
			expected: `{"value":-10.5}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := struct {
				Value NullableFloat64 `json:"value"`
			}{
				Value: NullableFloat64{Value: tt.value},
			}

			result, err := json.Marshal(input)
			if err != nil {
				t.Errorf("MarshalJSON() error = %v", err)
				return
			}

			if string(result) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(result))
			}
		})
	}
}

func TestNullableFloat64_HelperMethods(t *testing.T) {
	t.Run("Float64 method", func(t *testing.T) {
		value := float64Ptr(42.5)
		nf := NullableFloat64{Value: value}

		result := nf.Float64()
		if result != value {
			t.Errorf("Float64() should return the same pointer")
		}
	})

	t.Run("ToFloat64Ptr method", func(t *testing.T) {
		value := float64Ptr(42.5)
		nf := NullableFloat64{Value: value}

		result := nf.ToFloat64Ptr()
		if result != value {
			t.Errorf("ToFloat64Ptr() should return the same pointer")
		}
	})

	t.Run("nil value methods", func(t *testing.T) {
		nf := NullableFloat64{Value: nil}

		if nf.Float64() != nil {
			t.Errorf("Float64() should return nil for nil value")
		}
		if nf.ToFloat64Ptr() != nil {
			t.Errorf("ToFloat64Ptr() should return nil for nil value")
		}
	})
}

// Helper function to create float64 pointers for test cases
func float64Ptr(f float64) *float64 {
	return &f
}
