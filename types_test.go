package liftingcast

import (
	"encoding/json"
	"testing"
)

func TestRecordDataDecodesUpdatedRecordWeight(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  *float64
	}{
		{name: "unjudged", input: `{"lift":"T","recordWeight":582.5,"updatedRecordWeight":null}`, want: nil},
		{name: "judged good", input: `{"lift":"T","recordWeight":582.5,"updatedRecordWeight":595}`, want: float64Ptr(595)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rec RecordData
			if err := json.Unmarshal([]byte(tt.input), &rec); err != nil {
				t.Fatal(err)
			}
			if (rec.UpdatedRecordWeight == nil) != (tt.want == nil) ||
				(tt.want != nil && *rec.UpdatedRecordWeight != *tt.want) {
				t.Fatalf("UpdatedRecordWeight = %v, want %v", rec.UpdatedRecordWeight, tt.want)
			}
		})
	}
}
