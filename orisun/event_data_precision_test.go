package orisun

import (
	"testing"

	"github.com/goccy/go-json"
)

func TestPrepareEventsPreservesJSONNumbers(t *testing.T) {
	const document = `{"large":9223372036854775807,"small":-9223372036854775808,"nested":{"number":9007199254740993},"items":[18446744073709551615],"decimal":0.12345678901234567890123456789}`
	for _, tc := range []struct {
		name string
		data any
	}{
		{"string", document},
		{"bytes", []byte(document)},
		{"raw message", json.RawMessage(document)},
		{"map", map[string]any{
			"large": int64(9223372036854775807), "small": int64(-9223372036854775808),
			"nested": map[string]any{"number": int64(9007199254740993)},
			"items":  []uint64{18446744073709551615}, "decimal": json.Number("0.12345678901234567890123456789"),
		}},
		{"struct", struct {
			Large   int64            `json:"large"`
			Small   int64            `json:"small"`
			Nested  map[string]int64 `json:"nested"`
			Items   []uint64         `json:"items"`
			Decimal json.Number      `json:"decimal"`
		}{9223372036854775807, -9223372036854775808, map[string]int64{"number": 9007199254740993}, []uint64{18446744073709551615}, "0.12345678901234567890123456789"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := PrepareEventsForSave([]EventWithMapTags{{
				EventType: "Created", Data: tc.data, Metadata: tc.data,
			}})
			if err != nil {
				t.Fatal(err)
			}
			assertExactJSONFields(t, prepared[0].DataJSON, document)
			assertExactJSONFields(t, prepared[0].MetadataJSON, document)
			// PostgreSQL receives the marshaled prepared batch, so verify that
			// serialization keeps exact values as well as preparation itself.
			encoded, err := json.Marshal(prepared)
			if err != nil {
				t.Fatal(err)
			}
			var wire []struct {
				Data     json.RawMessage `json:"data"`
				Metadata json.RawMessage `json:"metadata"`
			}
			if err := json.Unmarshal(encoded, &wire); err != nil {
				t.Fatal(err)
			}
			assertExactJSONFields(t, string(wire[0].Data), document)
			assertExactJSONFields(t, string(wire[0].Metadata), document)
		})
	}
}

// Comparing raw values avoids a float64-based assertion masking rounding.
func assertExactJSONFields(t *testing.T, got, want string) {
	t.Helper()
	var actual, expected map[string]json.RawMessage
	if err := json.Unmarshal([]byte(got), &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &expected); err != nil {
		t.Fatal(err)
	}
	for key, value := range expected {
		if string(actual[key]) != string(value) {
			t.Errorf("field %q = %s, want %s", key, actual[key], value)
		}
	}
}
