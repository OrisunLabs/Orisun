package orisun

import (
	"strings"
	"testing"

	"github.com/goccy/go-json"
)

func TestPrepareEventsRejectsReservedRootFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		data any
	}{
		{"map", map[string]any{"__position": 1}},
		{"string", `{"__eventType":"Forged"}`},
		{"bytes", []byte(`{"__futureField":null}`)},
		{"escaped key", `{"\u005f\u005feventId":"forged"}`},
		{"prefix alone", map[string]string{"__": "reserved"}},
		{"struct", struct {
			Value string `json:"__custom"`
		}{Value: "reserved"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A later invalid event must reject the entire batch before storage.
			prepared, err := PrepareEventsForSave([]EventWithMapTags{
				{EventType: "Valid", Data: map[string]any{"id": "one"}},
				{EventType: "Invalid", Data: tc.data},
			})
			if err == nil || !strings.Contains(err.Error(), "event 1 data:") || !strings.Contains(err.Error(), "reserved") {
				t.Fatalf("expected reserved-field error for second event, got %v", err)
			}
			if prepared != nil {
				t.Fatalf("invalid batch returned prepared events: %#v", prepared)
			}
		})
	}
}

func TestPrepareEventsAllowsNestedReservedNames(t *testing.T) {
	input := map[string]any{
		"_custom": "allowed", "customer__id": "allowed",
		"nested": map[string]any{"__custom": "nested"},
		"items":  []any{map[string]any{"__custom": "array"}},
	}
	prepared, err := PrepareEventsForSave([]EventWithMapTags{{
		EventType: "Created", Data: input, Metadata: map[string]any{"__trace": "allowed"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(prepared[0].DataJSON), &data); err != nil {
		t.Fatal(err)
	}
	if data["nested"].(map[string]any)["__custom"] != "nested" ||
		data["items"].([]any)[0].(map[string]any)["__custom"] != "array" ||
		data["_custom"] != "allowed" || data["customer__id"] != "allowed" ||
		data["__eventType"] != nil || data["__eventId"] != nil || prepared[0].EventType != "Created" {
		t.Fatalf("unexpected prepared document: %s", prepared[0].DataJSON)
	}
	if prepared[0].MetadataJSON != `{"__trace":"allowed"}` {
		t.Fatalf("metadata changed: %s", prepared[0].MetadataJSON)
	}
	if _, mutated := input["__eventType"]; mutated {
		t.Fatal("preparation mutated caller data")
	}
}
