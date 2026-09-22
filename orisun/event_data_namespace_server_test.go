//go:build !orisun_embedded

package orisun

import (
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
)

func TestSaveRequestsRejectReservedFieldsBeforeStorage(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "V2"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			saver := &capturePreparedSaver{}
			store := &EventStore{saveEventsFn: saver, logger: noopLogger{}}
			events := []*EventToSave{
				{EventId: "one", EventType: "Created", Data: `{}`, Metadata: `{}`},
				{EventId: "two", EventType: "Created", Data: `{"__future":true}`, Metadata: `{}`},
			}
			var err error
			if legacy {
				_, err = store.SaveEvents(t.Context(), &SaveEventsRequest{Boundary: "orders", Events: events})
			} else {
				_, err = store.SaveEventsV2(t.Context(), &SaveEventsV2Request{Boundary: "orders", Events: events})
			}
			if statuscode.CodeOf(err) != statuscode.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", err)
			}
			if saver.prepared != nil {
				t.Fatal("rejected batch reached storage")
			}
		})
	}
}

func TestRequestPreparationAllowsReservedNamesInMetadata(t *testing.T) {
	_, err := prepareRequestedEventsForSave([]*EventToSave{{
		EventType: "Created", Data: `{"nested":{"__custom":1}}`, Metadata: `{"__trace":"allowed"}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRequestPreparationPreservesJSONNumbers(t *testing.T) {
	const document = `{"positionLike":9223372036854775807,"nested":{"number":9007199254740993},"decimal":0.12345678901234567890123456789}`
	prepared, err := prepareRequestedEventsForSave([]*EventToSave{{
		EventType: "Created", Data: document, Metadata: document,
	}})
	if err != nil {
		t.Fatal(err)
	}
	assertExactJSONFields(t, prepared[0].DataJSON, document)
	assertExactJSONFields(t, prepared[0].MetadataJSON, document)
}

func TestLiveCriteriaDistinguishesEnvelopeAndApplicationEventType(t *testing.T) {
	prepared, err := prepareRequestedEventsForSave([]*EventToSave{{
		EventType: "OrderPlaced", Data: `{"eventType":"domain"}`, Metadata: `{}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	event := &Event{EventId: "id-1", EventType: prepared[0].EventType, Data: prepared[0].DataJSON}
	store := &EventStore{}
	for _, tc := range []struct {
		key, value string
		matches    bool
	}{
		{"__eventType", "OrderPlaced", true},
		{"__eventId", "id-1", true},
		{"__eventId", "wrong", false},
		{"eventType", "domain", true},
		{"eventType", "OrderPlaced", false},
		{"__eventType", "domain", false},
	} {
		query := &Query{Criteria: []*Criterion{{Tags: []*Tag{{Key: tc.key, Value: tc.value}}}}}
		if got := store.eventMatchesQueryCriteria(event, query); got != tc.matches {
			t.Errorf("%s = %s matched %v, want %v", tc.key, tc.value, got, tc.matches)
		}
	}
}
