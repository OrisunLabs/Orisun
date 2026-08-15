package orisun

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
)

type capturePreparedSaver struct {
	prepared PreparedEventBatch
}

type captureEventsRetriever struct {
	batch       ReadEventBatch
	latestBatch LatestByCriteriaBatch
	latestQuery LatestByCriteriaQuery
	calls       int
}

func (r *captureEventsRetriever) GetBatch(_ context.Context, _ *GetEventsRequest) (ReadEventBatch, error) {
	r.calls++
	return r.batch, nil
}

func (r *captureEventsRetriever) GetLatestByCriteria(_ context.Context, query LatestByCriteriaQuery) (LatestByCriteriaBatch, error) {
	r.latestQuery = query
	return r.latestBatch, nil
}

func (s *capturePreparedSaver) SavePrepared(_ context.Context, events PreparedEventBatch, _ string, _ *Position, _ *Query) (string, int64, error) {
	s.prepared = events
	return "7", 8, nil
}

func TestOrisunServerSaveEventsAddsEventTypeToData(t *testing.T) {
	saver := &capturePreparedSaver{}
	server := &OrisunServer{eventStore: &EventStore{}, saveEvents: saver}

	pos, err := server.SaveEvents(context.Background(), []EventWithMapTags{
		{
			EventId:   "event-1",
			EventType: "OrderPlaced",
			Data: map[string]any{
				"order_id":  "order-1",
				"eventType": "stale",
			},
			Metadata: map[string]any{},
		},
	}, "orders", nil, nil)
	if err != nil {
		t.Fatalf("SaveEvents returned error: %v", err)
	}
	if pos.CommitPosition != 7 || pos.PreparePosition != 8 {
		t.Fatalf("unexpected position: %+v", pos)
	}
	if len(saver.prepared) != 1 {
		t.Fatalf("expected one saved event, got %d", len(saver.prepared))
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(saver.prepared[0].DataJSON), &data); err != nil {
		t.Fatalf("prepared data is invalid JSON: %v", err)
	}
	if data["eventType"] != "OrderPlaced" {
		t.Fatalf("expected canonical eventType in data, got %v", data["eventType"])
	}
	if data["order_id"] != "order-1" {
		t.Fatalf("expected original data to be preserved, got %v", data["order_id"])
	}
}

func TestOrisunServerSaveEventsUsesPreparedBatch(t *testing.T) {
	saver := &capturePreparedSaver{}
	server := &OrisunServer{eventStore: &EventStore{}, saveEvents: saver}

	_, err := server.SaveEvents(context.Background(), []EventWithMapTags{{
		EventId:   "event-1",
		EventType: "OrderPlaced",
		Data:      `{"order_id":"order-1","eventType":"stale"}`,
		Metadata:  `{"trace_id":"trace-1"}`,
	}}, "orders", nil, nil)
	if err != nil {
		t.Fatalf("SaveEvents returned error: %v", err)
	}
	if len(saver.prepared) != 1 {
		t.Fatalf("expected one prepared event, got %d", len(saver.prepared))
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(saver.prepared[0].DataJSON), &data); err != nil {
		t.Fatalf("prepared data is invalid JSON: %v", err)
	}
	if data["eventType"] != "OrderPlaced" || data["order_id"] != "order-1" {
		t.Fatalf("unexpected prepared data: %v", data)
	}
	if saver.prepared[0].MetadataJSON != `{"trace_id":"trace-1"}` {
		t.Fatalf("metadata was unnecessarily rewritten: %s", saver.prepared[0].MetadataJSON)
	}
}

func TestPrepareEventsForSaveOwnsByteInput(t *testing.T) {
	data := []byte(`{"value":1}`)
	metadata := []byte(`{"source":"test"}`)
	prepared, err := PrepareEventsForSave([]EventWithMapTags{{
		EventId: "event-1", EventType: "Created", Data: data, Metadata: metadata,
	}})
	if err != nil {
		t.Fatalf("PrepareEventsForSave returned error: %v", err)
	}
	data[2] = 'X'
	metadata[2] = 'X'
	var got map[string]any
	if err := json.Unmarshal([]byte(prepared[0].DataJSON), &got); err != nil {
		t.Fatalf("prepared data is invalid JSON: %v", err)
	}
	if got["value"] != float64(1) {
		t.Fatalf("prepared data affected by caller mutation: %s", prepared[0].DataJSON)
	}
	if prepared[0].MetadataJSON != `{"source":"test"}` {
		t.Fatalf("prepared metadata affected by caller mutation: %s", prepared[0].MetadataJSON)
	}
}

func TestOrisunServerGetEventsReturnsPackedBatch(t *testing.T) {
	created := time.Unix(1_700_000_000, 123).UTC()
	retriever := &captureEventsRetriever{batch: ReadEventBatch{{
		EventId:         "event-1",
		EventType:       "OrderPlaced",
		Data:            `{"eventType":"OrderPlaced","orderId":"order-1"}`,
		Metadata:        `{}`,
		CommitPosition:  7,
		PreparePosition: 8,
		DateCreated:     created,
	}}}
	server := &OrisunServer{eventStore: &EventStore{}, getEvents: retriever}

	batch, err := server.GetEvents(context.Background(), &GetEventsRequest{Count: 1})
	if err != nil {
		t.Fatalf("GetEvents returned error: %v", err)
	}
	if len(batch) != 1 || batch[0].CommitPosition != 7 || batch[0].PreparePosition != 8 {
		t.Fatalf("unexpected packed response: %+v", batch)
	}
	if got := batch[0].DateCreated; !got.Equal(created) {
		t.Fatalf("unexpected creation time: %v", got)
	}
	if batch[0].EventType != "OrderPlaced" {
		t.Fatalf("unexpected event type: %q", batch[0].EventType)
	}
	var publicData map[string]any
	if err := json.Unmarshal([]byte(batch[0].Data), &publicData); err != nil {
		t.Fatal(err)
	}
	if publicData["orderId"] != "order-1" {
		t.Fatalf("unexpected public data: %#v", publicData)
	}
	if _, leaked := publicData["eventType"]; leaked {
		t.Fatalf("storage eventType leaked into public data: %#v", publicData)
	}
	if retriever.batch[0].Data != `{"eventType":"OrderPlaced","orderId":"order-1"}` {
		t.Fatalf("public translation mutated backend batch: %s", retriever.batch[0].Data)
	}
}

func TestEventStoreGetEventsRejectsOversizedReadBeforeBackend(t *testing.T) {
	retriever := &captureEventsRetriever{}
	store := &EventStore{getEventsFn: retriever, logger: noopLogger{}}

	_, err := store.GetEvents(context.Background(), &GetEventsRequest{Count: MaxReadBatchSize + 1})
	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
	if retriever.calls != 0 {
		t.Fatalf("backend was called %d times", retriever.calls)
	}
}

func TestOrisunServerGetLatestByCriteriaUsesPackedTypes(t *testing.T) {
	retriever := &captureEventsRetriever{latestBatch: LatestByCriteriaBatch{
		Matches: []LatestCriterionMatch{{
			Found: true,
			Event: ReadEvent{EventId: "event-1", EventType: "OrderPlaced", Data: `{"eventType":"OrderPlaced","orderId":"order-1"}`, CommitPosition: 7, PreparePosition: 8},
		}},
		ContextCommitPosition:  7,
		ContextPreparePosition: 8,
	}}
	server := &OrisunServer{eventStore: &EventStore{}, getEvents: retriever}
	query := LatestByCriteriaQuery{
		Boundary: "orders",
		Criteria: []ReadCriterion{{Tags: []ReadTag{{Key: "order_id", Value: "order-1"}}}},
	}

	batch, err := server.GetLatestByCriteria(context.Background(), query)
	if err != nil {
		t.Fatalf("GetLatestByCriteria returned error: %v", err)
	}
	if len(batch.Matches) != 1 || !batch.Matches[0].Found || batch.Matches[0].Event.EventId != "event-1" {
		t.Fatalf("unexpected packed latest result: %+v", batch)
	}
	var publicData map[string]any
	if err := json.Unmarshal([]byte(batch.Matches[0].Event.Data), &publicData); err != nil {
		t.Fatal(err)
	}
	if publicData["orderId"] != "order-1" {
		t.Fatalf("unexpected public latest data: %#v", publicData)
	}
	if _, leaked := publicData["eventType"]; leaked {
		t.Fatalf("storage eventType leaked into public latest data: %#v", publicData)
	}
	if retriever.latestQuery.Boundary != "orders" || retriever.latestQuery.Criteria[0].Tags[0].Value != "order-1" {
		t.Fatalf("unexpected packed latest query: %+v", retriever.latestQuery)
	}
}
