package sqlite

import (
	"context"
	"strconv"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

func preparedGCEvents(t *testing.T, events ...eventstore.EventWithMapTags) eventstore.PreparedEventBatch {
	t.Helper()
	prepared, err := eventstore.PrepareEventsForSave(events)
	if err != nil {
		t.Fatalf("prepare events: %v", err)
	}
	return prepared
}

func gcConsistency(position eventstore.Position, criteria ...eventstore.ReadCriterion) []eventstore.ConsistencyCheck {
	return []eventstore.ConsistencyCheck{{Criteria: criteria, Position: position}}
}

func gcReadCriterion(tags ...eventstore.ReadTag) eventstore.ReadCriterion {
	return eventstore.ReadCriterion{Tags: tags}
}

func TestGroupCommitUnconditionalPreservesRequestPositions(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	requests := []*sqliteSaveRequest{
		{
			ctx: context.Background(),
			inserts: preparedGCEvents(t,
				mustEvent(t, "A1", map[string]any{"request": "a"}, map[string]any{}),
				mustEvent(t, "A2", map[string]any{"request": "a"}, map[string]any{}),
			),
			result: make(chan sqliteSaveResult, 1),
		},
		{
			ctx: context.Background(),
			inserts: preparedGCEvents(t,
				mustEvent(t, "B1", map[string]any{"request": "b"}, map[string]any{}),
				mustEvent(t, "B2", map[string]any{"request": "b"}, map[string]any{}),
				mustEvent(t, "B3", map[string]any{"request": "b"}, map[string]any{}),
			),
			result: make(chan sqliteSaveResult, 1),
		},
	}
	prepareGCWriteContexts(t, requests)
	saver.runFlush(gcBoundary, bp, requests)

	first, second := <-requests[0].result, <-requests[1].result
	if first.err != nil || second.err != nil {
		t.Fatalf("batch failed: first=%v second=%v", first.err, second.err)
	}
	if first.transactionID != "2" || first.globalID != 2 || second.transactionID != "5" || second.globalID != 5 {
		t.Fatalf("unexpected request positions: first=(%s,%d) second=(%s,%d)",
			first.transactionID, first.globalID, second.transactionID, second.globalID)
	}
	if next := readSeqNextID(t, bp); next != 6 {
		t.Fatalf("next_id = %d, want 6", next)
	}
}

func TestGroupCommitRejectsInvalidPreparedData(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	requests := []*sqliteSaveRequest{
		{
			ctx: context.Background(),
			inserts: eventstore.PreparedEventBatch{{
				EventId: "invalid", DataJSON: `{"broken":`, MetadataJSON: `{}`,
			}},
			result: make(chan sqliteSaveResult, 1),
		},
		{
			ctx: context.Background(),
			inserts: preparedGCEvents(t,
				mustEvent(t, "Valid", map[string]any{"valid": "yes"}, map[string]any{}),
			),
			result: make(chan sqliteSaveResult, 1),
		},
	}
	prepareGCWriteContexts(t, requests)
	saver.runFlush(gcBoundary, bp, requests)

	invalid, valid := <-requests[0].result, <-requests[1].result
	if statuscode.CodeOf(invalid.err) != statuscode.Internal {
		t.Fatalf("invalid request: expected Internal, got %v", invalid.err)
	}
	if valid.err != nil {
		t.Fatalf("valid request did not survive request validation: %v", valid.err)
	}
}

func TestGroupCommitIndependentCCC(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	_, seedGID, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
		mustEvent(t, "Seed", map[string]any{"stream_id": "current"}, map[string]any{}),
	}, gcBoundary, nil)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	request := func(value string, expected eventstore.Position, eventCount int) *sqliteSaveRequest {
		events := make([]eventstore.EventWithMapTags, eventCount)
		for i := range events {
			events[i] = mustEvent(t, "Independent", map[string]any{
				"stream_id": value,
				"sequence":  strconv.Itoa(i),
			}, map[string]any{})
		}
		return &sqliteSaveRequest{
			ctx:         context.Background(),
			inserts:     preparedGCEvents(t, events...),
			consistency: gcConsistency(expected, gcReadCriterion(eventstore.ReadTag{Key: "stream_id", Value: value})),
			result:      make(chan sqliteSaveResult, 1),
		}
	}
	requests := []*sqliteSaveRequest{
		request("new", eventstore.NotExistsPosition(), 2),
		request("current", eventstore.Position{CommitPosition: seedGID, PreparePosition: seedGID}, 1),
		request("stale", eventstore.NotExistsPosition(), 1),
	}
	// Make the last distinct context stale without causing overlap among the
	// other requests in the batch.
	if _, _, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
		mustEvent(t, "StaleSeed", map[string]any{"stream_id": "stale"}, map[string]any{}),
	}, gcBoundary, nil); err != nil {
		t.Fatalf("seed stale context: %v", err)
	}

	prepareGCWriteContexts(t, requests)
	saver.runFlush(gcBoundary, bp, requests)
	first, second, third := <-requests[0].result, <-requests[1].result, <-requests[2].result
	if first.err != nil || second.err != nil {
		t.Fatalf("independent accepted results: first=%v second=%v", first.err, second.err)
	}
	if statuscode.CodeOf(third.err) != statuscode.AlreadyExists {
		t.Fatalf("stale independent context: %v", third.err)
	}
	if count := countEventsMatching(t, bp, map[string]any{"stream_id": "stale"}); count != 1 {
		t.Fatalf("stale request wrote an event, count=%d", count)
	}
}

// Direct worker tests prepare the same durable representation as enqueue.
func prepareGCWriteContexts(t *testing.T, requests []*sqliteSaveRequest) {
	t.Helper()
	for _, req := range requests {
		data, err := eventstore.MarshalConsistency(req.consistency)
		if err != nil {
			t.Fatal(err)
		}
		req.consistencyJSON = string(data)
	}
}
