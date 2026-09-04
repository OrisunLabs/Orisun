package orisun

import (
	"context"
	"fmt"

	"github.com/goccy/go-json"
)

// EventsSaver accepts canonical batches prepared at an API boundary.
type EventsSaver interface {
	SavePrepared(
		ctx context.Context,
		events PreparedEventBatch,
		boundary string,
		consistency []ConsistencyCheck,
	) (transactionID string, globalID int64, err error)
}

// EventsRetriever reads backend-neutral event batches and carried-state
// snapshots without depending on a network transport.
type EventsRetriever interface {
	GetBatch(ctx context.Context, req *GetEventsRequest) (ReadEventBatch, error)
	GetLatestByCriteria(ctx context.Context, query LatestByCriteriaQuery) (LatestByCriteriaBatch, error)
}

// LockProvider coordinates named work within a backend runtime.
type LockProvider interface {
	Lock(ctx context.Context, lockName string) error
}

// LockLease proves ongoing lock ownership until it is released or lost.
type LockLease interface {
	Context() context.Context
	Check(ctx context.Context) error
	Release()
}

// LockLeaseProvider exposes explicit lock lifecycles to embedded subscribers
// and server-side publishers.
type LockLeaseProvider interface {
	AcquireLock(ctx context.Context, lockName string) (LockLease, error)
}

// NotExistsPosition is the optimistic-concurrency position before any event.
func NotExistsPosition() Position {
	return Position{CommitPosition: -1, PreparePosition: -1}
}

// FirstPosition returns the first persisted event position.
func FirstPosition() Position {
	return Position{}
}

// EventWithMapTags is the flexible input representation used by embedding APIs.
type EventWithMapTags struct {
	EventId   string `json:"event_id"`
	EventType string `json:"event_type"`
	Data      any    `json:"data"`
	Metadata  any    `json:"metadata"`
}

// PreparedEvent is the canonical backend-facing event representation.
type PreparedEvent struct {
	EventId      string
	EventType    string
	DataJSON     string
	MetadataJSON string
}

// MarshalJSON preserves the canonical data and metadata as JSON values.
func (e PreparedEvent) MarshalJSON() ([]byte, error) {
	type wireEvent struct {
		EventId   string          `json:"event_id"`
		EventType string          `json:"event_type"`
		Data      json.RawMessage `json:"data"`
		Metadata  json.RawMessage `json:"metadata"`
	}
	return json.Marshal(wireEvent{
		EventId: e.EventId, EventType: e.EventType,
		Data: json.RawMessage(e.DataJSON), Metadata: json.RawMessage(e.MetadataJSON),
	})
}

// PreparedEventBatch keeps canonical event descriptors contiguous.
type PreparedEventBatch []PreparedEvent

// PrepareEventsForSave normalizes flexible embedding input exactly once.
func PrepareEventsForSave(events []EventWithMapTags) (PreparedEventBatch, error) {
	prepared := make(PreparedEventBatch, len(events))
	for i, event := range events {
		dataJSON, err := prepareEventDataJSON(event.Data, event.EventType)
		if err != nil {
			return nil, fmt.Errorf("event %d data: %w", i, err)
		}
		metadataJSON, err := prepareJSONValue(event.Metadata)
		if err != nil {
			return nil, fmt.Errorf("event %d metadata: %w", i, err)
		}
		prepared[i] = PreparedEvent{
			EventId:      event.EventId,
			EventType:    event.EventType,
			DataJSON:     dataJSON,
			MetadataJSON: metadataJSON,
		}
	}
	return prepared, nil
}

func prepareEventDataJSON(value any, eventType string) (string, error) {
	return prepareJSONObjectJSON(value, eventType, true)
}

// prepareJSONObjectJSON normalizes an object and optionally sets eventType to
// the supplied value. The generic object exists only at the API boundary and
// is discarded after producing the immutable backend representation.
func prepareJSONObjectJSON(value any, eventType string, setEventType bool) (string, error) {
	var object map[string]any
	switch value := value.(type) {
	case nil:
		object = make(map[string]any, 1)
	case string:
		if value == "" {
			object = make(map[string]any, 1)
		} else if err := json.Unmarshal([]byte(value), &object); err != nil {
			return "", err
		}
	case []byte:
		if len(value) == 0 {
			object = make(map[string]any, 1)
		} else if err := json.Unmarshal(value, &object); err != nil {
			return "", err
		}
	case map[string]any:
		object = make(map[string]any, len(value)+1)
		for key, item := range value {
			object[key] = item
		}
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		if err := json.Unmarshal(encoded, &object); err != nil {
			return "", err
		}
	}
	if object == nil {
		object = make(map[string]any, 1)
	}
	if setEventType {
		object["eventType"] = eventType
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func prepareJSONValue(value any) (string, error) {
	if value == nil {
		return "{}", nil
	}
	switch value := value.(type) {
	case string:
		if value == "" {
			return "{}", nil
		}
		if !json.Valid([]byte(value)) {
			return "", fmt.Errorf("invalid JSON")
		}
		return value, nil
	case []byte:
		if len(value) == 0 {
			return "{}", nil
		}
		if !json.Valid(value) {
			return "", fmt.Errorf("invalid JSON")
		}
		return string(value), nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(encoded), nil
	}
}

// EventPublishingTracker stores per-boundary durable publisher checkpoints.
type EventPublishingTracker interface {
	GetLastPublishedEventPosition(ctx context.Context, boundary string) (Position, error)
	InsertLastPublishedEvent(ctx context.Context, boundary string, transactionID, globalID int64) error
}

// EventSignal reports that new events may be available.
type EventSignal interface {
	Wait(ctx context.Context) error
	Stop()
}
