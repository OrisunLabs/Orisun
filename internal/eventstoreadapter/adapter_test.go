package eventstoreadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
)

func TestAdapterConvertsNeutralAppend(t *testing.T) {
	legacy := &captureLegacyStore{}
	adapter := New(legacy, legacy, nil)
	result, err := adapter.Append(t.Context(), coreeventstore.AppendRequest{
		Boundary: "admin",
		Events: []coreeventstore.EventToAppend{{
			EventID:   "event-1",
			EventType: "$BoundaryCreated",
			Data:      `{"boundary":"orders"}`,
			Metadata:  `{"source":"test"}`,
		}},
		ExpectedPosition: &coreeventstore.Position{CommitPosition: 3, PreparePosition: 4},
		Subset: coreeventstore.Query{Criteria: []coreeventstore.Criterion{{
			Tags: []coreeventstore.Tag{{Key: "boundary", Value: "orders"}},
		}}},
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if result.Position != (coreeventstore.Position{CommitPosition: 17, PreparePosition: 9}) {
		t.Fatalf("Append() position = %#v", result.Position)
	}
	if legacy.boundary != "admin" || legacy.expected.CommitPosition != 3 || legacy.query.Criteria[0].Tags[0].Value != "orders" {
		t.Fatalf("legacy append request = %#v %#v %#v", legacy.boundary, legacy.expected, legacy.query)
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(legacy.events[0].DataJSON), &data); err != nil {
		t.Fatal(err)
	}
	if data["eventType"] != "$BoundaryCreated" || data["boundary"] != "orders" {
		t.Fatalf("prepared data = %#v", data)
	}
}

func TestAdapterConvertsNeutralReads(t *testing.T) {
	created := time.Date(2026, time.July, 23, 10, 0, 0, 0, time.UTC)
	legacy := &captureLegacyStore{
		readBatch: orisun.ReadEventBatch{{
			EventId: "event-1", EventType: "$BoundaryCreated", Data: `{"eventType":"$BoundaryCreated","boundary":"orders"}`,
			CommitPosition: 5, PreparePosition: 6, DateCreated: created,
		}},
		latestBatch: orisun.LatestByCriteriaBatch{
			Matches: []orisun.LatestCriterionMatch{{
				Found: true,
				Event: orisun.ReadEvent{EventId: "event-2", EventType: "$BoundaryActivated", Data: `{"eventType":"$BoundaryActivated","boundary":"orders"}`, CommitPosition: 7, PreparePosition: 8},
			}},
			ContextCommitPosition:  7,
			ContextPreparePosition: 8,
		},
	}
	adapter := New(nil, legacy, nil)
	read, err := adapter.Read(t.Context(), coreeventstore.ReadRequest{
		Boundary:     "admin",
		FromPosition: &coreeventstore.Position{CommitPosition: 1, PreparePosition: 2},
		Count:        10,
		Direction:    coreeventstore.DirectionDescending,
		Query: coreeventstore.Query{Criteria: []coreeventstore.Criterion{{
			Tags: []coreeventstore.Tag{{Key: "eventType", Value: "$BoundaryCreated"}},
		}}},
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if len(read) != 1 || read[0].EventID != "event-1" || read[0].Position.CommitPosition != 5 || !read[0].DateCreated.Equal(created) {
		t.Fatalf("Read() = %#v", read)
	}
	if read[0].Data != `{"boundary":"orders"}` {
		t.Fatalf("Read() leaked storage eventType: %s", read[0].Data)
	}
	if legacy.readRequest.Direction != orisun.Direction_DESC || legacy.readRequest.FromPosition.PreparePosition != 2 {
		t.Fatalf("legacy read request = %#v", legacy.readRequest)
	}

	latest, err := adapter.LatestByCriteria(t.Context(), coreeventstore.LatestByCriteriaRequest{
		Boundary: "admin",
		Criteria: []coreeventstore.Criterion{{Tags: []coreeventstore.Tag{{Key: "boundary", Value: "orders"}}}},
	})
	if err != nil {
		t.Fatalf("LatestByCriteria() error = %v", err)
	}
	if !latest.Matches[0].Found || latest.Matches[0].Event.EventID != "event-2" ||
		latest.ContextPosition != (coreeventstore.Position{CommitPosition: 7, PreparePosition: 8}) {
		t.Fatalf("LatestByCriteria() = %#v", latest)
	}
	if latest.Matches[0].Event.Data != `{"boundary":"orders"}` {
		t.Fatalf("LatestByCriteria() leaked storage eventType: %s", latest.Matches[0].Event.Data)
	}
}

func TestAdapterForwardsNeutralSubscriptionEvents(t *testing.T) {
	wantErr := errors.New("disconnected")
	var received coreeventstore.ReadEvent
	adapter := New(nil, nil, func(
		ctx context.Context,
		request coreeventstore.SubscribeRequest,
		handle coreeventstore.EventHandler,
	) error {
		if request.Boundary != "admin" || request.SubscriberName != "boundary-provisioning" {
			t.Fatalf("Subscribe() request = %#v", request)
		}
		if err := handle(ctx, coreeventstore.ReadEvent{
			EventID: "event-1",
			Position: coreeventstore.Position{
				CommitPosition:  4,
				PreparePosition: 2,
			},
		}); err != nil {
			return err
		}
		return wantErr
	})
	err := adapter.Subscribe(t.Context(), coreeventstore.SubscribeRequest{
		Boundary:       "admin",
		SubscriberName: "boundary-provisioning",
	}, func(_ context.Context, event coreeventstore.ReadEvent) error {
		received = event
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Subscribe() error = %v, want %v", err, wantErr)
	}
	if received.EventID != "event-1" ||
		received.Position != (coreeventstore.Position{CommitPosition: 4, PreparePosition: 2}) {
		t.Fatalf("subscription event = %#v", received)
	}
}

type captureLegacyStore struct {
	events      orisun.PreparedEventBatch
	boundary    string
	expected    *orisun.Position
	query       *orisun.Query
	readRequest *orisun.GetEventsRequest
	readBatch   orisun.ReadEventBatch
	latestBatch orisun.LatestByCriteriaBatch
}

func (s *captureLegacyStore) SavePrepared(
	_ context.Context,
	events orisun.PreparedEventBatch,
	boundary string,
	expected *orisun.Position,
	query *orisun.Query,
) (string, int64, error) {
	s.events, s.boundary, s.expected, s.query = events, boundary, expected, query
	return "17", 9, nil
}

func (s *captureLegacyStore) GetBatch(_ context.Context, request *orisun.GetEventsRequest) (orisun.ReadEventBatch, error) {
	s.readRequest = request
	return s.readBatch, nil
}

func (s *captureLegacyStore) GetLatestByCriteria(
	_ context.Context,
	_ orisun.LatestByCriteriaQuery,
) (orisun.LatestByCriteriaBatch, error) {
	return s.latestBatch, nil
}
