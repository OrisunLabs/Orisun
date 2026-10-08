package orisun

import (
	"context"
	"errors"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type subscriptionReadFunc func(context.Context, *GetEventsRequest) (ReadEventBatch, error)

func (f subscriptionReadFunc) GetBatch(ctx context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
	return f(ctx, req)
}

func (subscriptionReadFunc) GetLatestByCriteria(context.Context, LatestByCriteriaQuery) (LatestByCriteriaBatch, error) {
	return LatestByCriteriaBatch{}, errors.New("unexpected latest-by-criteria read")
}

type subscriptionStreamError struct {
	jetstream.JetStream
	err error
}

func (s subscriptionStreamError) Stream(context.Context, string) (jetstream.Stream, error) {
	return nil, s.err
}

func TestSubscriptionOmittedPositionWithNoMatchEntersLiveDelivery(t *testing.T) {
	stop := errors.New("live setup reached")
	calls := 0
	retriever := subscriptionReadFunc(func(_ context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
		calls++
		if calls != 1 || req.Direction != Direction_DESC || req.Count != 1 || req.FromPosition != nil {
			t.Fatalf("unexpected empty-boundary read: %#v", req)
		}
		return nil, nil
	})
	store := &EventStore{getEventsFn: retriever, lockProvider: newContextOnlyLockProvider(), logger: noopLogger{}, js: subscriptionStreamError{err: stop}}
	err := store.SubscribeToAllEvents(t.Context(), coreeventstore.SubscribeRequest{Boundary: "orders", SubscriberName: "empty"},
		func(context.Context, coreeventstore.ReadEvent) error {
			t.Fatal("empty result must not deliver an event")
			return nil
		})
	if err == nil || !strings.Contains(err.Error(), stop.Error()) {
		t.Fatalf("subscription error = %v, want live setup", err)
	}
}

func TestSubscriptionOmittedPositionStartsAtLatestMatchAndContinuesForward(t *testing.T) {
	stop := errors.New("stop after verifying cursor")
	calls := 0
	var delivered []int64
	retriever := subscriptionReadFunc(func(_ context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
		calls++
		switch calls {
		case 1:
			if req.Direction != Direction_DESC || req.Count != 1 || req.FromPosition != nil {
				t.Fatalf("initial read = %#v, want latest matching event only", req)
			}
			return ReadEventBatch{{CommitPosition: 7, PreparePosition: 200}}, nil
		case 2:
			if req.Direction != Direction_ASC || req.Count < 2 || req.FromPosition.PreparePosition != 200 {
				t.Fatalf("forward read = %#v", req)
			}
			// The inclusive cursor plus newly committed matches fill a batch.
			batch := make(ReadEventBatch, req.Count)
			for i := range batch {
				batch[i] = ReadEvent{CommitPosition: 7, PreparePosition: 200 + int64(i)}
			}
			return batch, nil
		default:
			if req.Direction != Direction_ASC || req.FromPosition.PreparePosition != 299 {
				t.Fatalf("cursor regressed: %#v", req)
			}
			return nil, stop
		}
	})
	store := &EventStore{getEventsFn: retriever, lockProvider: newContextOnlyLockProvider(), logger: noopLogger{}}
	err := store.SubscribeToAllEvents(t.Context(), coreeventstore.SubscribeRequest{Boundary: "orders", SubscriberName: "latest"},
		func(_ context.Context, event coreeventstore.ReadEvent) error {
			delivered = append(delivered, event.Position.PreparePosition)
			return nil
		})
	if err == nil || !strings.Contains(err.Error(), stop.Error()) {
		t.Fatalf("subscription error = %v", err)
	}
	if len(delivered) != 100 {
		t.Fatalf("delivered %d events, want 100", len(delivered))
	}
	for i, position := range delivered {
		if position != 200+int64(i) {
			t.Fatalf("delivery %d = %d", i, position)
		}
	}
}

type contextOnlyLockProvider struct {
	acquired chan struct{}
	released chan struct{}
	once     sync.Once
}

func newContextOnlyLockProvider() *contextOnlyLockProvider {
	return &contextOnlyLockProvider{
		acquired: make(chan struct{}),
		released: make(chan struct{}),
	}
}

func (p *contextOnlyLockProvider) Lock(ctx context.Context, _ string) error {
	p.once.Do(func() {
		close(p.acquired)
		go func() {
			<-ctx.Done()
			close(p.released)
		}()
	})
	return nil
}

func TestSubscribeToAllEventsCancelsLockContextOnEarlyError(t *testing.T) {
	lockProvider := newContextOnlyLockProvider()
	retriever := &fakeRetriever{errOnce: true}
	store := &EventStore{
		getEventsFn:  retriever,
		lockProvider: lockProvider,
		logger:       noopLogger{},
	}
	err := store.SubscribeToAllEvents(
		context.Background(),
		coreeventstore.SubscribeRequest{
			Boundary:       "orders",
			SubscriberName: "subscriber",
			AfterPosition:  &coreeventstore.Position{},
		},
		func(context.Context, coreeventstore.ReadEvent) error {
			return nil
		},
	)
	if err == nil {
		t.Fatal("expected catch-up read error")
	}

	select {
	case <-lockProvider.released:
	case <-time.After(time.Second):
		t.Fatal("lock context was not cancelled after an early subscription error")
	}
}

func TestSubscribeToAllEventsDeliversNeutralEventAndPropagatesHandlerError(t *testing.T) {
	retriever := &fakeRetriever{}
	retriever.add(ReadEvent{
		EventId:         "event-1",
		EventType:       "OrderPlaced",
		Data:            `{"orderId":"o-1"}`,
		Metadata:        `{}`,
		CommitPosition:  1,
		PreparePosition: 1,
		DateCreated:     time.Date(2026, time.July, 23, 10, 0, 0, 0, time.UTC),
	})
	store := &EventStore{
		getEventsFn:  retriever,
		lockProvider: newContextOnlyLockProvider(),
		logger:       noopLogger{},
	}
	wantErr := errors.New("projection failed")
	var received coreeventstore.ReadEvent

	err := store.SubscribeToAllEvents(
		t.Context(),
		coreeventstore.SubscribeRequest{
			Boundary:       "orders",
			SubscriberName: "orders-projection",
			AfterPosition:  &coreeventstore.Position{},
		},
		func(_ context.Context, event coreeventstore.ReadEvent) error {
			received = event
			return wantErr
		},
	)
	if err == nil || !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("SubscribeToAllEvents() error = %v, want handler error", err)
	}
	if received.EventID != "event-1" ||
		received.Position != (coreeventstore.Position{CommitPosition: 1, PreparePosition: 1}) {
		t.Fatalf("received event = %#v", received)
	}
	if received.EventType != "OrderPlaced" || received.Data != `{"orderId":"o-1"}` {
		t.Fatalf("received event leaked storage data = %#v", received)
	}
}

func TestNeutralPublishedEventExcludesStorageEventTypeFromData(t *testing.T) {
	event := Event{
		EventId:   "event-1",
		EventType: "OrderPlaced",
		Data:      `{"orderId":"o-1"}`,
	}

	got := neutralPublishedEvent(event)

	if got.EventType != "OrderPlaced" || got.Data != `{"orderId":"o-1"}` {
		t.Fatalf("neutralPublishedEvent() = %#v", got)
	}
}
