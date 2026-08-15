package orisun

import (
	"context"
	"errors"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"strings"
	"sync"
	"testing"
	"time"
)

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
		Data:            `{"eventType":"OrderPlaced","orderId":"o-1"}`,
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
		Data:      `{"eventType":"OrderPlaced","orderId":"o-1"}`,
	}

	got := neutralPublishedEvent(event)

	if got.EventType != "OrderPlaced" || got.Data != `{"orderId":"o-1"}` {
		t.Fatalf("neutralPublishedEvent() = %#v", got)
	}
}
