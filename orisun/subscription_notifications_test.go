//go:build !orisun_embedded

package orisun

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	natsgo "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionHintsReadBackendPayloadAndCleanUpListener(t *testing.T) {
	js := dynamicBoundaryTestNATS(t, false)
	store := NewEventStoreServer(js, nil, nil, newContextOnlyLockProvider(), nil, noopLogger{})
	store.subscriptionIdleThreshold = time.Hour // This test must use a hint.
	require.NoError(t, store.EnsureBoundary(t.Context(), "orders"))
	backend := &fakeRetriever{}
	firstRead := make(chan struct{})
	var once sync.Once
	store.getEventsFn = subscriptionReadFunc(func(ctx context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
		// Listener must already exist before the initial storage snapshot.
		require.Equal(t, 1, js.Conn().NumSubscriptions())
		batch, err := backend.GetBatch(ctx, req)
		once.Do(func() { close(firstRead) })
		return batch, err
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	var events []coreeventstore.ReadEvent
	go func() {
		result <- store.SubscribeToAllEvents(ctx, coreeventstore.SubscribeRequest{Boundary: "orders", SubscriberName: "hints", AfterPosition: &coreeventstore.Position{}}, func(_ context.Context, event coreeventstore.ReadEvent) error {
			events = append(events, event)
			if len(events) == 2 {
				cancel()
			}
			return nil
		})
	}()
	select {
	case <-firstRead:
	case <-time.After(time.Second):
		t.Fatal("initial read did not start")
	}
	large := strings.Repeat("x", 2*1024*1024)
	backend.add(ReadEvent{EventId: "one", CommitPosition: 1, PreparePosition: 1, Data: large}, ReadEvent{EventId: "two", CommitPosition: 1, PreparePosition: 2, Data: "storage"})
	// Neither this payload nor a NATS sequence becomes an event or a cursor.
	err := js.Conn().Publish(GetNotificationSubjectName("orders"), []byte("not an event"))
	require.NoError(t, err)
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("hint did not trigger backend delivery")
	}
	require.Len(t, events, 2)
	require.Equal(t, large, events[0].Data)
	require.Equal(t, "two", events[1].EventID)
	require.Zero(t, js.Conn().NumSubscriptions())
	_, err = js.Stream(t.Context(), "ORISUN_NOTIFICATIONS___orders")
	require.Error(t, err)
	_, err = js.Stream(t.Context(), "ORISUN_EVENTS___orders")
	require.Error(t, err)

}

func TestSubscriptionDoesNotPollBackendWhenNATSIsUnavailable(t *testing.T) {
	backend := &fakeRetriever{}
	var reads atomic.Int32
	js := dynamicBoundaryTestNATS(t, false)
	js.Conn().Close()
	store := &EventStore{js: js, lockProvider: newContextOnlyLockProvider(), logger: noopLogger{}, subscriptionIdleThreshold: 10 * time.Millisecond}
	store.getEventsFn = subscriptionReadFunc(func(ctx context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
		batch, err := backend.GetBatch(ctx, req)
		reads.Add(1)
		return batch, err
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	delivered := make(chan coreeventstore.ReadEvent, 1)
	go func() {
		result <- store.SubscribeToAllEvents(ctx, coreeventstore.SubscribeRequest{Boundary: "orders", SubscriberName: "unavailable", AfterPosition: &coreeventstore.Position{}}, func(_ context.Context, event coreeventstore.ReadEvent) error { delivered <- event; return nil })
	}()
	backend.add(ReadEvent{EventId: "pending", CommitPosition: 1, PreparePosition: 1})
	select {
	case <-delivered:
		t.Fatal("backend delivery bypassed NATS")
	case <-time.After(100 * time.Millisecond):
	}
	require.Zero(t, reads.Load(), "even initial reads require receipt of a NATS ping")
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestSubscriptionIdleWatchdogPublishesHintAndDeliversFromBackend(t *testing.T) {
	js := dynamicBoundaryTestNATS(t, false)
	backend := &fakeRetriever{}
	store := NewEventStoreServer(js, nil, backend, newContextOnlyLockProvider(), nil, noopLogger{})
	store.subscriptionIdleThreshold = 50 * time.Millisecond
	hints := make(chan *natsgo.Msg, 1)
	observer, err := js.Conn().Subscribe(GetNotificationSubjectName("orders"), func(msg *natsgo.Msg) {
		select {
		case hints <- msg:
		default:
		}
	})
	require.NoError(t, err)
	defer observer.Unsubscribe()
	require.NoError(t, js.Conn().FlushTimeout(time.Second))
	firstRead := make(chan struct{})
	var once sync.Once
	store.getEventsFn = subscriptionReadFunc(func(ctx context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
		batch, err := backend.GetBatch(ctx, req)
		once.Do(func() { close(firstRead) })
		return batch, err
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- store.SubscribeToAllEvents(ctx, coreeventstore.SubscribeRequest{Boundary: "orders", SubscriberName: "idle", AfterPosition: &coreeventstore.Position{}}, func(_ context.Context, event coreeventstore.ReadEvent) error {
			if event.EventID != "pending" {
				return errors.New("unexpected event")
			}
			cancel()
			return nil
		})
	}()
	select {
	case <-firstRead:
	case <-time.After(time.Second):
		t.Fatal("initial read did not start")
	}
	// Drain the startup ping observed on the same subject.
	select {
	case <-hints:
	case <-time.After(time.Second):
		t.Fatal("startup ping missing")
	}
	backend.add(ReadEvent{EventId: "pending", CommitPosition: 1, PreparePosition: 1})
	select {
	case hint := <-hints:
		require.Empty(t, hint.Data)
	case <-time.After(time.Second):
		t.Fatal("idle watchdog did not publish a NATS hint")
	}
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("watchdog hint did not trigger backend delivery")
	}
}

func TestSubscriptionHintActivityUsesReceiptAndIsIndependentPerSubscriber(t *testing.T) {
	start := time.Now()
	active := newSubscriptionHintActivity()
	idle := newSubscriptionHintActivity()
	active.received(start)
	idle.received(start)
	active.received(start.Add(900 * time.Millisecond))
	require.Equal(t, 900*time.Millisecond, active.remaining(start.Add(time.Second), time.Second))
	require.Zero(t, idle.remaining(start.Add(time.Second), time.Second))
	// Handler progress and published hints do not update receipt timestamps.
	require.Zero(t, idle.remaining(start.Add(2*time.Second), time.Second))
	require.Len(t, active.changed, 1, "activity signals must coalesce")
}

func TestSubscriptionNameLeaseExclusivitySurvivesNotificationOutage(t *testing.T) {
	js := dynamicBoundaryTestJetStream(t)
	locks, err := NewJetStreamLockProvider(t.Context(), js, noopLogger{})
	require.NoError(t, err)
	read := make(chan struct{})
	var once sync.Once
	store := &EventStore{js: js, lockProvider: locks, logger: noopLogger{}}
	store.getEventsFn = subscriptionReadFunc(func(context.Context, *GetEventsRequest) (ReadEventBatch, error) {
		once.Do(func() { close(read) })
		return nil, nil
	})
	request := coreeventstore.SubscribeRequest{Boundary: "orders", SubscriberName: "exclusive"}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- store.SubscribeToAllEvents(ctx, request, func(context.Context, coreeventstore.ReadEvent) error { return nil })
	}()
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Fatal("initial subscription did not start")
	}
	err = store.SubscribeToAllEvents(t.Context(), request, func(context.Context, coreeventstore.ReadEvent) error { return nil })
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	lease, err := locks.AcquireLock(t.Context(), "orders__exclusive")
	require.NoError(t, err)
	lease.Release()
}

func TestSubscriptionReconnectResumesFromApplicationCheckpoint(t *testing.T) {
	js := dynamicBoundaryTestJetStream(t)
	locks, err := NewJetStreamLockProvider(t.Context(), js, noopLogger{})
	require.NoError(t, err)
	backend := &fakeRetriever{}
	backend.add(ReadEvent{EventId: "one", CommitPosition: 1, PreparePosition: 1}, ReadEvent{EventId: "two", CommitPosition: 1, PreparePosition: 2})
	store := NewEventStoreServer(js, nil, backend, locks, nil, noopLogger{})
	require.NoError(t, store.EnsureBoundary(t.Context(), "orders"))
	stop := errors.New("disconnect")
	request := coreeventstore.SubscribeRequest{Boundary: "orders", SubscriberName: "resume", AfterPosition: &coreeventstore.Position{}}
	var checkpoint coreeventstore.Position
	err = store.SubscribeToAllEvents(t.Context(), request, func(_ context.Context, event coreeventstore.ReadEvent) error {
		checkpoint = event.Position
		return stop
	})
	require.ErrorContains(t, err, stop.Error())
	require.Equal(t, coreeventstore.Position{CommitPosition: 1, PreparePosition: 1}, checkpoint)
	request.AfterPosition = &checkpoint
	var resumed []string
	err = store.SubscribeToAllEvents(t.Context(), request, func(_ context.Context, event coreeventstore.ReadEvent) error {
		resumed = append(resumed, event.EventID)
		return stop
	})
	require.ErrorContains(t, err, stop.Error())
	require.Equal(t, []string{"two"}, resumed)
}

func TestCoreNotificationListenerRecoversAfterConnectionReconnect(t *testing.T) {
	js := dynamicBoundaryTestNATS(t, false)
	backend := &fakeRetriever{}
	store := NewEventStoreServer(js, nil, backend, newContextOnlyLockProvider(), nil, noopLogger{})
	store.subscriptionIdleThreshold = time.Hour
	firstRead := make(chan struct{})
	var once sync.Once
	store.getEventsFn = subscriptionReadFunc(func(ctx context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
		batch, err := backend.GetBatch(ctx, req)
		once.Do(func() { close(firstRead) })
		return batch, err
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- store.SubscribeToAllEvents(ctx, coreeventstore.SubscribeRequest{Boundary: "orders", SubscriberName: "reconnect", AfterPosition: &coreeventstore.Position{}}, func(context.Context, coreeventstore.ReadEvent) error { cancel(); return nil })
	}()
	select {
	case <-firstRead:
	case <-time.After(time.Second):
		t.Fatal("initial read did not start")
	}
	reconnected := js.Conn().StatusChanged(natsgo.CONNECTED)
	defer js.Conn().RemoveStatusListener(reconnected)
	backend.add(ReadEvent{EventId: "after reconnect", CommitPosition: 1, PreparePosition: 1})
	require.NoError(t, js.Conn().ForceReconnect())
	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("connection did not recover")
	}
	require.NoError(t, js.Conn().FlushTimeout(time.Second))

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("restored Core listener did not wake drain")
	}
	require.Zero(t, js.Conn().NumSubscriptions())
}

func TestSubscriptionNotificationBurstCoalescesWithoutSlowConsumerDrops(t *testing.T) {
	js := dynamicBoundaryTestNATS(t, false)
	store := &EventStore{js: js}
	errorsReceived := make(chan error, 1)
	js.Conn().SetErrorHandler(func(_ *natsgo.Conn, _ *natsgo.Subscription, err error) {
		select {
		case errorsReceived <- err:
		default:
		}
	})
	drain := newSubscriptionDrain(nil, "orders", nil, nil, nil, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	var first, cleanup sync.Once
	defer cleanup.Do(func() { close(release) })
	var callbacks atomic.Int32
	listener, err := store.openSubscriptionNotifications(t.Context(), "orders", func() {
		first.Do(func() { close(entered); <-release })
		drain.wake()
		callbacks.Add(1)
	})
	require.NoError(t, err)
	defer listener.Unsubscribe()
	require.NoError(t, js.Conn().Publish(GetNotificationSubjectName("orders"), nil))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("notification callback did not start")
	}
	// Delay the callback while a burst arrives, as scheduling pressure can do.
	const burst = 1000
	for range burst {
		require.NoError(t, js.Conn().Publish(GetNotificationSubjectName("orders"), nil))
	}
	require.NoError(t, js.Conn().FlushTimeout(time.Second))
	pending, _, err := listener.Pending()
	require.NoError(t, err)
	require.Equal(t, burst+1, pending, "pending includes the callback currently executing")
	cleanup.Do(func() { close(release) })
	require.Eventually(t, func() bool { return callbacks.Load() == burst+1 }, time.Second, time.Millisecond)
	dropped, err := listener.Dropped()
	require.NoError(t, err)
	require.Zero(t, dropped)
	require.Empty(t, errorsReceived)
	require.Len(t, drain.pending, 1, "coalescing belongs to the drain wake-up channel")
}
