//go:build !orisun_embedded

package orisun

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestEventStoreEnsureBoundaryIsIdempotent(t *testing.T) {
	js := dynamicBoundaryTestJetStream(t)
	store := &EventStore{
		js:     js,
		logger: noopLogger{},
	}

	if err := store.EnsureBoundary(t.Context(), "sales"); err != nil {
		t.Fatalf("first EnsureBoundary() error = %v", err)
	}
	if err := store.EnsureBoundary(t.Context(), "sales"); err != nil {
		t.Fatalf("idempotent EnsureBoundary() error = %v", err)
	}
	if _, err := js.Stream(t.Context(), "ORISUN_NOTIFICATIONS___sales"); err != jetstream.ErrStreamNotFound {
		t.Fatalf("Core NATS boundary unexpectedly provisioned a stream: %v", err)
	}

}

func TestEventStoreRejectsRequestsUntilBoundaryIsActive(t *testing.T) {
	saver := &countingBoundarySaver{}
	store := &EventStore{
		saveEventsFn:     saver,
		logger:           noopLogger{},
		activeBoundaries: make(map[string]struct{}),
	}
	if err := store.EnableBoundaryActivationGate("orisun_admin"); err != nil {
		t.Fatalf("EnableBoundaryActivationGate() error = %v", err)
	}

	request := &SaveEventsV2Request{
		Boundary: "sales",
		Events: []*EventToSave{{
			EventId:   "event-1",
			EventType: "SaleOpened",
			Data:      `{}`,
			Metadata:  `{}`,
		}},
	}
	if _, err := store.SaveEventsV2(t.Context(), request); statuscode.CodeOf(err) != statuscode.FailedPrecondition {
		t.Fatalf("SaveEvents() before activation error = %v, want FailedPrecondition", err)
	}
	if got := saver.calls.Load(); got != 0 {
		t.Fatalf("backend save calls before activation = %d, want 0", got)
	}

	if err := store.ActivateBoundary("sales"); err != nil {
		t.Fatalf("ActivateBoundary() error = %v", err)
	}
	if _, err := store.SaveEventsV2(t.Context(), request); err != nil {
		t.Fatalf("SaveEvents() after activation error = %v", err)
	}
	if got := saver.calls.Load(); got != 1 {
		t.Fatalf("backend save calls after activation = %d, want 1", got)
	}
}

func TestNotificationManagerStartsBoundaryOnce(t *testing.T) {
	js := dynamicBoundaryTestJetStream(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	lockProvider := &blockingBoundaryLockProvider{entered: make(chan struct{})}
	manager := &BoundaryNotificationManager{
		ctx:          ctx,
		lockProvider: lockProvider,
		conn:         js.Conn(),
		signalProvider: func(string) EventSignal {
			return &controlledNotificationSignal{pending: make(chan struct{}, 1)}
		},
		logger:  noopLogger{},
		running: make(map[string]struct{}),
	}

	if err := manager.StartBoundary("sales"); err != nil {
		t.Fatalf("first StartBoundary() error = %v", err)
	}
	if err := manager.StartBoundary("sales"); err != nil {
		t.Fatalf("idempotent StartBoundary() error = %v", err)
	}
	select {
	case <-lockProvider.entered:
	case <-time.After(time.Second):
		t.Fatal("publisher did not attempt to acquire its lock")
	}
	time.Sleep(20 * time.Millisecond)
	if got := lockProvider.calls.Load(); got != 1 {
		t.Fatalf("lock acquisition calls = %d, want 1", got)
	}
}

type countingBoundarySaver struct {
	calls atomic.Int32
}

func (s *countingBoundarySaver) SavePrepared(
	context.Context,
	PreparedEventBatch,
	string,
	[]ConsistencyCheck,
) (string, int64, error) {
	s.calls.Add(1)
	return "1", 1, nil
}

type blockingBoundaryLockProvider struct {
	calls   atomic.Int32
	entered chan struct{}
}

func (p *blockingBoundaryLockProvider) Lock(ctx context.Context, name string) error {
	_, err := p.AcquireLock(ctx, name)
	return err
}

func (p *blockingBoundaryLockProvider) AcquireLock(ctx context.Context, _ string) (LockLease, error) {
	if p.calls.Add(1) == 1 {
		close(p.entered)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type unusedBoundaryRetriever struct{}

func (unusedBoundaryRetriever) GetBatch(context.Context, *GetEventsRequest) (ReadEventBatch, error) {
	return nil, nil
}

func (unusedBoundaryRetriever) GetLatestByCriteria(context.Context, LatestByCriteriaQuery) (LatestByCriteriaBatch, error) {
	return LatestByCriteriaBatch{}, nil
}

func dynamicBoundaryTestJetStream(t *testing.T) jetstream.JetStream {
	return dynamicBoundaryTestNATS(t, true)
}

func dynamicBoundaryTestNATS(t *testing.T, enableJetStream bool) jetstream.JetStream {
	t.Helper()
	server, err := natsserver.NewServer(&natsserver.Options{
		ServerName: "orisun-dynamic-boundary-test",
		Port:       -1,
		JetStream:  enableJetStream,
		StoreDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	go server.Start()
	if !server.ReadyForConnections(5 * time.Second) {
		server.Shutdown()
		t.Fatal("NATS server did not become ready")
	}
	connection, err := natsgo.Connect("", natsgo.InProcessServer(server))
	if err != nil {
		server.Shutdown()
		t.Fatalf("connect to NATS: %v", err)
	}
	t.Cleanup(func() {
		connection.Close()
		server.Shutdown()
	})
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}
	return js
}
