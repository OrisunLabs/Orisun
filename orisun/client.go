//go:build !orisun_embedded

package orisun

import (
	"context"
	"fmt"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/OrisunLabs/Orisun/logging"
)

// OrisunServer provides a high-level interface to interact with the Orisun event store
type OrisunServer struct {
	eventStore   *EventStore
	saveEvents   EventsSaver
	getEvents    EventsRetriever
	lockProvider LockProvider
	logger       logging.Logger
	js           jetstream.JetStream
}

// NewOrisunServer creates a new Orisun client with the provided configuration
func NewOrisunServer(
	ctx context.Context,
	saveEvents EventsSaver,
	getEvents EventsRetriever,
	lockProvider LockProvider,
	js jetstream.JetStream,
	logger logging.Logger,
) (*OrisunServer, error) {

	if lockProvider == nil {
		var err error = nil
		lockProvider, err = NewJetStreamLockProvider(ctx, js, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create lock provider: %w", err)
		}
	}

	// Initialize event store
	eventStore := NewEventStoreServer(
		js,
		saveEvents,
		getEvents,
		lockProvider,
		nil,
		EventStreamConfig{},
		logger,
	)

	return &OrisunServer{
		eventStore:   eventStore,
		saveEvents:   saveEvents,
		getEvents:    getEvents,
		lockProvider: lockProvider,
		logger:       logger,
		js:           js,
	}, nil
}

// SaveEvents saves a batch of events to the event store
func (c *OrisunServer) SaveEvents(ctx context.Context, events []EventWithMapTags, boundary string,
	expectedPosition *Position, streamSubSet *Query) (*Position, error) {
	if err := c.RequireBoundaryActive(boundary); err != nil {
		return nil, err
	}
	prepared, prepareErr := PrepareEventsForSave(events)
	if prepareErr != nil {
		return nil, fmt.Errorf("failed to prepare events: %w", prepareErr)
	}
	transactionID, globalID, err := c.eventStore.savePreparedWithMetrics(
		ctx,
		c.saveEvents,
		prepared,
		boundary,
		expectedPosition,
		streamSubSet,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to save events: %w", err)
	}

	transID, err := parseInt64(transactionID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction id: %w", err)
	}

	return &Position{
		CommitPosition:  transID,
		PreparePosition: globalID,
	}, nil
}

// GetEvents retrieves events from the event store based on the request.
func (c *OrisunServer) GetEvents(ctx context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
	if req == nil {
		return nil, fmt.Errorf("get events request is required")
	}
	if err := c.RequireBoundaryActive(req.Boundary); err != nil {
		return nil, err
	}
	batch, err := c.getEvents.GetBatch(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get events: %w", err)
	}

	return batch, nil
}

// GetLatestByCriteria returns the latest event per criterion from one backend
// read snapshot, plus the max observed position as the optimistic-lock token
// for the combined context.
func (c *OrisunServer) GetLatestByCriteria(ctx context.Context, query LatestByCriteriaQuery) (LatestByCriteriaBatch, error) {
	if err := c.RequireBoundaryActive(query.Boundary); err != nil {
		return LatestByCriteriaBatch{}, err
	}
	batch, err := c.getEvents.GetLatestByCriteria(ctx, query)
	if err != nil {
		return LatestByCriteriaBatch{}, fmt.Errorf("failed to get latest by criteria: %w", err)
	}

	return batch, nil
}

// SubscribeToEvents subscribes to events from a boundary with the given handler
func (c *OrisunServer) SubscribeToEvents(
	ctx context.Context,
	request coreeventstore.SubscribeRequest,
	handler coreeventstore.EventHandler,
) error {

	// Subscribe to events
	return c.eventStore.SubscribeToAllEvents(
		ctx,
		request,
		handler,
	)
}

// EnsureBoundary prepares the real-time stream for a newly provisioned
// boundary. Durable backend creation remains the backend adapter's concern.
func (c *OrisunServer) EnsureBoundary(ctx context.Context, boundary string) error {
	if c == nil || c.eventStore == nil {
		return fmt.Errorf("event store is not configured")
	}
	return c.eventStore.EnsureBoundary(ctx, boundary)
}

// EnableBoundaryActivationGate makes public operations require an ACTIVE
// boundary catalog state. The admin boundary is supplied during bootstrap.
func (c *OrisunServer) EnableBoundaryActivationGate(initiallyActive ...string) error {
	if c == nil || c.eventStore == nil {
		return fmt.Errorf("event store is not configured")
	}
	return c.eventStore.EnableBoundaryActivationGate(initiallyActive...)
}

// ActivateBoundary exposes a durably activated boundary to public operations.
func (c *OrisunServer) ActivateBoundary(_ context.Context, boundary string) error {
	if c == nil || c.eventStore == nil {
		return fmt.Errorf("event store is not configured")
	}
	return c.eventStore.ActivateBoundary(boundary)
}

// RequireBoundaryActive validates a boundary against the local catalog gate.
func (c *OrisunServer) RequireBoundaryActive(boundary string) error {
	if c == nil || c.eventStore == nil {
		return fmt.Errorf("event store is not configured")
	}
	return c.eventStore.RequireBoundaryActive(boundary)
}

// Helper function to parse int64 from string
func parseInt64(s string) (int64, error) {
	var i int64
	_, err := fmt.Sscanf(s, "%d", &i)
	if err != nil {
		return 0, fmt.Errorf("failed to parse int64: %w", err)
	}
	return i, nil
}
