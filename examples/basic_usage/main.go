package main

import (
	"context"
	"time"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	c "github.com/OrisunLabs/Orisun/config"
	embeddedpg "github.com/OrisunLabs/Orisun/embedded/postgres"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// Create a context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Load default configuration
	config := c.InitializeConfig()

	logger := logging.InitializeDefaultLogger(config.Logging)

	// Bring your own JetStream-enabled NATS server.
	conn, err := natsgo.Connect("nats://localhost:4222")
	if err != nil {
		logger.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer conn.Close()
	jetStream, err := jetstream.New(conn)
	if err != nil {
		logger.Fatalf("Failed to create JetStream context: %v", err)
	}

	// Initialize database connection
	config.Postgres.User = "postgres"
	config.Postgres.Password = "password@1"
	config.Postgres.ListenEnabled = false
	store, err := embeddedpg.Start(ctx, config, logger, embeddedpg.WithJetStream(jetStream))
	if err != nil {
		logger.Fatalf("Failed to start embedded Orisun: %v", err)
	}
	defer store.Close(context.Background())

	// Example 1: Save events to the event store
	logger.Info("=== Saving Events Example ===")
	boundary := "example"
	if _, err := store.CreateBoundary(ctx, boundarymodel.Definition{
		Name:        boundary,
		Description: "Example events",
		Placement: boundarymodel.Placement{
			Backend:   "postgres",
			Namespace: "public",
		},
	}); err != nil {
		logger.Fatalf("Failed to create boundary: %v", err)
	}
	for {
		created, err := store.GetBoundary(ctx, boundary)
		if err != nil {
			logger.Fatalf("Failed to read boundary: %v", err)
		}
		if created.Status == boundarymodel.StatusActive {
			break
		}
		if created.Status == boundarymodel.StatusFailed {
			logger.Fatalf("Failed to provision boundary: %s", created.LastError)
		}
		select {
		case <-ctx.Done():
			logger.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}

	// Create sample events
	events := []orisun.EventWithMapTags{
		{
			EventId:   generateUUID(),
			EventType: "UserCreated",
			Data: map[string]any{
				"userId":   "user-123",
				"username": "john_doe",
				"email":    "john@example.com",
			},
			Metadata: map[string]any{
				"source":  "user-service",
				"version": "1.0",
			},
		},
		{
			EventId:   generateUUID(),
			EventType: "UserUpdated",
			Data: map[string]interface{}{
				"userId":   "user-123",
				"username": "john_doe_updated",
				"email":    "john_updated@example.com",
			},
			Metadata: map[string]interface{}{
				"source":  "user-service",
				"version": "1.0",
			},
		},
	}

	// Save events to the event store
	position := orisun.NotExistsPosition()
	// Save events
	newPosition, err := store.SaveEvents(
		ctx,
		events,
		boundary,
		&position, // No expected position for new stream
		nil,       // No subset query
	)
	if err != nil {
		logger.Fatalf("Failed to save events: %v", err)
	}

	logger.Infof("Successfully saved events at position: Commit=%d, Prepare=%d",
		newPosition.CommitPosition, newPosition.PreparePosition)

	// Example 2: Get events from the event store
	logger.Info("=== Getting Events Example ===")

	// Get events from the stream
	getEventsReq := &orisun.GetEventsRequest{
		Count:     10,
		Direction: orisun.Direction_ASC,
		Boundary:  boundary,
	}

	readEvents, err := store.GetEvents(ctx, getEventsReq)
	if err != nil {
		logger.Fatalf("Failed to get events: %v", err)
	}

	logger.Infof("Retrieved %d events:", len(readEvents))
	for i, event := range readEvents {
		logger.Infof("Event %d: ID=%s, Type=%s, Position=Commit=%d,Prepare=%d",
			i+1, event.EventId, event.EventType,
			event.CommitPosition, event.PreparePosition)
	}

	// Example 3: Subscribe to events
	logger.Info("=== Subscribing to Events Example ===")

	// Subscribe to all events in the boundary
	subscriberName := "example-subscriber"

	// Start subscription in a separate goroutine to not block the main function
	go func() {
		logger.Info("Starting subscription to all events...")
		err := store.SubscribeToEvents(
			ctx,
			coreeventstore.SubscribeRequest{
				Boundary:       boundary,
				SubscriberName: subscriberName,
			},
			func(_ context.Context, event coreeventstore.ReadEvent) error {
				logger.Infof("Received event: ID=%s, Type=%s, Data=%s",
					event.EventID, event.EventType, event.Data)
				return nil
			},
		)
		if err != nil {
			logger.Errorf("Subscription failed: %v", err)
		}
	}()

	// Wait a bit to receive some events
	logger.Info("Waiting to receive events...")
	select {
	case <-time.After(5 * time.Second):
		logger.Info("Example completed after 5 seconds")
	case <-ctx.Done():
		logger.Info("Example cancelled")
	}
}

func generateUUID() string {
	id, _ := uuid.NewRandom()

	return id.String()
}
