package sqlite

import (
	"context"
	"strings"
	"testing"
	"time"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	c "github.com/OrisunLabs/Orisun/config"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedSQLiteSubscriptionWatchdogUsesBackendOperatorsWithoutRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cfg := c.InitializeConfig()
	cfg.Backend.Type = "sqlite"
	cfg.Sqlite.Dir = t.TempDir()
	cfg.Nats.Port = -1
	cfg.Nats.StoreDir = t.TempDir()
	cfg.Nats.Cluster.Enabled = false
	cfg.SubscriptionIdleThreshold = 20 * time.Millisecond
	store, err := Start(ctx, cfg, testLogger{})
	require.NoError(t, err)
	defer store.Close()
	_, err = store.CreateBoundary(ctx, boundarymodel.Definition{Name: "sales", Placement: boundarymodel.Placement{Backend: "sqlite", Namespace: "sales"}})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		boundary, err := store.GetBoundary(ctx, "sales")
		return err == nil && boundary.Status == boundarymodel.StatusActive && store.RequireBoundaryActive("sales") == nil
	}, 5*time.Second, 10*time.Millisecond)

	query := coreeventstore.Query{Criteria: []coreeventstore.Criterion{
		{Tags: []coreeventstore.Tag{{Key: "__eventType", Value: "SaleAdded"}, {Key: "amount", Value: "10", Operator: "gte"}, {Key: "amount", Value: "20", Operator: "lt"}}},
		{Tags: []coreeventstore.Tag{{Key: "override", Value: "true", Operator: "eq"}}},
	}}
	received := make(chan coreeventstore.ReadEvent, 3)
	subCtx, stopSubscription := context.WithCancel(ctx)
	defer stopSubscription()
	result := make(chan error, 1)
	go func() {
		result <- store.SubscribeToEvents(subCtx, coreeventstore.SubscribeRequest{Boundary: "sales", SubscriberName: "native-query", AfterPosition: &coreeventstore.Position{}, Query: query}, func(_ context.Context, event coreeventstore.ReadEvent) error {
			select {
			case received <- event:
				return nil
			case <-subCtx.Done():
				return subCtx.Err()
			}
		})
	}()
	// Stop hint forwarding while keeping the independent subscription lease
	// available. The idle watchdog must publish hints that deliver native matches.
	store.notifications.Stop()
	large := strings.Repeat("x", 2*1024*1024)
	_, err = store.SaveEventsV2(ctx, []orisun.EventWithMapTags{
		{EventId: "match-10", EventType: "SaleAdded", Data: map[string]any{"amount": 10, "payload": large}},
		{EventId: "not-matching", EventType: "SaleAdded", Data: map[string]any{"amount": 25}},
		{EventId: "match-19", EventType: "SaleAdded", Data: map[string]any{"amount": 19}},
		{EventId: "override", EventType: "Other", Data: map[string]any{"override": true}},
	}, "sales", nil)
	require.NoError(t, err)
	var previous coreeventstore.Position
	for _, id := range []string{"match-10", "match-19", "override"} {
		select {
		case event := <-received:
			require.Equal(t, id, event.EventID)
			require.True(t, event.Position.After(previous))
			previous = event.Position
			if id == "match-10" {
				require.Contains(t, event.Data, large)
			}
		case <-ctx.Done():
			t.Fatal("notification outage stranded filtered events")
		}
	}
	stopSubscription()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("subscription did not stop")
	}
}
