package sqlite

import (
	"context"
	"testing"
	"time"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	c "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/google/uuid"
)

func TestEmbeddedSQLiteCreatesBoundaryThroughCatalog(t *testing.T) {
	for _, inMemory := range []bool{false, true} {
		name := "disk"
		if inMemory {
			name = "memory"
		}
		t.Run(name, func(t *testing.T) { testEmbeddedSQLiteCreatesBoundaryThroughCatalog(t, inMemory) })
	}
}

func testEmbeddedSQLiteCreatesBoundaryThroughCatalog(t *testing.T, inMemory bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := c.InitializeConfig()
	cfg.Backend.Type = "sqlite"
	cfg.Sqlite.Dir = t.TempDir()
	cfg.Sqlite.InMemory = inMemory
	if inMemory {
		cfg.Sqlite.Dir = ""
	}
	cfg.Nats.Port = -1
	cfg.Nats.StoreDir = t.TempDir()
	cfg.Nats.Cluster.Enabled = false
	cfg.Nats.Cluster.Timeout = 5 * time.Second

	store, err := Start(ctx, cfg, testLogger{})
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer store.Close()

	created, err := store.CreateBoundary(ctx, boundarymodel.Definition{
		Name:        "sales",
		Description: "Sales domain",
		Placement:   boundarymodel.Placement{Backend: "sqlite", Namespace: "sales"},
	})
	if err != nil || created.Status != boundarymodel.StatusProvisioning {
		t.Fatalf("CreateBoundary() = %#v, %v", created, err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		boundary, getErr := store.GetBoundary(ctx, "sales")
		if getErr == nil && boundary.Status == boundarymodel.StatusActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("boundary did not activate: %#v, %v", boundary, getErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	events := []orisun.EventWithMapTags{{
		EventId: uuid.NewString(), EventType: "SaleOpened", Data: map[string]any{"sale_id": "1"},
	}}
	for {
		_, err = store.SaveEvents(ctx, events, "sales", nil, nil)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("local boundary runtime did not activate: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
