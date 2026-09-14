package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestMemoryDatabaseLifetimeAndIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cfg := config.SqliteConfig{InMemory: true, Dir: filepath.Join(t.TempDir(), "unused"), ReadPoolSize: 8, TempStore: "FILE"}
	open := func(name string) *BoundaryPools {
		p, err := OpenBoundaryPoolsWithConfig(ctx, cfg, name, "admin")
		require.NoError(t, err)
		return p
	}
	first := open("sales")
	second := open("sales") // Same name and directory, independent database owner.
	defer second.Close()
	other := open("other")
	defer other.Close()
	metadata, err := OpenMetadataPoolsWithConfig(ctx, cfg, "sales")
	require.NoError(t, err)
	defer metadata.Close()

	conn, err := first.Write.Take(ctx)
	require.NoError(t, err)
	require.NoError(t, sqlitex.ExecuteScript(conn, "CREATE TABLE lifetime_test (value INTEGER); INSERT INTO lifetime_test VALUES (42);", nil))
	first.Write.Put(conn)
	conn, err = first.Read.Take(ctx)
	require.NoError(t, err)
	require.NoError(t, sqlitex.Execute(conn, "SELECT value FROM lifetime_test", &sqlitex.ExecOptions{ResultFunc: func(s *sqlite.Stmt) error {
		require.Equal(t, int64(42), s.ColumnInt64(0))
		return nil
	}}))
	require.NoError(t, sqlitex.Execute(conn, "PRAGMA database_list", &sqlitex.ExecOptions{ResultFunc: func(s *sqlite.Stmt) error {
		require.Empty(t, s.ColumnText(2), "database must not have a disk filename")
		return nil
	}}))
	require.NoError(t, sqlitex.Execute(conn, "PRAGMA temp_store", &sqlitex.ExecOptions{ResultFunc: func(s *sqlite.Stmt) error {
		require.Equal(t, int64(2), s.ColumnInt64(0))
		return nil
	}}))
	first.Read.Put(conn)
	require.NoError(t, first.Close())
	reopened := open("sales")
	defer reopened.Close()
	for _, p := range []*BoundaryPools{second, other, metadata, reopened} {
		conn, err := p.Read.Take(ctx)
		require.NoError(t, err)
		exists, err := tableExists(conn, "lifetime_test")
		p.Read.Put(conn)
		require.NoError(t, err)
		require.False(t, exists)
	}
	require.NoDirExists(t, cfg.Dir)
}

func TestMemoryRuntimeConsistencyOrderingAndCheckpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	cfg := config.SqliteConfig{InMemory: true, Dir: filepath.Join(t.TempDir(), "unused")}
	start := func() *DatabaseRuntime {
		r, err := InitializeSqliteDatabaseRuntimeWithLockProvider(ctx, cfg, config.AdminConfig{Boundary: "admin"}, provisioningTestLock{}, logger)
		require.NoError(t, err)
		return r
	}
	r := start()
	definition := eventstore.BoundaryDefinition{Name: "sales", Placement: eventstore.BoundaryPlacement{Backend: "sqlite", Namespace: "sales"}}
	require.NoError(t, r.ProvisionBoundary(ctx, definition))
	_, err = r.GetEvents.GetBatch(ctx, &eventstore.GetEventsRequest{Boundary: "sales"})
	require.Error(t, err, "provisioning must not expose an uninstalled boundary")
	require.NoError(t, r.InstallBoundary(ctx, definition))
	saver := r.SaveEvents.(*SqliteSaveEvents)
	query := &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{{Key: "sale_id", Value: "1"}}}}}
	save := func(id string, expected *eventstore.Position, query *eventstore.Query) error {
		_, _, err := saver.Save(ctx, []eventstore.EventWithMapTags{mustEvent(t, "SaleUpdated", map[string]any{"sale_id": id}, nil)}, "sales", expected, query)
		return err
	}
	require.NoError(t, save("1", nil, query))
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(save("1", nil, query)))
	require.NoError(t, save("1", &eventstore.Position{CommitPosition: 1, PreparePosition: 1}, query))
	require.NoError(t, r.InstallBoundary(ctx, definition)) // Reinstallation preserves data.

	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := range 20 {
		wg.Add(2)
		go func() { defer wg.Done(); errs <- save(fmt.Sprint(i+2), nil, nil) }()
		go func() {
			defer wg.Done()
			_, err := r.GetEvents.GetBatch(ctx, &eventstore.GetEventsRequest{Boundary: "sales", Direction: eventstore.Direction_ASC, Count: 100})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	events, err := r.GetEvents.GetBatch(ctx, &eventstore.GetEventsRequest{Boundary: "sales", Direction: eventstore.Direction_ASC, Count: 100})
	require.NoError(t, err)
	require.Len(t, events, 22)
	for i, event := range events {
		require.Equal(t, int64(i+1), event.PreparePosition)
		if i > 0 {
			require.GreaterOrEqual(t, event.CommitPosition, events[i-1].CommitPosition)
		}
	}
	last := events[len(events)-1]
	require.NoError(t, r.EventPublishing.InsertLastPublishedEvent(ctx, "sales", last.CommitPosition, last.PreparePosition))
	pos, err := r.EventPublishing.GetLastPublishedEventPosition(ctx, "sales")
	require.NoError(t, err)
	require.Equal(t, last.PreparePosition, pos.PreparePosition)
	adminPos, err := r.EventPublishing.GetLastPublishedEventPosition(ctx, "admin")
	require.NoError(t, err)
	require.Equal(t, eventstore.NotExistsPosition(), adminPos)

	// A fresh runtime has neither the previous boundary installation nor its data/checkpoint.
	fresh := start()
	_, err = fresh.GetEvents.GetBatch(ctx, &eventstore.GetEventsRequest{Boundary: "sales"})
	require.Error(t, err)
	require.NoError(t, fresh.ProvisionBoundary(ctx, definition))
	require.NoError(t, fresh.InstallBoundary(ctx, definition))
	empty, err := fresh.GetEvents.GetBatch(ctx, &eventstore.GetEventsRequest{Boundary: "sales"})
	require.NoError(t, err)
	require.Empty(t, empty)
	pos, err = fresh.EventPublishing.GetLastPublishedEventPosition(ctx, "sales")
	require.NoError(t, err)
	require.Equal(t, eventstore.NotExistsPosition(), pos)
	require.NoDirExists(t, cfg.Dir)
}
