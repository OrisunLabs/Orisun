package sqlite

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
)

type provisioningTestLock struct{}

func (provisioningTestLock) Lock(context.Context, string) error { return nil }

func TestSqliteBoundaryProvisionerMakesBoundaryAvailableToRuntime(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dir := t.TempDir()
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	runtime, err := InitializeSqliteDatabaseRuntimeWithLockProvider(
		ctx,
		config.SqliteConfig{Dir: dir, Synchronous: "FULL"},
		config.AdminConfig{Boundary: "admin"},
		provisioningTestLock{},
		logger,
	)
	require.NoError(t, err)

	definition := orisun.BoundaryDefinition{
		Name:      "sales",
		Placement: orisun.BoundaryPlacement{Backend: "sqlite", Namespace: "sales"},
	}
	require.NoError(t, runtime.ProvisionBoundary(ctx, definition))
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- runtime.InstallBoundary(ctx, definition)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	prepared, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{
		EventId: "event-1", EventType: "SaleOpened", Data: map[string]any{"sale_id": "1"},
	}})
	require.NoError(t, err)
	_, _, err = runtime.SaveEvents.SavePrepared(ctx, prepared, "sales", nil, nil)
	require.NoError(t, err)
	events, err := runtime.GetEvents.GetBatch(ctx, &orisun.GetEventsRequest{Boundary: "sales"})
	require.NoError(t, err)
	require.Len(t, events, 1)

	require.FileExists(t, filepath.Join(dir, "sales.db"))
	require.FileExists(t, filepath.Join(dir, "sales_metadata.db"))
	saver := runtime.SaveEvents.(*SqliteSaveEvents)
	require.Len(t, saver.queues, 2)
}

func TestSqliteBoundaryProvisionerRejectsNamespaceMismatch(t *testing.T) {
	registry := NewBoundaryRegistry(nil, nil)
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	saver, err := newSqliteSaveEventsWithRegistry(registry, logger, config.SqliteGroupCommitConfig{})
	require.NoError(t, err)
	t.Cleanup(saver.close)
	provisioner := NewSqliteBoundaryProvisioner(
		config.SqliteConfig{Dir: t.TempDir()},
		config.AdminConfig{Boundary: "admin"},
		registry,
		saver,
	)
	err = provisioner.ProvisionBoundary(t.Context(), orisun.BoundaryDefinition{
		Name: "sales", Placement: orisun.BoundaryPlacement{Backend: "sqlite", Namespace: "other"},
	})
	require.Error(t, err)
}
