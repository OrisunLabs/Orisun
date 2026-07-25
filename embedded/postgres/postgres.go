package postgres

import (
	"context"
	"fmt"
	"time"

	boundarycatalog "github.com/OrisunLabs/Orisun/admin/slices/boundary_catalog"
	boundaryprovisioning "github.com/OrisunLabs/Orisun/admin/slices/boundary_provisioning"
	createboundary "github.com/OrisunLabs/Orisun/admin/slices/create_boundary"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	c "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/eventstoreadapter"
	l "github.com/OrisunLabs/Orisun/logging"
	natsruntime "github.com/OrisunLabs/Orisun/nats"
	"github.com/OrisunLabs/Orisun/orisun"
	pg "github.com/OrisunLabs/Orisun/postgres"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"
)

type Store struct {
	*orisun.OrisunServer

	indexManager    orisun.BoundaryIndexManager
	adminBoundary   string
	boundaryEvents  *eventstoreadapter.Adapter
	cancel          context.CancelFunc
	boundaryWorkers *errgroup.Group
	natsRuntime     *natsruntime.Runtime
	closePG         func(context.Context)
}

type StartOption func(*startOptions)

type startOptions struct {
	natsOptions []natsruntime.Option
}

func WithNATSURL(url string, opts ...natsgo.Option) StartOption {
	return func(o *startOptions) {
		o.natsOptions = append(o.natsOptions, natsruntime.WithURL(url, opts...))
	}
}

func WithNATSConnection(conn *natsgo.Conn, opts ...jetstream.JetStreamOpt) StartOption {
	return func(o *startOptions) {
		o.natsOptions = append(o.natsOptions, natsruntime.WithConnection(conn, opts...))
	}
}

func WithJetStream(js jetstream.JetStream) StartOption {
	return func(o *startOptions) {
		o.natsOptions = append(o.natsOptions, natsruntime.WithJetStream(js))
	}
}

func Start(ctx context.Context, config c.AppConfig, logger l.Logger, opts ...StartOption) (*Store, error) {
	config.Backend.Type = "postgres"
	runCtx, cancel := context.WithCancel(ctx)

	startOpts := startOptions{}
	for _, apply := range opts {
		apply(&startOpts)
	}

	natsRuntime, err := natsruntime.Start(runCtx, config.Nats, logger, startOpts.natsOptions...)
	if err != nil {
		cancel()
		return nil, err
	}
	js := natsRuntime.JetStream
	database := pg.InitializePostgresDatabaseRuntime(runCtx, config.Postgres, config.Admin, js, logger)
	saveEvents := database.SaveEvents
	getEvents := database.GetEvents
	lockProvider := database.LockProvider
	adminDB := database.AdminDB
	eventPublishing := database.EventPublishing
	pgListener := database.Listener

	store, err := orisun.NewOrisunServer(runCtx, saveEvents, getEvents, lockProvider, js, logger)
	if err != nil {
		cancel()
		natsRuntime.Close()
		return nil, err
	}
	if err := store.EnsureBoundary(runCtx, config.Admin.Boundary); err != nil {
		cancel()
		natsRuntime.Close()
		return nil, err
	}
	if err := store.EnableBoundaryActivationGate(config.Admin.Boundary); err != nil {
		cancel()
		natsRuntime.Close()
		return nil, err
	}
	boundaryEvents := eventstoreadapter.New(saveEvents, getEvents, store.SubscribeToEvents)
	if err := createboundary.RequireMigratedCatalog(
		runCtx,
		database.PreexistingAdminStore,
		config.Admin.Boundary,
		boundaryEvents,
	); err != nil {
		cancel()
		natsRuntime.Close()
		return nil, fmt.Errorf("PostgreSQL catalog upgrade check failed: %w", err)
	}

	var signalProvider func(string) orisun.EventSignal
	var stopListener context.CancelFunc
	var closePG func(context.Context)
	if pgListener != nil {
		listenerCtx, stop := context.WithCancel(runCtx)
		stopListener = stop
		go pgListener.Start(listenerCtx)
		signalProvider = func(boundary string) orisun.EventSignal {
			return pgListener.Signal(boundary, 30*time.Second)
		}
		closePG = func(ctx context.Context) {
			if stopListener != nil {
				stopListener()
			}
			waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
			defer waitCancel()
			pgListener.Close(waitCtx)
		}
	} else {
		signalProvider = func(boundary string) orisun.EventSignal {
			return orisun.NewPollingSignal(time.Second)
		}
	}

	pollingManager := orisun.StartEventPolling(runCtx, config, lockProvider, getEvents, js, eventPublishing, signalProvider, logger)
	if err := pollingManager.StartBoundary(config.Admin.Boundary); err != nil {
		cancel()
		natsRuntime.Close()
		return nil, err
	}
	provisionBoundary := func(provisionCtx context.Context, definition boundarymodel.Definition) error {
		return database.ProvisionBoundary(provisionCtx, definition)
	}
	installBoundary := func(installCtx context.Context, definition boundarymodel.Definition) error {
		if err := database.InstallBoundary(installCtx, definition); err != nil {
			return err
		}
		return pollingManager.StartBoundary(definition.Name)
	}
	provisioningHandler := boundaryprovisioning.NewBoundaryProvisioningEventHandler(
		config.Admin.Boundary,
		boundaryEvents,
		boundaryEvents,
		provisionBoundary,
		store.EnsureBoundary,
	)
	runtimeHandler := boundaryprovisioning.NewBoundaryRuntimeEventHandler(
		config.Admin.Boundary,
		boundaryEvents,
		installBoundary,
		store.ActivateBoundary,
	)
	createdAdmin, err := createboundary.EnsureBootstrapBoundary(
		runCtx,
		pg.AdminBoundaryDefinition(config.Postgres, config.Admin),
		config.Admin.Boundary,
		boundaryEvents,
		boundaryEvents,
	)
	if err != nil {
		cancel()
		natsRuntime.Close()
		if closePG != nil {
			closePG(context.WithoutCancel(ctx))
		}
		return nil, fmt.Errorf("bootstrap admin boundary catalog: %w", err)
	}
	logger.Infof("Admin boundary catalog bootstrap completed: created=%t", createdAdmin)
	provisioningSubscriber := boundaryprovisioning.NewBoundaryProvisioningSubscriber(
		config.Admin.Boundary,
		boundaryEvents,
		boundaryEvents.Subscribe,
		provisioningHandler.Handle,
		logger,
	)
	runtimeSubscriber := boundaryprovisioning.NewBoundaryRuntimeSubscriber(
		config.Admin.Boundary,
		boundaryEvents,
		boundaryEvents.Subscribe,
		runtimeHandler.Handle,
		logger,
	)
	if err := runtimeSubscriber.Replay(runCtx); err != nil {
		cancel()
		natsRuntime.Close()
		if closePG != nil {
			closePG(context.WithoutCancel(ctx))
		}
		return nil, fmt.Errorf("replay boundary catalog into embedded PostgreSQL runtime: %w", err)
	}
	boundaryWorkers, boundaryCtx := errgroup.WithContext(runCtx)
	boundaryWorkers.Go(func() error {
		runtimeSubscriber.Run(boundaryCtx)
		return nil
	})
	boundaryWorkers.Go(func() error {
		provisioningSubscriber.RunExclusive(boundaryCtx)
		return nil
	})

	return &Store{
		OrisunServer:    store,
		indexManager:    adminDB,
		adminBoundary:   config.Admin.Boundary,
		boundaryEvents:  boundaryEvents,
		cancel:          cancel,
		boundaryWorkers: boundaryWorkers,
		natsRuntime:     natsRuntime,
		closePG:         closePG,
	}, nil
}

// CreateBoundary emits a boundary definition event. The store's provisioning
// subscriber creates the physical boundary and activates it asynchronously.
func (s *Store) CreateBoundary(ctx context.Context, definition boundarymodel.Definition) (boundarymodel.Boundary, error) {
	result, err := createboundary.CreateBoundaryCommandHandler(
		ctx,
		createboundary.CreateBoundaryCommand{
			Name:                 definition.Name,
			Description:          definition.Description,
			Placement:            definition.Placement,
			ExistedBeforeCatalog: definition.ExistedBeforeCatalog,
			Metadata:             createboundary.CommandMetadata{"source": "embedded_postgres", "operation": "create_boundary"},
		},
		s.adminBoundary,
		s.boundaryEvents,
		s.boundaryEvents,
	)
	return result.Boundary, err
}

func (s *Store) ListBoundaries(ctx context.Context) ([]boundarymodel.Boundary, error) {
	return boundarycatalog.ListBoundariesQueryHandler(
		ctx,
		boundarycatalog.ListBoundariesQuery{},
		s.adminBoundary,
		s.boundaryEvents,
	)
}

func (s *Store) GetBoundary(ctx context.Context, name string) (boundarymodel.Boundary, error) {
	return boundarycatalog.GetBoundaryQueryHandler(
		ctx,
		boundarycatalog.GetBoundaryQuery{Name: name},
		s.adminBoundary,
		s.boundaryEvents,
	)
}

func (s *Store) CreateBoundaryIndex(ctx context.Context, boundary, name string, fields []orisun.BoundaryIndexField, conditions []orisun.BoundaryIndexCondition, combinator string) error {
	if err := s.RequireBoundaryActive(boundary); err != nil {
		return err
	}
	return s.indexManager.CreateBoundaryIndex(ctx, boundary, name, fields, conditions, combinator)
}

func (s *Store) DropBoundaryIndex(ctx context.Context, boundary, name string) error {
	if err := s.RequireBoundaryActive(boundary); err != nil {
		return err
	}
	return s.indexManager.DropBoundaryIndex(ctx, boundary, name)
}

func (s *Store) ListBoundaryIndexes(ctx context.Context, boundary string) ([]orisun.BoundaryIndex, error) {
	if err := s.RequireBoundaryActive(boundary); err != nil {
		return nil, err
	}
	return s.indexManager.ListBoundaryIndexes(ctx, boundary)
}

func (s *Store) GetBoundaryIndex(ctx context.Context, boundary, name string) (*orisun.BoundaryIndex, error) {
	if err := s.RequireBoundaryActive(boundary); err != nil {
		return nil, err
	}
	return s.indexManager.GetBoundaryIndex(ctx, boundary, name)
}

func (s *Store) NATSConnection() *natsgo.Conn {
	if s == nil || s.natsRuntime == nil {
		return nil
	}
	return s.natsRuntime.Conn
}

func (s *Store) JetStream() jetstream.JetStream {
	if s == nil || s.natsRuntime == nil {
		return nil
	}
	return s.natsRuntime.JetStream
}

func (s *Store) Close(ctx context.Context) {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.boundaryWorkers != nil {
		_ = s.boundaryWorkers.Wait()
	}
	if s.closePG != nil {
		s.closePG(ctx)
	}
	if s.natsRuntime != nil {
		s.natsRuntime.Close()
	}
}
