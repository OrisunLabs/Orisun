package foundationdb

import (
	"context"
	"fmt"

	boundarycatalog "github.com/OrisunLabs/Orisun/admin/slices/boundary_catalog"
	boundaryprovisioning "github.com/OrisunLabs/Orisun/admin/slices/boundary_provisioning"
	createboundary "github.com/OrisunLabs/Orisun/admin/slices/create_boundary"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	c "github.com/OrisunLabs/Orisun/config"
	fdbbackend "github.com/OrisunLabs/Orisun/foundationdb"
	"github.com/OrisunLabs/Orisun/internal/eventstoreadapter"
	l "github.com/OrisunLabs/Orisun/logging"
	natsruntime "github.com/OrisunLabs/Orisun/nats"
	"github.com/OrisunLabs/Orisun/orisun"
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
	closeFDB        func(context.Context)
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
	config.Backend.Type = "foundationdb"
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

	runtime, err := fdbbackend.InitializeFoundationDBRuntime(
		runCtx,
		config.FoundationDB,
		config.Admin,
		logger,
	)
	if err != nil {
		cancel()
		natsRuntime.Close()
		return nil, err
	}

	store, err := orisun.NewOrisunServer(
		runCtx, runtime.SaveEvents, runtime.GetEvents, runtime.LockProvider,
		js, logger,
	)
	if err != nil {
		cancel()
		natsRuntime.Close()
		if runtime.Close != nil {
			runtime.Close(context.WithoutCancel(ctx))
		}
		return nil, err
	}
	if err := store.EnsureBoundary(runCtx, config.Admin.Boundary); err != nil {
		cancel()
		natsRuntime.Close()
		if runtime.Close != nil {
			runtime.Close(context.WithoutCancel(ctx))
		}
		return nil, err
	}
	if err := store.EnableBoundaryActivationGate(config.Admin.Boundary); err != nil {
		cancel()
		natsRuntime.Close()
		if runtime.Close != nil {
			runtime.Close(context.WithoutCancel(ctx))
		}
		return nil, err
	}
	boundaryEvents := eventstoreadapter.New(runtime.SaveEvents, runtime.GetEvents, store.SubscribeToEvents)

	pollingManager := orisun.StartEventPolling(
		runCtx, config, runtime.LockProvider,
		runtime.GetEvents, js, runtime.EventPublishing, runtime.SignalProvider, logger,
	)
	if err := pollingManager.StartBoundary(config.Admin.Boundary); err != nil {
		cancel()
		natsRuntime.Close()
		if runtime.Close != nil {
			runtime.Close(context.WithoutCancel(ctx))
		}
		return nil, err
	}
	provisionBoundary := func(provisionCtx context.Context, definition boundarymodel.Definition) error {
		return runtime.ProvisionBoundary(provisionCtx, definition)
	}
	installBoundary := func(installCtx context.Context, definition boundarymodel.Definition) error {
		if err := runtime.InstallBoundary(installCtx, definition); err != nil {
			return err
		}
		return pollingManager.StartBoundary(definition.Name)
	}
	provisioningHandler := boundaryprovisioning.NewBoundaryProvisioningEventHandler(
		config.Admin.Boundary, boundaryEvents, boundaryEvents, provisionBoundary, store.EnsureBoundary,
	)
	runtimeHandler := boundaryprovisioning.NewBoundaryRuntimeEventHandler(
		config.Admin.Boundary, boundaryEvents, installBoundary, store.ActivateBoundary,
	)
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
		if runtime.Close != nil {
			runtime.Close(context.WithoutCancel(ctx))
		}
		return nil, fmt.Errorf("replay boundary catalog into embedded FoundationDB runtime: %w", err)
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
		indexManager:    runtime.AdminDB,
		adminBoundary:   config.Admin.Boundary,
		boundaryEvents:  boundaryEvents,
		cancel:          cancel,
		boundaryWorkers: boundaryWorkers,
		natsRuntime:     natsRuntime,
		closeFDB:        runtime.Close,
	}, nil
}

func (s *Store) CreateBoundary(ctx context.Context, definition boundarymodel.Definition) (boundarymodel.Boundary, error) {
	result, err := createboundary.CreateBoundaryCommandHandler(
		ctx,
		createboundary.CreateBoundaryCommand{
			Name: definition.Name, Description: definition.Description, Placement: definition.Placement,
			ExistedBeforeCatalog: definition.ExistedBeforeCatalog,
			Metadata:             createboundary.CommandMetadata{"source": "embedded_foundationdb", "operation": "create_boundary"},
		},
		s.adminBoundary, s.boundaryEvents, s.boundaryEvents,
	)
	return result.Boundary, err
}

func (s *Store) ListBoundaries(ctx context.Context) ([]boundarymodel.Boundary, error) {
	return boundarycatalog.ListBoundariesQueryHandler(
		ctx, boundarycatalog.ListBoundariesQuery{}, s.adminBoundary, s.boundaryEvents,
	)
}

func (s *Store) GetBoundary(ctx context.Context, name string) (boundarymodel.Boundary, error) {
	return boundarycatalog.GetBoundaryQueryHandler(
		ctx, boundarycatalog.GetBoundaryQuery{Name: name}, s.adminBoundary, s.boundaryEvents,
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
	if s.closeFDB != nil {
		s.closeFDB(ctx)
	}
	if s.natsRuntime != nil {
		s.natsRuntime.Close()
	}
}
