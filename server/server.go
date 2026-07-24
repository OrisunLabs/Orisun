package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/OrisunLabs/Orisun/admin"
	boundaryprovisioning "github.com/OrisunLabs/Orisun/admin/slices/boundary_provisioning"
	common "github.com/OrisunLabs/Orisun/admin/slices/common"
	createboundary "github.com/OrisunLabs/Orisun/admin/slices/create_boundary"
	"github.com/OrisunLabs/Orisun/admin/slices/create_user"
	"github.com/OrisunLabs/Orisun/admin/slices/dashboard/event_count"
	"github.com/OrisunLabs/Orisun/admin/slices/dashboard/user_count"
	up "github.com/OrisunLabs/Orisun/admin/slices/users_projection"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	c "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/eventstoreadapter"
	l "github.com/OrisunLabs/Orisun/logging"
	nats2 "github.com/OrisunLabs/Orisun/nats"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go/jetstream"
	_ "go.uber.org/automaxprocs" // auto-set GOMAXPROCS from cgroup CPU limit
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

// ensureJetStreamStreamIsProperlySetup ensures that a JetStream stream with the given name exists.
// If the stream doesn't exist, it creates a new one with default configuration.
func ensureJetStreamStreamIsProperlySetup(ctx context.Context, js jetstream.JetStream, streamName string, logger l.Logger) (jetstream.Stream, error) {
	natsStream, err := js.Stream(ctx, streamName)

	if err != nil && err.Error() != jetstream.ErrStreamNotFound.Error() {
		return nil, fmt.Errorf("failed to subscribe: %v", err)
	}

	if natsStream == nil {
		natsStream, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:              streamName,
			Subjects:          []string{streamName + ".>"},
			Storage:           jetstream.MemoryStorage,
			MaxConsumers:      -1, // Allow unlimited consumers
			MaxAge:            24 * time.Second,
			MaxMsgsPerSubject: 10,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to add stream: %v", err)
		}
		if logger.IsDebugEnabled() {
			logger.Debugf("stream info: %v", natsStream)
		}
	}

	return natsStream, nil
}

type Backend struct {
	SaveEvents            orisun.EventsSaver
	GetEvents             orisun.EventsRetriever
	LockProvider          orisun.LockProvider
	AdminDB               common.DB
	EventPublishing       orisun.EventPublishingTracker
	SignalProvider        func(string) orisun.EventSignal
	ProvisionBoundary     boundaryprovisioning.ProvisionBoundary
	InstallBoundary       boundaryprovisioning.InstallBoundary
	BootstrapBoundary     *boundarymodel.Definition
	PreexistingAdminStore bool
	Start                 func(context.Context)
	Close                 func(context.Context)
}

type BackendInitializer func(context.Context, c.AppConfig, jetstream.JetStream, l.Logger) (Backend, error)

func Run(ctx context.Context, config c.AppConfig, AppLogger l.Logger, initializeBackend BackendInitializer) {
	if AppLogger.IsDebugEnabled() {
		AppLogger.Debugf("config: %v", config)
	}

	// Log Go runtime tuning. GOMAXPROCS auto-set from cgroup via automaxprocs side-effect import.
	// GOMEMLIMIT/GOGC honor the standard Go env vars; tune in container/orchestrator manifest.
	memLimit := debug.SetMemoryLimit(-1)
	gcPercent := debug.SetGCPercent(-1)
	debug.SetGCPercent(gcPercent) // restore after read
	AppLogger.Infof("runtime: GOMAXPROCS=%d GOMEMLIMIT=%d GOGC=%d", runtime.GOMAXPROCS(0), memLimit, gcPercent)

	// Start pprof server if enabled
	if config.Pprof.Enabled {
		addr := fmt.Sprintf(":%s", config.Pprof.Port)
		go func() {
			AppLogger.Infof("pprof listening on %s", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				AppLogger.Errorf("pprof server error: %v", err)
			}
		}()
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Initialize NATS
	natsRuntime, err := nats2.Start(ctx, config.Nats, AppLogger)
	if err != nil {
		AppLogger.Fatalf("Failed to initialize NATS: %v", err)
	}
	defer natsRuntime.Close()
	js := natsRuntime.JetStream

	backend, err := initializeBackend(ctx, config, js, AppLogger)
	if err != nil {
		AppLogger.Fatalf("Failed to initialize backend: %v", err)
	}
	if backend.Close != nil {
		defer func() {
			closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer closeCancel()
			backend.Close(closeCtx)
		}()
	}
	if backend.Start != nil {
		backend.Start(ctx)
	}

	// Initialize EventStore
	eventStore := orisun.InitializeEventStore(
		ctx,
		config,
		backend.SaveEvents,
		backend.GetEvents,
		backend.LockProvider,
		backend.AdminDB,
		js,
		AppLogger,
	)
	if err := eventStore.EnableBoundaryActivationGate(config.Admin.Boundary); err != nil {
		AppLogger.Fatalf("Failed to enable boundary activation gate: %v", err)
	}

	signalProvider := backend.SignalProvider
	if signalProvider == nil {
		signalProvider = func(boundary string) orisun.EventSignal {
			return orisun.NewPollingSignal(1 * time.Second)
		}
	}
	pollingManager := orisun.StartEventPolling(ctx, config, backend.LockProvider, backend.GetEvents, js, backend.EventPublishing, signalProvider, AppLogger)
	if err := pollingManager.StartBoundary(config.Admin.Boundary); err != nil {
		AppLogger.Fatalf("Failed to start admin boundary publisher: %v", err)
	}
	provisionedBoundaries := map[string]struct{}{config.Admin.Boundary: {}}
	var provisionedBoundariesMu sync.Mutex
	var eventCountProjectors *eventCountProjectionManager

	if backend.ProvisionBoundary != nil && backend.InstallBoundary != nil {
		boundaryEvents := eventstoreadapter.New(
			backend.SaveEvents,
			backend.GetEvents,
			eventStore.SubscribeToAllEvents,
		)
		if err := createboundary.RequireMigratedCatalog(
			ctx,
			backend.PreexistingAdminStore,
			config.Admin.Boundary,
			boundaryEvents,
		); err != nil {
			AppLogger.Fatalf("PostgreSQL catalog upgrade check failed: %v", err)
		}
		provisionPhysicalBoundary := func(provisionCtx context.Context, definition boundarymodel.Definition) error {
			return backend.ProvisionBoundary(provisionCtx, definition)
		}
		installBoundary := func(installCtx context.Context, definition boundarymodel.Definition) error {
			if err := backend.InstallBoundary(installCtx, definition); err != nil {
				return err
			}
			if err := pollingManager.StartBoundary(definition.Name); err != nil {
				return err
			}
			provisionedBoundariesMu.Lock()
			provisionedBoundaries[definition.Name] = struct{}{}
			projectors := eventCountProjectors
			provisionedBoundariesMu.Unlock()
			if projectors != nil {
				projectors.StartBoundary(definition.Name)
			}
			return nil
		}
		provisioningHandler := boundaryprovisioning.NewBoundaryProvisioningEventHandler(
			config.Admin.Boundary,
			boundaryEvents,
			boundaryEvents,
			provisionPhysicalBoundary,
			eventStore.EnsureBoundary,
		)
		runtimeHandler := boundaryprovisioning.NewBoundaryRuntimeEventHandler(
			config.Admin.Boundary,
			boundaryEvents,
			installBoundary,
			func(_ context.Context, boundary string) error {
				return eventStore.ActivateBoundary(boundary)
			},
		)
		provisioningSubscriber := boundaryprovisioning.NewBoundaryProvisioningSubscriber(
			config.Admin.Boundary,
			boundaryEvents,
			boundaryEvents.Subscribe,
			provisioningHandler.Handle,
			AppLogger,
		)
		runtimeSubscriber := boundaryprovisioning.NewBoundaryRuntimeSubscriber(
			config.Admin.Boundary,
			boundaryEvents,
			boundaryEvents.Subscribe,
			runtimeHandler.Handle,
			AppLogger,
		)
		if backend.BootstrapBoundary != nil {
			created, bootstrapErr := createboundary.EnsureBootstrapBoundary(
				ctx,
				*backend.BootstrapBoundary,
				config.Admin.Boundary,
				boundaryEvents,
				boundaryEvents,
			)
			if bootstrapErr != nil {
				AppLogger.Fatalf("Failed to bootstrap the admin boundary catalog: %v", bootstrapErr)
			}
			AppLogger.Infof("Admin boundary catalog bootstrap completed: created=%t", created)
		}
		if err := runtimeSubscriber.Replay(ctx); err != nil {
			AppLogger.Fatalf("Failed to replay boundary catalog into this runtime: %v", err)
		}
		boundaryWorkers, boundaryCtx := errgroup.WithContext(ctx)
		boundaryWorkers.Go(func() error {
			runtimeSubscriber.Run(boundaryCtx)
			return nil
		})
		boundaryWorkers.Go(func() error {
			provisioningSubscriber.RunExclusive(boundaryCtx)
			return nil
		})
		defer func() {
			cancel()
			if err := boundaryWorkers.Wait(); err != nil {
				AppLogger.Errorf("Boundary workers stopped: %v", err)
			}
		}()
	} else if backend.ProvisionBoundary != nil || backend.InstallBoundary != nil || backend.BootstrapBoundary != nil {
		AppLogger.Fatalf("Backend boundary provisioning requires both physical provisioner and local installer")
	}

	// Start projectors
	pubsubStreamName := "ORISUN-ADMIN"
	var jetStreamPublishFunction = func(ctx context.Context, req *common.PublishRequest) error {
		if AppLogger.IsDebugEnabled() {
			AppLogger.Debugf("Publishing to jetstream: %v", req)
		}

		_, err := ensureJetStreamStreamIsProperlySetup(ctx, js, pubsubStreamName, AppLogger)
		if err != nil {
			return err
		}

		data, err := json.Marshal(req)
		if err != nil {
			return err
		}

		res, err := js.Publish(ctx, pubsubStreamName+"."+req.Subject, data)
		if err != nil {
			AppLogger.Errorf("Failed to publish to pubsub: %v", err)
			return err
		}

		if AppLogger.IsDebugEnabled() {
			AppLogger.Debugf("Published to jetstream: %v", res)
		}
		return nil
	}

	getUserCount := func() (user_count.UserCountReadModel, error) {
		count, err := backend.AdminDB.GetUsersCount()
		if err != nil {
			return user_count.UserCountReadModel{}, err
		}
		return user_count.UserCountReadModel{
			Count: count,
		}, nil
	}

	getEventCount := func(boundary string) (event_count.EventCountReadModel, error) {
		count, err := backend.AdminDB.GetEventsCount(boundary)
		if err != nil {
			return event_count.EventCountReadModel{}, err
		}
		return event_count.EventCountReadModel{
			Count: count,
		}, nil
	}

	startUserCountProjector(
		ctx,
		config.Admin.Boundary,
		backend.AdminDB.GetProjectorLastPosition,
		backend.AdminDB.UpdateProjectorPosition,
		eventStore.SubscribeToAllEvents,
		getUserCount,
		jetStreamPublishFunction,
		backend.AdminDB.SaveUsersCount,
		AppLogger,
	)

	newEventCountProjectors := newEventCountProjectionManager(
		ctx,
		backend.AdminDB.GetProjectorLastPosition,
		backend.AdminDB.UpdateProjectorPosition,
		eventStore.SubscribeToAllEvents,
		getEventCount,
		jetStreamPublishFunction,
		backend.AdminDB.SaveEventCount,
		AppLogger,
	)
	provisionedBoundariesMu.Lock()
	eventCountProjectors = newEventCountProjectors
	for boundary := range provisionedBoundaries {
		newEventCountProjectors.StartBoundary(boundary)
	}
	provisionedBoundariesMu.Unlock()

	startUserProjector(
		ctx,
		config.Admin.Boundary,
		backend.AdminDB.GetProjectorLastPosition,
		backend.AdminDB.UpdateProjectorPosition,
		backend.AdminDB.UpsertUser,
		backend.AdminDB.DeleteUser,
		backend.AdminDB.GetUserById,
		eventStore.SubscribeToAllEvents,
		eventStore.SaveEvents,
		eventStore.GetEvents,
		AppLogger,
	)

	startAuthUserProjector(
		ctx,
		config.Admin.Boundary,
		eventStore.SubscribeToAllEvents,
		AppLogger,
	)

	// Create default user
	err = createDefaultUser(
		ctx,
		config.Admin.Boundary,
		eventStore,
		AppLogger,
	)
	if err != nil {
		AppLogger.Infof("%v", err)
	}

	// Create authenticator for gRPC authentication
	authenticator := admin.NewAuthenticator(
		eventStore.GetEvents,
		AppLogger,
		config.Admin.Boundary,
		backend.AdminDB.GetUserByUsername,
	)

	// Start gRPC server
	startGRPCServer(
		ctx,
		config,
		eventStore,
		authenticator,
		backend.AdminDB,
		backend.SaveEvents,
		backend.GetEvents,
		AppLogger,
	)
}

func createDefaultUser(ctx context.Context, adminBoundary string, eventstore *orisun.EventStore, logger l.Logger) error {
	var userExistsError create_user.UserExistsError
	if _, err := create_user.CreateUser(
		ctx,
		"admin",
		"admin",
		"changeit",
		[]orisun.Role{orisun.RoleAdmin},
		adminBoundary,
		eventstore.SaveEvents,
		eventstore.GetEvents,
		logger,
		nil,
	); err != nil && !errors.As(err, &userExistsError) {
		return err
	}
	return nil
}

func startUserProjector(
	ctx context.Context,
	adminBoundary string,
	getProjectorLastPosition common.GetProjectorLastPositionType,
	updateProjectorPosition common.UpdateProjectorPositionType,
	createNewUser up.CreateNewUserType,
	deleteUser up.DeleteUserType,
	getUserById up.GetUserById,
	subscribeToEvents common.SubscribeToEventStoreType,
	saveEvents common.SaveEventsType,
	getEvents common.GetEventsType,
	logger l.Logger) {
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		userProjector := up.NewUserProjector(
			getProjectorLastPosition,
			updateProjectorPosition,
			createNewUser,
			deleteUser,
			getUserById,
			saveEvents,
			getEvents,
			logger,
			adminBoundary,
			subscribeToEvents,
		)
		backoff := orisun.Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			default:
				err := userProjector.Start(groupCtx)

				if err != nil {
					if logger.IsDebugEnabled() {
						logger.Debugf("Failed to start user projection (likely due to lock contention): %v - will retry", err)
					}
					if waitErr := backoff.Wait(groupCtx); waitErr != nil {
						return waitErr
					}
					continue
				}
				logger.Info("User projector started")
				return nil
			}
		}
	})
}

func startAuthUserProjector(
	ctx context.Context,
	adminBoundary string,
	subscribeToEvents common.SubscribeToEventStoreType,
	logger l.Logger) {
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		userProjector := admin.NewAuthUserProjector(
			logger,
			subscribeToEvents,
			adminBoundary,
		)
		backoff := orisun.Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			default:
				err := userProjector.Start(groupCtx)

				if err != nil {
					if logger.IsDebugEnabled() {
						logger.Debugf("Failed to start auth user projection (likely due to lock contention): %v - will retry", err)
					}
					if waitErr := backoff.Wait(groupCtx); waitErr != nil {
						return waitErr
					}
					continue
				}
				logger.Info("Auth user projector started")
				return nil
			}
		}
	})
}

func startUserCountProjector(
	ctx context.Context,
	adminBoundary string,
	getProjectorLastPosition common.GetProjectorLastPositionType,
	updateProjectorPosition common.UpdateProjectorPositionType,
	subscribeToEvents common.SubscribeToEventStoreType,
	getUserCount user_count.GetUserCount,
	publishToPubSub common.PublishToPubSubType,
	saveUserCount user_count.SaveUserCount,
	logger l.Logger) {

	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		userProjector := user_count.NewUserCountProjection(
			adminBoundary,
			getProjectorLastPosition,
			publishToPubSub,
			getUserCount,
			saveUserCount,
			subscribeToEvents,
			updateProjectorPosition,
			logger,
		)
		backoff := orisun.Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
		for {
			select {
			case <-groupCtx.Done():
				return groupCtx.Err()
			default:
				err := userProjector.Start(groupCtx)

				if err != nil {
					if logger.IsDebugEnabled() {
						logger.Debugf("Failed to start user count projection (likely due to lock contention): %v - will retry", err)
					}
					if waitErr := backoff.Wait(groupCtx); waitErr != nil {
						return waitErr
					}
					continue
				}
				logger.Info("User count projector started")
				return nil
			}
		}
	})

}

type eventCountProjectionManager struct {
	ctx                      context.Context
	getProjectorLastPosition common.GetProjectorLastPositionType
	updateProjectorPosition  common.UpdateProjectorPositionType
	subscribeToEvents        common.SubscribeToEventStoreType
	getEventCount            event_count.GetEventCount
	publishToPubSub          common.PublishToPubSubType
	saveEventCount           event_count.SaveEventCount
	logger                   l.Logger

	mu      sync.Mutex
	running map[string]struct{}
}

func newEventCountProjectionManager(
	ctx context.Context,
	getProjectorLastPosition common.GetProjectorLastPositionType,
	updateProjectorPosition common.UpdateProjectorPositionType,
	subscribeToEvents common.SubscribeToEventStoreType,
	getEventCount event_count.GetEventCount,
	publishToPubSub common.PublishToPubSubType,
	saveEventCount event_count.SaveEventCount,
	logger l.Logger,
) *eventCountProjectionManager {
	return &eventCountProjectionManager{
		ctx:                      ctx,
		getProjectorLastPosition: getProjectorLastPosition,
		updateProjectorPosition:  updateProjectorPosition,
		subscribeToEvents:        subscribeToEvents,
		getEventCount:            getEventCount,
		publishToPubSub:          publishToPubSub,
		saveEventCount:           saveEventCount,
		logger:                   logger,
		running:                  make(map[string]struct{}),
	}
}

func (m *eventCountProjectionManager) StartBoundary(boundary string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if _, exists := m.running[boundary]; exists {
		m.mu.Unlock()
		return
	}
	m.running[boundary] = struct{}{}
	m.mu.Unlock()

	go func() {
		eventProjector := event_count.NewEventCountProjection(
			boundary,
			m.getProjectorLastPosition,
			m.publishToPubSub,
			m.getEventCount,
			m.saveEventCount,
			m.subscribeToEvents,
			m.updateProjectorPosition,
			m.logger,
		)
		backoff := orisun.Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
		for m.ctx.Err() == nil {
			if err := eventProjector.Start(m.ctx); err != nil {
				if m.logger.IsDebugEnabled() {
					m.logger.Debugf("Failed to start event count projection for boundary %s (likely due to lock contention): %v - will retry", boundary, err)
				}
				if waitErr := backoff.Wait(m.ctx); waitErr != nil {
					return
				}
				continue
			}
			m.logger.Infof("Event count projector started for boundary %s", boundary)
			return
		}
	}()
}

func streamErrorInterceptor(logger l.Logger) grpc.StreamServerInterceptor {
	return func(srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err != nil {
			logger.Errorf("Error in streaming RPC %s: %v", info.FullMethod, err)
			if _, ok := status.FromError(err); ok {
				return err
			}
			return status.Errorf(codes.Internal, "Error: %v", err)
		}

		return nil
	}
}

func recoveryInterceptor(logger l.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Errorf("Panic in %s: %v\nStack Trace:\n%s", info.FullMethod, r, debug.Stack())
				err = status.Errorf(codes.Internal, "Internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

// clientAcceptsGzip returns true when the inbound RPC advertises gzip in grpc-accept-encoding.
// Server only enables compression when the client explicitly supports it (older clients
// without the gzip codec registered keep working uncompressed).
func clientAcceptsGzip(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, ae := range md.Get("grpc-accept-encoding") {
		for _, enc := range strings.Split(ae, ",") {
			if strings.TrimSpace(enc) == "gzip" {
				return true
			}
		}
	}
	return false
}

func compressionUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if clientAcceptsGzip(ctx) {
			_ = grpc.SetSendCompressor(ctx, "gzip")
		}
		return handler(ctx, req)
	}
}

func compressionStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if clientAcceptsGzip(ss.Context()) {
			_ = grpc.SetSendCompressor(ss.Context(), "gzip")
		}
		return handler(srv, ss)
	}
}

// LoadTLSCredentials creates TLS credentials from the configured certificate files.
func LoadTLSCredentials(config c.AppConfig, logger l.Logger) (credentials.TransportCredentials, error) {
	// Load server certificate and key
	cert, err := tls.LoadX509KeyPair(config.Grpc.TLS.CertFile, config.Grpc.TLS.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %w", err)
	}

	// Create TLS config
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	// If client authentication is required, load CA certificate
	if config.Grpc.TLS.ClientAuthRequired && config.Grpc.TLS.CAFile != "" {
		caCert, err := os.ReadFile(config.Grpc.TLS.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.ClientCAs = certPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	// Create credentials from TLS config
	creds := credentials.NewTLS(tlsConfig)

	logger.Infof("TLS enabled - Cert: %s, Key: %s, ClientAuth: %v",
		config.Grpc.TLS.CertFile,
		config.Grpc.TLS.KeyFile,
		config.Grpc.TLS.ClientAuthRequired,
	)

	return creds, nil
}

func startGRPCServer(
	ctx context.Context,
	config c.AppConfig,
	eventStore *orisun.EventStore,
	authenticator *admin.Authenticator,
	adminDB common.DB,
	boundarySaver orisun.EventsSaver,
	boundaryReader orisun.EventsRetriever,
	logger l.Logger,
) {
	// Initialize OpenTelemetry
	var otelShutdown func(context.Context) error
	if config.OpenTelemetry.Enabled {
		serviceName := config.OpenTelemetry.ServiceName
		if serviceName == "" {
			serviceName = "orisun"
		}
		var err error
		otelShutdown, err = admin.InitTracerWithContext(ctx, serviceName, config.OpenTelemetry.Endpoint, logger)
		if err != nil {
			logger.Errorf("Failed to initialize OpenTelemetry: %v", err)
		}
		if otelShutdown != nil {
			defer func() {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer shutdownCancel()
				if err := otelShutdown(shutdownCtx); err != nil {
					logger.Errorf("Failed to shutdown OpenTelemetry: %v", err)
				}
			}()
		}
	}

	// Prepare gRPC server options
	var serverOpts []grpc.ServerOption

	// Add TLS credentials if enabled
	if config.Grpc.TLS.Enabled {
		tlsCreds, err := LoadTLSCredentials(config, logger)
		if err != nil {
			logger.Fatalf("Failed to load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(tlsCreds))
		logger.Infof("gRPC server running in TLS mode")
	} else {
		logger.Infof("gRPC server running in insecure mode (TLS disabled)")
	}

	// Add interceptors and other options
	serverOpts = append(serverOpts,
		grpc.ChainUnaryInterceptor(
			compressionUnaryInterceptor(),
			admin.UnaryTracingInterceptor(logger),
			admin.UnaryAuthInterceptor(authenticator, logger),
			recoveryInterceptor(logger),
		),
		grpc.ChainStreamInterceptor(
			compressionStreamInterceptor(),
			admin.StreamTracingInterceptor(logger),
			admin.StreamAuthInterceptor(authenticator, logger),
			streamErrorInterceptor(logger),
		),
		grpc.ConnectionTimeout(config.Grpc.ConnectionTimeout),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    config.Grpc.KeepAliveTime,
			Timeout: config.Grpc.KeepAliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             config.Grpc.KeepaliveMinTime,
			PermitWithoutStream: config.Grpc.KeepalivePermitWithoutStream,
		}),
		grpc.MaxConcurrentStreams(config.Grpc.MaxConcurrentStreams),
		grpc.MaxRecvMsgSize(config.Grpc.MaxReceiveMessageSize),
		grpc.MaxSendMsgSize(config.Grpc.MaxSendMessageSize),
		grpc.InitialWindowSize(config.Grpc.InitialWindowSize),
		grpc.InitialConnWindowSize(config.Grpc.InitialConnWindowSize),
		grpc.WriteBufferSize(config.Grpc.WriteBufferSize),
		grpc.ReadBufferSize(config.Grpc.ReadBufferSize),
	)

	grpcServer := grpc.NewServer(serverOpts...)
	grpcapi.RegisterEventStoreServer(grpcServer, grpcapi.AdaptEventStore(eventStore))

	// Register Admin service
	grpcAdminServer := admin.NewGRPCAdminServerWithDependencies(
		logger,
		config.Admin.Boundary,
		admin.GRPCAdminDependencies{
			GetEvents:            eventStore.GetEvents,
			SaveEvents:           eventStore.SaveEvents,
			ListAdminUsers:       adminDB.ListAdminUsers,
			GetUserCount:         adminDB.GetUsersCount,
			GetEventCount:        adminDB.GetEventsCount,
			CredentialsValidator: authenticator,
			BoundarySaver:        boundarySaver,
			BoundaryReader:       boundaryReader,
		},
	)
	grpcapi.RegisterAdminServer(grpcServer, grpcAdminServer)

	if config.Grpc.EnableReflection {
		logger.Infof("Enabling gRPC server reflection")
		reflection.Register(grpcServer)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", config.Grpc.Port))
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}

	logger.Infof("gRPC server listening on port %s (TLS: %v)", config.Grpc.Port, config.Grpc.TLS.Enabled)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("Failed to serve: %v", err)
	}
}
