package main

import (
	"context"
	"fmt"
	logger "log"
	"runtime"
	"time"

	c "github.com/OrisunLabs/Orisun/config"
	fdbbackend "github.com/OrisunLabs/Orisun/foundationdb"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	pg "github.com/OrisunLabs/Orisun/postgres"
	"github.com/OrisunLabs/Orisun/server"
	sqlitebackend "github.com/OrisunLabs/Orisun/sqlite"
	"github.com/common-nighthawk/go-figure"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc/credentials"
)

func main() {
	defer logger.Println("Server shutting down")
	fmt.Printf("Starting Orisun %s (Go %s)\n", orisun.GetVersion(), runtime.Version())

	config := c.InitializeConfig()
	appLogger := l.InitializeDefaultLogger(config.Logging)
	myFigure := figure.NewColorFigure("Orisun", "isometric1", "cyan", true)
	appLogger.Infof("\n%s", myFigure.String())

	server.Run(context.Background(), config, appLogger, initializeBackend)
}

func initializeBackend(ctx context.Context, config c.AppConfig, js jetstream.JetStream, logger l.Logger) (server.Backend, error) {
	switch config.BackendType() {
	case "postgres":
		runtime := pg.InitializePostgresDatabaseRuntime(ctx, config.Postgres, config.Admin, js, logger)
		adminBoundary := pg.AdminBoundaryDefinition(config.Postgres, config.Admin)
		backend := server.Backend{
			SaveEvents:            runtime.SaveEvents,
			GetEvents:             runtime.GetEvents,
			LockProvider:          runtime.LockProvider,
			AdminDB:               runtime.AdminDB,
			EventPublishing:       runtime.EventPublishing,
			ProvisionBoundary:     runtime.ProvisionBoundary,
			InstallBoundary:       runtime.InstallBoundary,
			BootstrapBoundary:     &adminBoundary,
			PreexistingAdminStore: runtime.PreexistingAdminStore,
		}
		if runtime.Listener != nil {
			var stopListener context.CancelFunc
			backend.Start = func(parent context.Context) {
				listenerCtx, cancel := context.WithCancel(parent)
				stopListener = cancel
				go runtime.Listener.Start(listenerCtx)
			}
			backend.Close = func(ctx context.Context) {
				if stopListener != nil {
					stopListener()
				}
				waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
				defer waitCancel()
				runtime.Listener.Close(waitCtx)
			}
			backend.SignalProvider = func(boundary string) orisun.EventSignal {
				return runtime.Listener.Signal(boundary, 30*time.Second)
			}
		}
		return backend, nil
	case "sqlite":
		runtime, err := sqlitebackend.InitializeSqliteDatabaseRuntime(
			ctx,
			config.Sqlite,
			config.Admin,
			js,
			logger,
		)
		if err != nil {
			return server.Backend{}, err
		}
		adminBoundary := sqlitebackend.AdminBoundaryDefinition(config.Admin)
		return server.Backend{
			SaveEvents:        runtime.SaveEvents,
			GetEvents:         runtime.GetEvents,
			LockProvider:      runtime.LockProvider,
			AdminDB:           runtime.AdminDB,
			EventPublishing:   runtime.EventPublishing,
			SignalProvider:    runtime.SignalProvider,
			ProvisionBoundary: runtime.ProvisionBoundary,
			InstallBoundary:   runtime.InstallBoundary,
			BootstrapBoundary: &adminBoundary,
		}, nil
	case "foundationdb":
		runtime, err := fdbbackend.InitializeFoundationDBRuntime(
			ctx,
			config.FoundationDB,
			config.Admin,
			logger,
		)
		if err != nil {
			return server.Backend{}, err
		}
		return server.Backend{
			SaveEvents:        runtime.SaveEvents,
			GetEvents:         runtime.GetEvents,
			LockProvider:      runtime.LockProvider,
			AdminDB:           runtime.AdminDB,
			EventPublishing:   runtime.EventPublishing,
			SignalProvider:    runtime.SignalProvider,
			ProvisionBoundary: runtime.ProvisionBoundary,
			InstallBoundary:   runtime.InstallBoundary,
			Close:             runtime.Close,
		}, nil
	default:
		return server.Backend{}, fmt.Errorf("unsupported backend: %s", config.BackendType())
	}
}

func loadTLSCredentials(config c.AppConfig, logger l.Logger) (credentials.TransportCredentials, error) {
	return server.LoadTLSCredentials(config, logger)
}
