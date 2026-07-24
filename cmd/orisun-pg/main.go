package main

import (
	"context"
	"fmt"
	logger "log"
	"runtime"
	"time"

	c "github.com/OrisunLabs/Orisun/config"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	pg "github.com/OrisunLabs/Orisun/postgres"
	"github.com/OrisunLabs/Orisun/server"
	"github.com/common-nighthawk/go-figure"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	defer logger.Println("PostgreSQL server shutting down")
	fmt.Printf("Starting Orisun PostgreSQL %s (Go %s)\n", orisun.GetVersion(), runtime.Version())

	config := c.InitializeConfig()
	if config.Backend.Type != "" && config.Backend.Type != "postgres" {
		logger.Fatalf("orisun-pg only supports ORISUN_BACKEND=postgres, got %q", config.Backend.Type)
	}
	config.Backend.Type = "postgres"

	appLogger := l.InitializeDefaultLogger(config.Logging)
	myFigure := figure.NewColorFigure("Orisun PG", "isometric1", "cyan", true)
	appLogger.Infof("\n%s", myFigure.String())

	server.Run(context.Background(), config, appLogger, initializeBackend)
}

func initializeBackend(ctx context.Context, config c.AppConfig, js jetstream.JetStream, logger l.Logger) (server.Backend, error) {
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
	if runtime.Listener == nil {
		return backend, nil
	}

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
	return backend, nil
}
