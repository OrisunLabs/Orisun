package main

import (
	"context"
	"fmt"
	logger "log"
	"runtime"

	c "github.com/OrisunLabs/Orisun/config"
	fdbbackend "github.com/OrisunLabs/Orisun/foundationdb"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/server"
	"github.com/common-nighthawk/go-figure"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	defer logger.Println("FoundationDB server shutting down")
	fmt.Printf("Starting Orisun FoundationDB %s (Go %s)\n", orisun.GetVersion(), runtime.Version())

	config := c.InitializeConfig()
	if config.Backend.Type != "" && config.Backend.Type != "foundationdb" {
		logger.Fatalf("orisun-fdb only supports ORISUN_BACKEND=foundationdb, got %q", config.Backend.Type)
	}
	config.Backend.Type = "foundationdb"

	appLogger := l.InitializeDefaultLogger(config.Logging)
	myFigure := figure.NewColorFigure("Orisun FDB", "isometric1", "cyan", true)
	appLogger.Infof("\n%s", myFigure.String())

	server.Run(context.Background(), config, appLogger, initializeBackend)
}

func initializeBackend(ctx context.Context, config c.AppConfig, js jetstream.JetStream, logger l.Logger) (server.Backend, error) {
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
}
