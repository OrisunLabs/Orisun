package server

import (
	"context"
	"fmt"
	"time"

	"github.com/OrisunLabs/Orisun/orisun/grpcapi"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	grpcHealthProbeInterval = 5 * time.Second
	grpcHealthProbeTimeout  = 2 * time.Second
)

type grpcHealthProbe struct {
	name  string
	check func(context.Context) error
}

var grpcHealthServices = []string{
	"",
	grpcapi.EventStore_ServiceDesc.ServiceName,
	grpcapi.Admin_ServiceDesc.ServiceName,
}

func newGRPCHealthServer() *health.Server {
	server := health.NewServer()
	setGRPCHealthStatus(server, healthpb.HealthCheckResponse_NOT_SERVING)
	return server
}

func setGRPCHealthStatus(server *health.Server, status healthpb.HealthCheckResponse_ServingStatus) {
	for _, service := range grpcHealthServices {
		server.SetServingStatus(service, status)
	}
}

func refreshGRPCHealth(
	ctx context.Context,
	server *health.Server,
	probeTimeout time.Duration,
	probes ...grpcHealthProbe,
) error {
	for _, probe := range probes {
		if probe.check == nil {
			setGRPCHealthStatus(server, healthpb.HealthCheckResponse_NOT_SERVING)
			return fmt.Errorf("%s: health probe is not configured", probe.name)
		}
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		err := probe.check(probeCtx)
		cancel()
		if err != nil {
			setGRPCHealthStatus(server, healthpb.HealthCheckResponse_NOT_SERVING)
			return fmt.Errorf("%s: %w", probe.name, err)
		}
	}
	setGRPCHealthStatus(server, healthpb.HealthCheckResponse_SERVING)
	return nil
}

func monitorGRPCHealth(
	ctx context.Context,
	server *health.Server,
	interval time.Duration,
	probeTimeout time.Duration,
	initialErr error,
	onTransition func(error),
	probes ...grpcHealthProbe,
) {
	wasReady := initialErr == nil
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := refreshGRPCHealth(ctx, server, probeTimeout, probes...)
			ready := err == nil
			if ready != wasReady {
				if onTransition != nil {
					onTransition(err)
				}
				wasReady = ready
			}
		}
	}
}
