package server

import (
	"context"
	"testing"

	"github.com/OrisunLabs/Orisun/orisun/grpcapi"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestGRPCHealthLifecycle(t *testing.T) {
	t.Parallel()

	server := newGRPCHealthServer()
	assertGRPCHealthStatus(t, server, "", healthpb.HealthCheckResponse_NOT_SERVING)
	assertGRPCHealthStatus(t, server, grpcapi.EventStore_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	assertGRPCHealthStatus(t, server, grpcapi.Admin_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)

	setGRPCHealthStatus(server, healthpb.HealthCheckResponse_SERVING)
	assertGRPCHealthStatus(t, server, "", healthpb.HealthCheckResponse_SERVING)
	assertGRPCHealthStatus(t, server, grpcapi.EventStore_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	assertGRPCHealthStatus(t, server, grpcapi.Admin_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)

	server.Shutdown()
	assertGRPCHealthStatus(t, server, "", healthpb.HealthCheckResponse_NOT_SERVING)
	assertGRPCHealthStatus(t, server, grpcapi.EventStore_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	assertGRPCHealthStatus(t, server, grpcapi.Admin_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
}

func assertGRPCHealthStatus(
	t *testing.T,
	server healthpb.HealthServer,
	service string,
	want healthpb.HealthCheckResponse_ServingStatus,
) {
	t.Helper()
	response, err := server.Check(context.Background(), &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		t.Fatalf("Check(%q) returned an error: %v", service, err)
	}
	if response.Status != want {
		t.Fatalf("Check(%q) status = %s, want %s", service, response.Status, want)
	}
}
