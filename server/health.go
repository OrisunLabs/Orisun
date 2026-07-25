package server

import (
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

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
