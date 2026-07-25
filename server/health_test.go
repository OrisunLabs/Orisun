package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

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

func TestRefreshGRPCHealthTracksDependencyFailureAndRecovery(t *testing.T) {
	t.Parallel()

	server := newGRPCHealthServer()
	var storageUnavailable atomic.Bool
	probes := []grpcHealthProbe{
		{name: "JetStream", check: func(context.Context) error { return nil }},
		{name: "durable storage", check: func(context.Context) error {
			if storageUnavailable.Load() {
				return errors.New("database unavailable")
			}
			return nil
		}},
	}

	if err := refreshGRPCHealth(t.Context(), server, time.Second, probes...); err != nil {
		t.Fatalf("initial readiness: %v", err)
	}
	assertGRPCHealthStatus(t, server, "", healthpb.HealthCheckResponse_SERVING)

	storageUnavailable.Store(true)
	if err := refreshGRPCHealth(t.Context(), server, time.Second, probes...); err == nil {
		t.Fatal("expected readiness failure")
	}
	assertGRPCHealthStatus(t, server, grpcapi.EventStore_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	assertGRPCHealthStatus(t, server, grpcapi.Admin_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)

	storageUnavailable.Store(false)
	if err := refreshGRPCHealth(t.Context(), server, time.Second, probes...); err != nil {
		t.Fatalf("recovered readiness: %v", err)
	}
	assertGRPCHealthStatus(t, server, "", healthpb.HealthCheckResponse_SERVING)
}

func TestMonitorGRPCHealthPublishesTransitions(t *testing.T) {
	t.Parallel()

	server := newGRPCHealthServer()
	var unavailable atomic.Bool
	probe := grpcHealthProbe{name: "dependency", check: func(context.Context) error {
		if unavailable.Load() {
			return errors.New("unavailable")
		}
		return nil
	}}
	if err := refreshGRPCHealth(t.Context(), server, time.Second, probe); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	transitions := make(chan error, 2)
	go monitorGRPCHealth(
		ctx,
		server,
		time.Millisecond,
		time.Second,
		nil,
		func(err error) { transitions <- err },
		probe,
	)

	unavailable.Store(true)
	select {
	case err := <-transitions:
		if err == nil {
			t.Fatal("failure transition had no error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failure transition")
	}
	assertGRPCHealthStatus(t, server, "", healthpb.HealthCheckResponse_NOT_SERVING)

	unavailable.Store(false)
	select {
	case err := <-transitions:
		if err != nil {
			t.Fatalf("recovery transition error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for recovery transition")
	}
	assertGRPCHealthStatus(t, server, "", healthpb.HealthCheckResponse_SERVING)
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
