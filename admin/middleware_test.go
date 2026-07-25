package admin

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestUnaryAuthInterceptorAllowsHealthWithoutCredentials(t *testing.T) {
	t.Parallel()

	called := false
	response, err := UnaryAuthInterceptor(nil, authTestLogger{})(
		context.Background(),
		&healthpb.HealthCheckRequest{},
		&grpc.UnaryServerInfo{FullMethod: healthpb.Health_Check_FullMethodName},
		func(context.Context, any) (any, error) {
			called = true
			return "healthy", nil
		},
	)
	if err != nil {
		t.Fatalf("health interceptor returned an error: %v", err)
	}
	if !called || response != "healthy" {
		t.Fatalf("health handler result = (%v, %v), want (called, healthy)", called, response)
	}
}

func TestStreamAuthInterceptorAllowsHealthWatchWithoutCredentials(t *testing.T) {
	t.Parallel()

	called := false
	err := StreamAuthInterceptor(nil, authTestLogger{})(
		nil,
		healthTestServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: healthpb.Health_Watch_FullMethodName},
		func(any, grpc.ServerStream) error {
			called = true
			return nil
		},
	)
	if err != nil {
		t.Fatalf("health stream interceptor returned an error: %v", err)
	}
	if !called {
		t.Fatal("health watch handler was not called")
	}
}

func TestUnaryAuthInterceptorStillProtectsApplicationMethods(t *testing.T) {
	t.Parallel()

	_, err := UnaryAuthInterceptor(nil, authTestLogger{})(
		context.Background(),
		struct{}{},
		&grpc.UnaryServerInfo{FullMethod: "/orisun.EventStore/Ping"},
		func(context.Context, any) (any, error) {
			t.Fatal("protected handler was called without credentials")
			return nil, nil
		},
	)
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("protected method error = %s, want %s", got, codes.Unauthenticated)
	}
}

func TestOnlyStandardHealthMethodsAreUnauthenticated(t *testing.T) {
	t.Parallel()

	tests := map[string]bool{
		healthpb.Health_Check_FullMethodName:                        true,
		healthpb.Health_List_FullMethodName:                         true,
		healthpb.Health_Watch_FullMethodName:                        true,
		"/orisun.EventStore/Ping":                                   false,
		"/orisun.EventStore/GetServerInfo":                          false,
		"/orisun.Admin/ListUsers":                                   false,
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo": false,
	}
	for method, want := range tests {
		if got := isUnauthenticatedMethod(method); got != want {
			t.Errorf("isUnauthenticatedMethod(%q) = %v, want %v", method, got, want)
		}
	}
}

type healthTestServerStream struct {
	ctx context.Context
}

func (s healthTestServerStream) Context() context.Context   { return s.ctx }
func (healthTestServerStream) SetHeader(metadata.MD) error  { return nil }
func (healthTestServerStream) SendHeader(metadata.MD) error { return nil }
func (healthTestServerStream) SetTrailer(metadata.MD)       {}
func (healthTestServerStream) SendMsg(any) error            { return nil }
func (healthTestServerStream) RecvMsg(any) error            { return nil }
