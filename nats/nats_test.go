package nats

import (
	"context"
	"testing"
	"time"

	c "github.com/OrisunLabs/Orisun/config"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
)

type testLogger struct{}

func (testLogger) IsDebugEnabled() bool  { return false }
func (testLogger) Debug(...any)          {}
func (testLogger) Debugf(string, ...any) {}
func (testLogger) Info(...any)           {}
func (testLogger) Infof(string, ...any)  {}
func (testLogger) Warn(...any)           {}
func (testLogger) Warnf(string, ...any)  {}
func (testLogger) Error(...any)          {}
func (testLogger) Errorf(string, ...any) {}
func (testLogger) Fatal(...any)          {}
func (testLogger) Fatalf(string, ...any) {}

func testNatsConfig(t *testing.T) c.NatsConfig {
	t.Helper()
	return c.NatsConfig{
		ServerName: "orisun-test",
		Port:       -1,
		MaxPayload: 1048576,
		StoreDir:   t.TempDir(),
		Cluster: c.NatsClusterConfig{
			Timeout: 5 * time.Second,
		},
	}
}

func TestStartUsesEmbeddedNATSByDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runtime, err := Start(ctx, testNatsConfig(t), testLogger{})
	if err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
	defer runtime.Close()

	if runtime.Server == nil {
		t.Fatal("expected embedded NATS server")
	}
	if runtime.Conn == nil {
		t.Fatal("expected NATS connection")
	}
	if runtime.JetStream == nil {
		t.Fatal("expected JetStream context")
	}
	if err := runtime.HealthCheck(ctx); err != nil {
		t.Fatalf("HealthCheck() returned error: %v", err)
	}
	runtime.Close()
	if err := runtime.HealthCheck(ctx); err == nil {
		t.Fatal("HealthCheck() succeeded after runtime close")
	}
}

func TestStartConnectsToExternalNATSURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	external, err := startNATSServer(natsserver.Options{
		ServerName: "orisun-test-external",
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
	}, 5*time.Second, testLogger{})
	if err != nil {
		t.Fatalf("failed to start external NATS: %v", err)
	}
	defer external.Shutdown()

	cfg := testNatsConfig(t)
	cfg.URL = external.ClientURL()
	runtime, err := Start(ctx, cfg, testLogger{})
	if err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}

	if runtime.Server != nil {
		t.Fatal("did not expect Orisun to start an embedded NATS server")
	}
	if runtime.Conn == nil {
		t.Fatal("expected NATS connection")
	}

	runtime.Close()
	if !external.Running() {
		t.Fatal("external NATS server should remain caller-owned")
	}
}

func TestStartReturnsErrorForInvalidExternalNATSURL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cfg := testNatsConfig(t)
	cfg.URL = "nats://127.0.0.1:1"
	_, err := Start(ctx, cfg, testLogger{}, WithConnectOptions(natsgo.Timeout(10*time.Millisecond)))
	if err == nil {
		t.Fatal("expected error")
	}
}
