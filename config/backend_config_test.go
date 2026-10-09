package config

import (
	"strings"
	"testing"
	"time"
)

func TestValidateConfigSupportedBackends(t *testing.T) {
	for _, backend := range []string{"", "postgres", "sqlite", "unknown"} {
		t.Run(backend, func(t *testing.T) {
			cfg := AppConfig{
				Backend:                   BackendConfig{Type: backend},
				Postgres:                  PostgresDBConfig{AdminSchema: "admin"},
				Sqlite:                    SqliteConfig{InMemory: true},
				Admin:                     AdminConfig{Boundary: "orisun_admin"},
				SubscriptionIdleThreshold: time.Second,
			}
			cfg.Auth.SessionTTL = time.Hour
			err := validateConfig(cfg)
			if backend == "unknown" {
				if err == nil || !strings.Contains(err.Error(), "unknown backend type") {
					t.Fatalf("unsupported backend %q: got %v", backend, err)
				}
			} else if err != nil {
				t.Fatalf("supported backend %q: %v", backend, err)
			}
		})
	}
}
