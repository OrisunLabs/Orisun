package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestLoadConfigReadsAuthSessionTTL(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("ORISUN_AUTH_SESSION_TTL", "45m")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.Auth.SessionTTL != 45*time.Minute {
		t.Fatalf("session TTL = %s, want 45m", cfg.Auth.SessionTTL)
	}
}

func TestValidateConfigRejectsNonPositiveAuthSessionTTL(t *testing.T) {
	cfg := AppConfig{
		Backend: BackendConfig{Type: "sqlite"},
		Sqlite:  SqliteConfig{Dir: t.TempDir()},
		Admin:   AdminConfig{Boundary: "orisun_admin"},
	}

	err := validateConfig(cfg)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "ORISUN_AUTH_SESSION_TTL") {
		t.Fatalf("expected session TTL error, got %v", err)
	}
}
