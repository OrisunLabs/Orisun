package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestValidateConfigAcceptsFoundationDB(t *testing.T) {
	cfg := AppConfig{
		Backend: BackendConfig{Type: "foundationdb"},
		FoundationDB: FoundationDBConfig{
			APIVersion: 730,
			Root:       "orisun",
		},
		Admin: AdminConfig{Boundary: "orisun_admin"},
	}
	cfg.Auth.SessionTTL = 24 * time.Hour

	if err := validateConfig(cfg); err != nil {
		t.Fatalf("validateConfig returned error: %v", err)
	}
}

func TestValidateConfigRejectsFoundationDBWithoutRoot(t *testing.T) {
	cfg := AppConfig{
		Backend:      BackendConfig{Type: "foundationdb"},
		FoundationDB: FoundationDBConfig{APIVersion: 730},
		Admin:        AdminConfig{Boundary: "orisun_admin"},
	}
	cfg.Auth.SessionTTL = 24 * time.Hour

	err := validateConfig(cfg)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "ORISUN_FDB_ROOT") {
		t.Fatalf("expected ORISUN_FDB_ROOT error, got %v", err)
	}
}

func TestLoadConfigReadsFoundationDBDefaults(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("ORISUN_BACKEND", "foundationdb")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.FoundationDB.APIVersion != 730 {
		t.Fatalf("expected api version 730, got %d", cfg.FoundationDB.APIVersion)
	}
	if cfg.FoundationDB.Root != "orisun" {
		t.Fatalf("expected root orisun, got %q", cfg.FoundationDB.Root)
	}
	if cfg.FoundationDB.TransactionTimeoutMs != 10000 {
		t.Fatalf("expected transaction timeout 10000, got %d", cfg.FoundationDB.TransactionTimeoutMs)
	}
}
