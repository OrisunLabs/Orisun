package config

import (
	"testing"
	"time"

	"github.com/spf13/viper"
)

func TestPostgresBootstrapSchemaMappingContainsOnlyAdminBoundary(t *testing.T) {
	mappings := (PostgresDBConfig{AdminSchema: "control_data"}).BootstrapSchemaMapping("orisun_admin")
	if len(mappings) != 1 {
		t.Fatalf("mappings = %d, want 1", len(mappings))
	}
	mapping := mappings["orisun_admin"]
	if mapping.Boundary != "orisun_admin" || mapping.Schema != "control_data" {
		t.Fatalf("admin mapping = %#v", mapping)
	}
}

func TestLoadConfigReadsPostgresGroupCommit(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("ORISUN_PG_GC_MAX_BATCH_REQUESTS", "17")
	t.Setenv("ORISUN_PG_GC_MAX_BATCH_EVENTS", "31")
	t.Setenv("ORISUN_PG_GC_MAX_DELAY", "2ms")
	t.Setenv("ORISUN_PG_GC_MAX_PENDING", "43")
	t.Setenv("ORISUN_PG_GC_FLUSH_TIMEOUT", "7s")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	groupCommit := cfg.Postgres.GroupCommit
	if groupCommit.MaxBatchRequests != 17 ||
		groupCommit.MaxBatchEvents != 31 ||
		groupCommit.MaxDelay != 2*time.Millisecond ||
		groupCommit.MaxPending != 43 ||
		groupCommit.FlushTimeout != 7*time.Second {
		t.Fatalf("PostgreSQL group commit config = %#v", groupCommit)
	}
}
