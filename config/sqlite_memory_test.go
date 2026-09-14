package config

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestSQLiteMemoryConfiguration(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("ORISUN_BACKEND", "sqlite")
	t.Setenv("ORISUN_NATS_CLUSTER_ENABLED", "false")
	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.False(t, cfg.Sqlite.InMemory)

	viper.Reset()
	t.Setenv("ORISUN_SQLITE_IN_MEMORY", "true")
	cfg, err = LoadConfig()
	require.NoError(t, err)
	require.True(t, cfg.Sqlite.InMemory)
	cfg.Sqlite.Dir = ""
	require.NoError(t, validateConfig(cfg))
	cfg.Nats.Cluster.Enabled = true
	require.ErrorContains(t, validateConfig(cfg), "does not support NATS clustering")
	cfg.Nats.Cluster.Enabled = false
	cfg.Sqlite.InMemory = false
	require.ErrorContains(t, validateConfig(cfg), "requires ORISUN_SQLITE_DIR")
}
