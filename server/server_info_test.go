package server

import (
	"testing"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"

	"github.com/google/uuid"
)

func TestNewServerRuntimeInfo(t *testing.T) {
	t.Parallel()

	tests := map[string]grpcapi.StorageBackend{
		"postgres":     grpcapi.StorageBackend_STORAGE_BACKEND_POSTGRES,
		"sqlite":       grpcapi.StorageBackend_STORAGE_BACKEND_SQLITE,
		"foundationdb": grpcapi.StorageBackend_STORAGE_BACKEND_FOUNDATIONDB,
		"unknown":      grpcapi.StorageBackend_STORAGE_BACKEND_UNSPECIFIED,
	}
	for backend, want := range tests {
		cfg := config.AppConfig{Backend: config.BackendConfig{Type: backend}}
		info := newServerRuntimeInfo(cfg)

		version, buildTime, gitCommit := orisun.GetBuildInfo()
		if info.Version != version || info.BuildTime != buildTime || info.GitCommit != gitCommit {
			t.Errorf("%s build info = (%q, %q, %q), want (%q, %q, %q)",
				backend, info.Version, info.BuildTime, info.GitCommit, version, buildTime, gitCommit)
		}
		if info.Backend != want {
			t.Errorf("%s backend = %s, want %s", backend, info.Backend, want)
		}
		if _, err := uuid.Parse(info.NodeID); err != nil {
			t.Errorf("%s node ID %q is not a UUID: %v", backend, info.NodeID, err)
		}
		if len(info.Capabilities) != 5 {
			t.Errorf("%s capabilities = %v, want 5 entries", backend, info.Capabilities)
		}
	}
}

func TestNewServerRuntimeInfoGeneratesProcessIdentity(t *testing.T) {
	t.Parallel()

	cfg := config.AppConfig{Backend: config.BackendConfig{Type: "sqlite"}}
	first := newServerRuntimeInfo(cfg)
	second := newServerRuntimeInfo(cfg)
	if first.NodeID == second.NodeID {
		t.Fatalf("separate server runtime identities matched: %s", first.NodeID)
	}
}
