package postgres

import (
	"context"
	"errors"
	"github.com/OrisunLabs/Orisun/boundary"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/orisun"
)

func TestBoundaryRegistryRegisterMakesQueriesAvailable(t *testing.T) {
	registry := NewBoundaryRegistry(map[string]config.BoundaryToPostgresSchemaMapping{
		"admin": {Boundary: "admin", Schema: "public"},
	})

	if err := registry.Register("sales", "tenant_data"); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	entry, ok := registry.lookup("sales")
	if !ok {
		t.Fatal("registered boundary was not found")
	}
	for name, query := range map[string]string{
		"insert requests":       entry.insertEventRequests,
		"select events":         entry.selectEvents,
		"select latest":         entry.selectLatest,
		"get last published":    entry.getLastPublished,
		"insert last published": entry.insertLastPublished,
		"get event count":       entry.getEventCount,
		"fallback event count":  entry.fallbackGetEventCount,
		"save event count":      entry.saveEventCount,
	} {
		if !strings.Contains(query, "tenant_data") {
			t.Errorf("%s query does not use registered schema: %q", name, query)
		}
	}

	mappings := registry.Mappings()
	mappings["sales"] = config.BoundaryToPostgresSchemaMapping{Boundary: "sales", Schema: "changed"}
	entry, _ = registry.lookup("sales")
	if entry.mapping.Schema != "tenant_data" {
		t.Fatal("Mappings() exposed mutable registry state")
	}
}

func TestBoundaryRegistryRegisterIsIdempotentAndRejectsConflicts(t *testing.T) {
	registry := NewBoundaryRegistry(nil)
	if err := registry.Register("sales", "tenant_data"); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	if err := registry.Register("sales", "tenant_data"); err != nil {
		t.Fatalf("idempotent Register() error = %v", err)
	}
	if err := registry.Register("sales", "other_schema"); err == nil {
		t.Fatal("conflicting Register() error = nil")
	}
}

func TestBoundaryRegistryRegisterValidatesIdentifiers(t *testing.T) {
	registry := NewBoundaryRegistry(nil)
	for _, test := range []struct {
		boundary string
		schema   string
	}{
		{boundary: "bad-boundary", schema: "public"},
		{boundary: "sales", schema: "bad-schema"},
	} {
		if err := registry.Register(test.boundary, test.schema); err == nil {
			t.Errorf("Register(%q, %q) error = nil", test.boundary, test.schema)
		}
	}
}

func TestPostgresBoundaryProvisionerMigratesBeforeLocalRegistration(t *testing.T) {
	registry := NewBoundaryRegistry(nil)
	var calls atomic.Int32
	provisioner := newPostgresBoundaryProvisioner(registry, func(_ context.Context, boundary, schema string) error {
		calls.Add(1)
		if boundary != "sales" || schema != "tenant_data" {
			t.Fatalf("migrate(%q, %q), want sales, tenant_data", boundary, schema)
		}
		if _, found := registry.lookup(boundary); found {
			t.Fatal("boundary registered before migration completed")
		}
		return nil
	})
	definition := orisun.BoundaryDefinition{
		Name:      "sales",
		Placement: orisun.BoundaryPlacement{Backend: "postgres", Namespace: "tenant_data"},
	}

	if err := provisioner.ProvisionBoundary(t.Context(), definition); err != nil {
		t.Fatalf("ProvisionBoundary() error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("migration calls = %d, want 1", got)
	}
	if _, found := registry.lookup("sales"); found {
		t.Fatal("physical provisioning must not mutate the process-local registry")
	}
	if err := provisioner.InstallBoundary(t.Context(), definition); err != nil {
		t.Fatalf("InstallBoundary() error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("migration calls after installation = %d, want 1", got)
	}
	if err := provisioner.InstallBoundary(t.Context(), definition); err != nil {
		t.Fatalf("idempotent InstallBoundary() error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("migration calls after duplicate installation = %d, want 1", got)
	}
	if entry, found := registry.lookup("sales"); !found || entry.mapping.Schema != "tenant_data" {
		t.Fatalf("registered entry = %#v, found = %v", entry, found)
	}
}

func TestPostgresBoundaryProvisionerDoesNotRegisterFailedMigration(t *testing.T) {
	registry := NewBoundaryRegistry(nil)
	wantErr := errors.New("migration failed")
	provisioner := newPostgresBoundaryProvisioner(registry, func(context.Context, string, string) error {
		return wantErr
	})
	definition := orisun.BoundaryDefinition{
		Name:      "sales",
		Placement: orisun.BoundaryPlacement{Backend: "postgres", Namespace: "tenant_data"},
	}

	err := provisioner.ProvisionBoundary(t.Context(), definition)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ProvisionBoundary() error = %v, want wrapping %v", err, wantErr)
	}
	if _, found := registry.lookup("sales"); found {
		t.Fatal("failed migration registered boundary")
	}
}

func TestPostgresBoundaryInstallerDoesNotRegisterFailedMigration(t *testing.T) {
	registry := NewBoundaryRegistry(nil)
	wantErr := errors.New("migration failed")
	provisioner := newPostgresBoundaryProvisioner(registry, func(context.Context, string, string) error {
		return wantErr
	})
	definition := orisun.BoundaryDefinition{
		Name:      "sales",
		Placement: orisun.BoundaryPlacement{Backend: "postgres", Namespace: "tenant_data"},
	}

	err := provisioner.InstallBoundary(t.Context(), definition)
	if !errors.Is(err, wantErr) {
		t.Fatalf("InstallBoundary() error = %v, want wrapping %v", err, wantErr)
	}
	if _, found := registry.lookup("sales"); found {
		t.Fatal("failed installation registered boundary")
	}
}

func TestPostgresBoundaryInstallerHandlesConcurrentDuplicateDeliveries(t *testing.T) {
	registry := NewBoundaryRegistry(nil)
	var calls atomic.Int32
	provisioner := newPostgresBoundaryProvisioner(registry, func(context.Context, string, string) error {
		calls.Add(1)
		return nil
	})
	definition := boundary.Definition{
		Name:      "sales",
		Placement: boundary.Placement{Backend: "postgres", Namespace: "tenant_data"},
	}

	var wait sync.WaitGroup
	errorsByCall := make(chan error, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsByCall <- provisioner.InstallBoundary(context.Background(), definition)
		}()
	}
	wait.Wait()
	close(errorsByCall)
	for err := range errorsByCall {
		if err != nil {
			t.Errorf("InstallBoundary() error = %v", err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("migration calls = %d, want 1", got)
	}
	if _, found := registry.lookup("sales"); !found {
		t.Fatal("concurrent local installation did not register boundary")
	}
}

func TestPostgresBoundaryProvisionerRejectsPlacementBeforeMigration(t *testing.T) {
	registry := NewBoundaryRegistry(nil)
	var calls atomic.Int32
	provisioner := newPostgresBoundaryProvisioner(registry, func(context.Context, string, string) error {
		calls.Add(1)
		return nil
	})

	definitions := []orisun.BoundaryDefinition{
		{Name: "new_boundary", Placement: orisun.BoundaryPlacement{Backend: "sqlite", Namespace: "tenant_data"}},
		{Name: "new_boundary", Placement: orisun.BoundaryPlacement{Backend: "postgres", Namespace: "bad-schema"}},
	}
	for _, definition := range definitions {
		if err := provisioner.ProvisionBoundary(t.Context(), definition); err == nil {
			t.Errorf("ProvisionBoundary(%#v) error = nil", definition)
		}
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("migration calls = %d, want 0", got)
	}
}

func TestPostgresBoundaryInstallerRejectsConflictingLocalPlacement(t *testing.T) {
	registry := NewBoundaryRegistry(map[string]config.BoundaryToPostgresSchemaMapping{
		"sales": {Boundary: "sales", Schema: "tenant_data"},
	})
	provisioner := newPostgresBoundaryProvisioner(registry, func(context.Context, string, string) error {
		return nil
	})
	err := provisioner.InstallBoundary(t.Context(), orisun.BoundaryDefinition{
		Name:      "sales",
		Placement: orisun.BoundaryPlacement{Backend: "postgres", Namespace: "different"},
	})
	if err == nil {
		t.Fatal("InstallBoundary() conflicting placement error = nil")
	}
}
