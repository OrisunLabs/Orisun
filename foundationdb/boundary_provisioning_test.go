//go:build foundationdb

package foundationdb

import (
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	"slices"
	"testing"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

func TestFoundationDBSystemIndexesCoverBoundaryDefinitionReplay(t *testing.T) {
	want := []eventstore.BoundaryIndexField{{JsonKey: "__eventType", ValueType: "text"}}
	for _, index := range systemAdminIndexes {
		if index.name == "sys_admin_event_type" {
			if !slices.Equal(index.fields, want) {
				t.Fatalf("sys_admin_event_type fields = %#v", index.fields)
			}
			return
		}
	}
	t.Fatal("missing eventType-only system index required by catalog replay")
}

func TestFoundationDBBoundaryProvisioningIsIdempotentAndInstallsSeparately(t *testing.T) {
	backend := newTestBackend(t)
	definition := boundarymodel.Definition{
		Name:      "sales",
		Placement: boundarymodel.Placement{Backend: "foundationdb", Namespace: backend.root},
	}
	if err := backend.ProvisionBoundary(t.Context(), definition); err != nil {
		t.Fatalf("ProvisionBoundary() error = %v", err)
	}
	if err := backend.ProvisionBoundary(t.Context(), definition); err != nil {
		t.Fatalf("idempotent ProvisionBoundary() error = %v", err)
	}
	if err := backend.checkBoundary("sales"); err == nil {
		t.Fatal("physical provisioning must not install the boundary in local runtime state")
	}
	if err := backend.InstallBoundary(t.Context(), definition); err != nil {
		t.Fatalf("InstallBoundary() error = %v", err)
	}
	if err := backend.checkBoundary("sales"); err != nil {
		t.Fatalf("checkBoundary() error = %v", err)
	}
}

func TestFoundationDBBoundaryProvisioningRejectsWrongRoot(t *testing.T) {
	backend := newTestBackend(t)
	err := backend.ProvisionBoundary(t.Context(), boundarymodel.Definition{
		Name:      "sales",
		Placement: boundarymodel.Placement{Backend: "foundationdb", Namespace: "other"},
	})
	if err == nil {
		t.Fatal("ProvisionBoundary() error = nil")
	}
}
