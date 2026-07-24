package config

import "testing"

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
