package postgres

import (
	"testing"

	"github.com/OrisunLabs/Orisun/config"
)

func TestAdminBoundaryDefinitionUsesConfiguredSchema(t *testing.T) {
	definition := AdminBoundaryDefinition(
		config.PostgresDBConfig{AdminSchema: "admin_data"},
		config.AdminConfig{Boundary: "control"},
	)
	if definition.Name != "control" ||
		definition.Placement.Backend != "postgres" ||
		definition.Placement.Namespace != "admin_data" {
		t.Fatalf("definition = %#v", definition)
	}
}
