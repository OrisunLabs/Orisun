package sqlite

import (
	"testing"

	"github.com/OrisunLabs/Orisun/config"
)

func TestAdminBoundaryDefinitionUsesAdminBoundaryAsNamespace(t *testing.T) {
	definition := AdminBoundaryDefinition(config.AdminConfig{Boundary: "control"})
	if definition.Name != "control" ||
		definition.Placement.Backend != "sqlite" ||
		definition.Placement.Namespace != "control" {
		t.Fatalf("definition = %#v", definition)
	}
}
