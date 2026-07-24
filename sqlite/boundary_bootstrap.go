package sqlite

import (
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	"github.com/OrisunLabs/Orisun/config"
)

func AdminBoundaryDefinition(adminConfig config.AdminConfig) boundarymodel.Definition {
	return boundarymodel.Definition{
		Name: adminConfig.Boundary,
		Placement: boundarymodel.Placement{
			Backend:   "sqlite",
			Namespace: adminConfig.Boundary,
		},
	}
}
