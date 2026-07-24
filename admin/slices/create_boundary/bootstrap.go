package create_boundary

import (
	"context"
	"fmt"
	"strings"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	adminevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
)

// RequireMigratedCatalog prevents a pre-0.8 PostgreSQL installation from
// starting without first importing its legacy boundary mappings in 0.8.0.
func RequireMigratedCatalog(
	ctx context.Context,
	preexistingAdminStore bool,
	adminBoundary string,
	retriever LatestByCriteriaRetriever,
) error {
	if !preexistingAdminStore {
		return nil
	}

	model, err := loadContext(ctx, adminBoundary, adminBoundary, retriever)
	if err != nil {
		return fmt.Errorf("inspect existing boundary catalog: %w", err)
	}
	if model.existing == nil {
		return fmt.Errorf(
			"existing PostgreSQL admin store has no boundary catalog; upgrade this installation through Orisun 0.8.0 with its complete ORISUN_PG_SCHEMAS mapping before starting this version",
		)
	}
	return nil
}

// EnsureBootstrapBoundary creates the admin catalog definition on a fresh
// installation. Existing definitions are accepted only when their immutable
// placement matches.
func EnsureBootstrapBoundary(
	ctx context.Context,
	definition boundarymodel.Definition,
	adminBoundary string,
	appender EventAppender,
	retriever LatestByCriteriaRetriever,
) (bool, error) {
	_, err := CreateBoundaryCommandHandler(
		ctx,
		CreateBoundaryCommand{
			Name:        definition.Name,
			Description: definition.Description,
			Placement:   definition.Placement,
			Metadata: CommandMetadata{
				"source":    "startup_bootstrap",
				"operation": "create_boundary",
			},
		},
		adminBoundary,
		appender,
		retriever,
	)
	if statuscode.CodeOf(err) == statuscode.AlreadyExists {
		if err := verifyBootstrapDefinition(ctx, adminBoundary, definition, retriever); err != nil {
			return false, err
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("bootstrap boundary %q: %w", definition.Name, err)
	}
	return true, nil
}

func verifyBootstrapDefinition(
	ctx context.Context,
	adminBoundary string,
	expected boundarymodel.Definition,
	retriever LatestByCriteriaRetriever,
) error {
	model, err := loadContext(ctx, adminBoundary, expected.Name, retriever)
	if err != nil {
		return fmt.Errorf("load bootstrap boundary %q: %w", expected.Name, err)
	}
	if model.existing == nil {
		return fmt.Errorf("bootstrap boundary %q reported as existing but its definition event was not found", expected.Name)
	}
	actual, err := definitionFromEvent(*model.existing)
	if err != nil {
		return fmt.Errorf("decode bootstrap boundary %q: %w", expected.Name, err)
	}
	if !samePlacement(actual.Placement, expected.Placement) {
		return fmt.Errorf(
			"bootstrap boundary %q placement conflicts with catalog: configured %s/%s, catalog %s/%s",
			expected.Name,
			expected.Placement.Backend,
			expected.Placement.Namespace,
			actual.Placement.Backend,
			actual.Placement.Namespace,
		)
	}
	return nil
}

func definitionFromEvent(event coreeventstore.ReadEvent) (boundarymodel.Definition, error) {
	switch event.EventType {
	case adminevents.EventTypeBoundaryCreated:
		var data adminevents.BoundaryCreated
		if err := json.Unmarshal([]byte(event.Data), &data); err != nil {
			return boundarymodel.Definition{}, err
		}
		return boundarymodel.Definition{
			Name:                 data.Boundary,
			Description:          data.Description,
			Placement:            data.Placement,
			ExistedBeforeCatalog: data.ExistedBeforeCatalog,
		}, nil
	default:
		return boundarymodel.Definition{}, fmt.Errorf("unsupported definition event %q", event.EventType)
	}
}

func samePlacement(left, right boundarymodel.Placement) bool {
	return strings.EqualFold(strings.TrimSpace(left.Backend), strings.TrimSpace(right.Backend)) &&
		strings.TrimSpace(left.Namespace) == strings.TrimSpace(right.Namespace)
}
