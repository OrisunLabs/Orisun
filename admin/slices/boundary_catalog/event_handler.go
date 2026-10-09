// Package boundary_catalog projects boundary lifecycle events into a catalog.
package boundary_catalog

import (
	"fmt"
	"sort"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/goccy/go-json"
)

// Catalog folds boundary lifecycle events into a read model. It deliberately
// ignores other admin events so it can consume the complete admin log.
type Catalog struct {
	boundaries map[string]boundarymodel.Boundary
}

func NewCatalog() *Catalog {
	return &Catalog{boundaries: make(map[string]boundarymodel.Boundary)}
}

// Apply applies one event and reports whether it was a boundary lifecycle
// event. An outcome before its definition is ignored. This is fail-closed—the
// outcome cannot expose a boundary—and permits a new BoundaryCreated event to
// supersede lifecycle outcomes from a removed pre-catalog definition path.
func (c *Catalog) Apply(event coreeventstore.ReadEvent) (bool, error) {
	if c.boundaries == nil {
		c.boundaries = make(map[string]boundarymodel.Boundary)
	}

	switch event.EventType {
	case boundaryevents.EventTypeBoundaryCreated:
		var data boundaryevents.BoundaryCreated
		if err := decode(event, &data); err != nil {
			return true, err
		}
		return true, c.define(
			data.Boundary,
			data.Description,
			data.Placement,
			&event.Position,
		)

	case boundaryevents.EventTypeBoundaryActivated:
		var data boundaryevents.BoundaryActivated
		if err := decode(event, &data); err != nil {
			return true, err
		}
		if data.Boundary == "" {
			return true, fmt.Errorf("boundary catalog: %s has no boundary", event.EventType)
		}
		boundary, defined := c.boundaries[data.Boundary]
		if !defined {
			return true, nil
		}
		boundary.Status = boundarymodel.StatusActive
		boundary.LastError = ""
		boundary.StatusPosition = clonePosition(&event.Position)
		c.boundaries[data.Boundary] = boundary
		return true, nil

	case boundaryevents.EventTypeBoundaryFailed:
		var data boundaryevents.BoundaryProvisioningFailed
		if err := decode(event, &data); err != nil {
			return true, err
		}
		if data.Boundary == "" {
			return true, fmt.Errorf("boundary catalog: %s has no boundary", event.EventType)
		}
		boundary, defined := c.boundaries[data.Boundary]
		if !defined {
			return true, nil
		}
		if data.Error == "" {
			return true, fmt.Errorf("boundary catalog: %s has empty error", event.EventType)
		}
		// ACTIVE is terminal. A later node-local provisioning failure must not
		// downgrade a boundary that another node successfully activated.
		if boundary.Status == boundarymodel.StatusActive {
			return true, nil
		}
		boundary.Status = boundarymodel.StatusFailed
		boundary.LastError = data.Error
		boundary.StatusPosition = clonePosition(&event.Position)
		c.boundaries[data.Boundary] = boundary
		return true, nil

	default:
		return false, nil
	}
}

func (c *Catalog) Get(name string) (boundarymodel.Boundary, bool) {
	boundary, ok := c.boundaries[name]
	if !ok {
		return boundarymodel.Boundary{}, false
	}
	return cloneBoundary(boundary), true
}

// List returns a stable name-ordered snapshot suitable for API responses and
// deterministic reconciliation.
func (c *Catalog) List() []boundarymodel.Boundary {
	boundaries := make([]boundarymodel.Boundary, 0, len(c.boundaries))
	for _, boundary := range c.boundaries {
		boundaries = append(boundaries, cloneBoundary(boundary))
	}
	sort.Slice(boundaries, func(i, j int) bool {
		return boundaries[i].Name < boundaries[j].Name
	})
	return boundaries
}

func (c *Catalog) define(
	name string,
	description string,
	placement boundarymodel.Placement,
	position *coreeventstore.Position,
) error {
	if name == "" {
		return fmt.Errorf("boundary catalog: boundary name is required")
	}
	if placement.Backend == "" {
		return fmt.Errorf("boundary catalog: boundary %q has no placement backend", name)
	}
	if placement.Namespace == "" {
		return fmt.Errorf("boundary catalog: boundary %q has no placement namespace", name)
	}
	if _, exists := c.boundaries[name]; exists {
		return fmt.Errorf("boundary catalog: boundary %q is already defined", name)
	}
	c.boundaries[name] = boundarymodel.Boundary{
		Name:        name,
		Description: description,
		Placement:   placement,
		Status:      boundarymodel.StatusProvisioning,

		DefinitionPosition: clonePosition(position),
		StatusPosition:     clonePosition(position),
	}
	return nil
}

func decode(event coreeventstore.ReadEvent, target any) error {
	if err := json.Unmarshal([]byte(event.Data), target); err != nil {
		return fmt.Errorf("boundary catalog: decode %s: %w", event.EventType, err)
	}
	return nil
}

func cloneBoundary(boundary boundarymodel.Boundary) boundarymodel.Boundary {
	boundary.DefinitionPosition = clonePosition(boundary.DefinitionPosition)
	boundary.StatusPosition = clonePosition(boundary.StatusPosition)
	return boundary
}

func clonePosition(position *coreeventstore.Position) *coreeventstore.Position {
	if position == nil {
		return nil
	}
	return &coreeventstore.Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition,
	}
}
