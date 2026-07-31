package postgres

import (
	"fmt"
	"sync"

	"github.com/OrisunLabs/Orisun/config"
)

type postgresBoundary struct {
	mapping config.BoundaryToPostgresSchemaMapping

	insertEventRequests   string
	insertUnconditional   string
	insertCanonical       string
	insertIndependent     string
	selectEvents          string
	selectLatest          string
	getLastPublished      string
	insertLastPublished   string
	getEventCount         string
	fallbackGetEventCount string
	saveEventCount        string
}

// BoundaryRegistry is the shared, concurrency-safe source for every
// PostgreSQL component that resolves boundary-specific SQL.
type BoundaryRegistry struct {
	mu          sync.RWMutex
	provisionMu sync.Mutex
	entries     map[string]postgresBoundary
}

func NewBoundaryRegistry(mappings map[string]config.BoundaryToPostgresSchemaMapping) *BoundaryRegistry {
	registry := &BoundaryRegistry{entries: make(map[string]postgresBoundary, len(mappings))}
	for boundary, mapping := range mappings {
		mapping.Boundary = boundary
		registry.entries[boundary] = buildPostgresBoundary(mapping)
	}
	return registry
}

func (r *BoundaryRegistry) Register(boundary, schema string) error {
	if err := validateBoundaryName(boundary); err != nil {
		return fmt.Errorf("invalid boundary name %s: %w", boundary, err)
	}
	if err := validateBoundaryName(schema); err != nil {
		return fmt.Errorf("invalid schema name %s: %w", schema, err)
	}
	mapping := config.BoundaryToPostgresSchemaMapping{Boundary: boundary, Schema: schema}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.entries[boundary]; ok {
		if existing.mapping.Schema == schema {
			return nil
		}
		return fmt.Errorf("boundary %s is already registered in schema %s", boundary, existing.mapping.Schema)
	}
	r.entries[boundary] = buildPostgresBoundary(mapping)
	return nil
}

func (r *BoundaryRegistry) lookup(boundary string) (postgresBoundary, bool) {
	if r == nil {
		return postgresBoundary{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.entries[boundary]
	return entry, ok
}

func (r *BoundaryRegistry) Mappings() map[string]config.BoundaryToPostgresSchemaMapping {
	r.mu.RLock()
	defer r.mu.RUnlock()
	mappings := make(map[string]config.BoundaryToPostgresSchemaMapping, len(r.entries))
	for boundary, entry := range r.entries {
		mappings[boundary] = entry.mapping
	}
	return mappings
}

func buildPostgresBoundary(mapping config.BoundaryToPostgresSchemaMapping) postgresBoundary {
	boundary := mapping.Boundary
	schema := mapping.Schema
	return postgresBoundary{
		mapping:               mapping,
		insertEventRequests:   fmt.Sprintf(insertEventRequestsWithConsistency, schema),
		insertUnconditional:   fmt.Sprintf(insertUnconditionalEventRequests, schema),
		insertCanonical:       fmt.Sprintf(insertCanonicalEventRequestsWithConsistency, schema),
		insertIndependent:     fmt.Sprintf(insertIndependentEventRequestsWithConsistency, schema),
		selectEvents:          fmt.Sprintf(selectMatchingEvents, schema),
		selectLatest:          fmt.Sprintf(selectLatestByCriteria, schema),
		getLastPublished:      fmt.Sprintf(getLastPublishedEventQuery, schema, boundary),
		insertLastPublished:   fmt.Sprintf(insertLastPublishedPosition, schema, boundary),
		getEventCount:         fmt.Sprintf("SELECT event_count FROM %s.%s_events_count limit 1", schema, boundary),
		fallbackGetEventCount: fmt.Sprintf("SELECT COUNT(*) FROM %s.%s_orisun_es_event", schema, boundary),
		saveEventCount:        fmt.Sprintf("INSERT INTO %s.%s_events_count (id, event_count, created_at, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET event_count = $2, updated_at = $4", schema, boundary),
	}
}
