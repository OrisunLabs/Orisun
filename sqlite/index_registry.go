package sqlite

import (
	"fmt"
	"strings"
	"sync"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// sqliteFieldInfo records how an index references a JSON key. declaredField
// distinguishes keys typed via an index field list (queries must emit the raw
// json_extract shape to match the index expression) from keys that only appear
// in partial-index conditions (queries must emit the CASE scalar-text shape to
// match the condition predicate).
type sqliteFieldInfo struct {
	valueType     string
	declaredField bool
}

type sqliteIndexRegistry struct {
	mu               sync.RWMutex
	fieldsByBoundary map[string]map[string]sqliteFieldInfo
}

func newSqliteIndexRegistry() *sqliteIndexRegistry {
	return &sqliteIndexRegistry{
		fieldsByBoundary: make(map[string]map[string]sqliteFieldInfo),
	}
}

// fieldTypeInfo returns the key's value type, whether it is declared as an
// index field, and whether the registry knows the key at all (field or
// condition). Unknown keys default to text.
func (r *sqliteIndexRegistry) fieldTypeInfo(boundary, key string) (valueType string, declaredField, known bool) {
	if r == nil {
		return "text", false, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if fields := r.fieldsByBoundary[boundary]; fields != nil {
		if info, ok := fields[key]; ok {
			return info.valueType, info.declaredField, true
		}
	}
	return "text", false, false
}

func (r *sqliteIndexRegistry) replaceBoundaryFields(boundary string, fields map[string]sqliteFieldInfo) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(fields) == 0 {
		delete(r.fieldsByBoundary, boundary)
		return
	}
	r.fieldsByBoundary[boundary] = fields
}

func normalizeIndexValueType(valueType string) string {
	switch strings.ToLower(strings.TrimSpace(valueType)) {
	case "numeric":
		return "numeric"
	case "boolean":
		return "boolean"
	case "timestamptz":
		return "timestamptz"
	default:
		return "text"
	}
}

func loadBoundaryIndexMetadata(conn *sqlite.Conn, boundary string, registry *sqliteIndexRegistry) error {
	fieldsByKey := make(map[string]sqliteFieldInfo)
	err := sqlitex.Execute(conn,
		"SELECT fields FROM orisun_boundary_index_metadata ORDER BY name",
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				var fields []eventstore.BoundaryIndexField
				if err := json.Unmarshal([]byte(stmt.ColumnText(0)), &fields); err != nil {
					return fmt.Errorf("decode index metadata fields: %w", err)
				}
				for _, field := range fields {
					if field.JsonKey == "" {
						continue
					}
					fieldsByKey[field.JsonKey] = sqliteFieldInfo{
						valueType:     normalizeIndexValueType(field.ValueType),
						declaredField: true,
					}
				}
				return nil
			},
		})
	if err != nil {
		return err
	}
	err = sqlitex.Execute(conn,
		"SELECT conditions FROM orisun_boundary_index_metadata ORDER BY name",
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				var conditions []eventstore.BoundaryIndexCondition
				if err := json.Unmarshal([]byte(stmt.ColumnText(0)), &conditions); err != nil {
					return fmt.Errorf("decode index metadata conditions: %w", err)
				}
				for _, condition := range conditions {
					if condition.Key == "" {
						continue
					}
					if _, ok := fieldsByKey[condition.Key]; !ok {
						fieldsByKey[condition.Key] = sqliteFieldInfo{valueType: "text"}
					}
				}
				return nil
			},
		})
	if err != nil {
		return err
	}
	registry.replaceBoundaryFields(boundary, fieldsByKey)
	return nil
}

type sqliteStoredIndexDefinition struct {
	name       string
	fields     []eventstore.BoundaryIndexField
	conditions []eventstore.BoundaryIndexCondition
	combinator string
	ddl        string
}

// ensureBoundaryIndexesOrderByPosition upgrades indexes created before
// position columns became part of SQLite's physical index shape. Metadata is
// the source of truth; rebuilding happens atomically and is skipped on every
// later startup once the suffix is present.
func ensureBoundaryIndexesOrderByPosition(
	conn *sqlite.Conn,
	boundary string,
	registry *sqliteIndexRegistry,
) (err error) {
	definitions := make([]sqliteStoredIndexDefinition, 0)
	err = sqlitex.Execute(conn,
		`SELECT metadata.name, metadata.fields, metadata.conditions, metadata.combinator,
		        COALESCE(master.sql, '')
		 FROM orisun_boundary_index_metadata AS metadata
		 LEFT JOIN sqlite_master AS master
		   ON master.type = 'index' AND master.name = metadata.name || '_idx'
		 ORDER BY metadata.name`,
		&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			definition := sqliteStoredIndexDefinition{
				name:       stmt.ColumnText(0),
				combinator: stmt.ColumnText(3),
				ddl:        stmt.ColumnText(4),
			}
			if err := json.Unmarshal([]byte(stmt.ColumnText(1)), &definition.fields); err != nil {
				return fmt.Errorf("decode index %q fields: %w", definition.name, err)
			}
			if err := json.Unmarshal([]byte(stmt.ColumnText(2)), &definition.conditions); err != nil {
				return fmt.Errorf("decode index %q conditions: %w", definition.name, err)
			}
			definitions = append(definitions, definition)
			return nil
		}})
	if err != nil {
		return err
	}

	needsRebuild := definitions[:0]
	for _, definition := range definitions {
		if sqliteIndexOrdersByPosition(definition.ddl) {
			continue
		}
		needsRebuild = append(needsRebuild, definition)
	}
	if len(needsRebuild) == 0 {
		return nil
	}

	endFn, err := sqlitex.ImmediateTransaction(conn)
	if err != nil {
		return err
	}
	defer endFn(&err)
	for _, definition := range needsRebuild {
		ddl, _, buildErr := buildSQLiteBoundaryIndexDDLWithRegistry(
			boundary,
			definition.name,
			definition.fields,
			definition.conditions,
			definition.combinator,
			registry,
		)
		if buildErr != nil {
			return fmt.Errorf("rebuild index %q: %w", definition.name, buildErr)
		}
		if err = sqlitex.Execute(conn,
			"DROP INDEX IF EXISTS "+quoteIdent(definition.name+"_idx"), nil); err != nil {
			return fmt.Errorf("drop old index %q: %w", definition.name, err)
		}
		if err = sqlitex.Execute(conn, ddl, nil); err != nil {
			return fmt.Errorf("create position-ordered index %q: %w", definition.name, err)
		}
	}
	return nil
}

func sqliteIndexOrdersByPosition(ddl string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(ddl), " "))
	return strings.Contains(normalized, "transaction_id desc, global_id desc")
}
