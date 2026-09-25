package sqlite

import (
	"fmt"
	"strings"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

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
) error {
	return rebuildBoundaryIndexes(conn, false)
}

// migrateScalarTextIndexes upgrades pre-existing text expressions and removes
// cross-index type dependencies from partial predicates in one migration.
func migrateScalarTextIndexes(conn *sqlite.Conn) error {
	return rebuildBoundaryIndexes(conn, true)
}

func rebuildBoundaryIndexes(conn *sqlite.Conn, all bool) (err error) {
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
		if !all && sqliteIndexOrdersByPosition(definition.ddl) {
			continue
		}
		needsRebuild = append(needsRebuild, definition)
	}
	if len(needsRebuild) == 0 {
		return nil
	}

	endFn := sqlitex.Save(conn)
	defer endFn(&err)
	for _, definition := range needsRebuild {
		ddl, _, buildErr := buildSQLiteBoundaryIndexDDL(
			definition.name,
			definition.fields,
			definition.conditions,
			definition.combinator,
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
