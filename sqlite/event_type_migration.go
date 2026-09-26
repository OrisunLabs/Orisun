package sqlite

import (
	"fmt"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"github.com/goccy/go-json"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Runs in the same transaction as the schema version bump. Failure restores
// documents, observations, and indexes together.
func migrateReservedEventType(conn *sqlite.Conn) error {
	for _, spec := range []struct {
		table, id, column string
		convert           func([]byte) ([]byte, error)
	}{
		{"orisun_es_event", "global_id", "data", eventdata.MigrateLegacyEventType},
		{"orisun_es_write", "write_id", "consistency", eventdata.MigrateLegacyConsistency},
	} {
		var after int64 = -1
		for {
			type row struct {
				id    int64
				value string
			}
			var batch []row
			err := sqlitex.ExecuteTransient(conn, fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s > ? ORDER BY %s LIMIT 256", spec.id, spec.column, spec.table, spec.id, spec.id), &sqlitex.ExecOptions{
				Args: []any{after}, ResultFunc: func(stmt *sqlite.Stmt) error {
					batch = append(batch, row{stmt.ColumnInt64(0), stmt.ColumnText(1)})
					return nil
				},
			})
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				break
			}
			for _, row := range batch {
				value, err := spec.convert([]byte(row.value))
				if err != nil {
					return fmt.Errorf("migrate %s row %d: %w", spec.table, row.id, err)
				}
				if err := sqlitex.ExecuteTransient(conn, fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?", spec.table, spec.column, spec.id), &sqlitex.ExecOptions{Args: []any{string(value), row.id}}); err != nil {
					return err
				}
				after = row.id
			}
		}
	}
	var indexes []sqliteStoredIndexDefinition
	err := sqlitex.Execute(conn, "SELECT name, fields, conditions, combinator FROM orisun_boundary_index_metadata", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		fields, fieldsChanged, err := eventdata.MigrateLegacyIndexKeys([]byte(stmt.ColumnText(1)), "JsonKey")
		if err != nil {
			return err
		}
		conditions, conditionsChanged, err := eventdata.MigrateLegacyIndexKeys([]byte(stmt.ColumnText(2)), "Key")
		if err != nil {
			return err
		}
		if !fieldsChanged && !conditionsChanged {
			return nil
		}
		def := sqliteStoredIndexDefinition{name: stmt.ColumnText(0), combinator: stmt.ColumnText(3)}
		if err := json.Unmarshal(fields, &def.fields); err != nil {
			return err
		}
		if err := json.Unmarshal(conditions, &def.conditions); err != nil {
			return err
		}
		indexes = append(indexes, def)
		return nil
	}})
	if err != nil {
		return err
	}
	for _, def := range indexes {
		fields, err := json.Marshal(def.fields)
		if err != nil {
			return err
		}
		conditions, err := json.Marshal(def.conditions)
		if err != nil {
			return err
		}
		if err := sqlitex.Execute(conn, "UPDATE orisun_boundary_index_metadata SET fields = ?, conditions = ? WHERE name = ?", &sqlitex.ExecOptions{Args: []any{string(fields), string(conditions), def.name}}); err != nil {
			return err
		}
	}
	for _, def := range indexes {
		ddl, _, err := buildSQLiteBoundaryIndexDDL(def.name, def.fields, def.conditions, def.combinator)
		if err != nil {
			return err
		}
		if err := sqlitex.Execute(conn, "DROP INDEX IF EXISTS "+quoteIdent(def.name+"_idx"), nil); err != nil {
			return err
		}
		if err := sqlitex.Execute(conn, ddl, nil); err != nil {
			return err
		}
	}
	// Use the same scalar-text expression as untyped equality queries.
	if err := sqlitex.Execute(conn, "DROP INDEX IF EXISTS idx_event_type_order", nil); err != nil {
		return err
	}
	return sqlitex.Execute(conn, "CREATE INDEX idx_event_type_order ON orisun_es_event ("+sqliteJSONScalarTextExpr("__eventType")+", transaction_id DESC, global_id DESC)", nil)
}
