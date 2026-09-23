package sqlite

import (
	"fmt"
	"time"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Generated projections preserve ordered reads and FK constraints without a
// second writable copy of envelope fields. SQLite cannot use a generated column
// as a primary key, so the position is enforced by UNIQUE NOT NULL instead.
const eventDocumentDDL = `CREATE TABLE orisun_es_event_document (
 data TEXT NOT NULL CHECK (json_valid(data) AND json_type(data) = 'object'),
 transaction_id INTEGER GENERATED ALWAYS AS (json_extract(data, '$.__commitPosition')) VIRTUAL NOT NULL,
 global_id INTEGER GENERATED ALWAYS AS (json_extract(data, '$.__preparePosition')) VIRTUAL NOT NULL UNIQUE,
 write_id INTEGER GENERATED ALWAYS AS (CAST(substr(json_extract(data, '$.__writeId'), instr(json_extract(data, '$.__writeId'), ':') + 1) AS INTEGER)) VIRTUAL REFERENCES orisun_es_write(write_id),
 metadata TEXT GENERATED ALWAYS AS (data -> '__metadata') VIRTUAL,
 date_created TEXT GENERATED ALWAYS AS (json_extract(data, '$.__dateCreated')) VIRTUAL NOT NULL
);`

func migrateEventEnvelope(conn *sqlite.Conn) error {
	var schemaObjects []string
	if err := sqlitex.ExecuteTransient(conn, "SELECT sql FROM sqlite_schema WHERE type IN ('index', 'trigger') AND tbl_name = 'orisun_es_event' AND sql IS NOT NULL", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { schemaObjects = append(schemaObjects, stmt.ColumnText(0)); return nil }}); err != nil {
		return err
	}
	if err := sqlitex.ExecuteScript(conn, eventDocumentDDL, nil); err != nil {
		return err
	}
	// Keyset batches bound memory while the outer migration transaction keeps the
	// table swap, rebuilt indexes, and version change atomic.
	var after int64
	first := true
	for {
		type row struct {
			id   int64
			data string
		}
		var rows []row
		err := sqlitex.ExecuteTransient(conn, "SELECT transaction_id, global_id, data, metadata, date_created, write_id FROM orisun_es_event WHERE ? OR global_id > ? ORDER BY global_id LIMIT 256", &sqlitex.ExecOptions{Args: []any{first, after}, ResultFunc: func(stmt *sqlite.Stmt) error {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(stmt.ColumnText(2)), &fields); err != nil {
				return err
			}
			for _, key := range []string{"__commitPosition", "__preparePosition", "__writeId", "__metadata", "__dateCreated"} {
				if _, ok := fields[key]; ok {
					return fmt.Errorf("legacy event data conflicts with reserved %s", key)
				}
			}
			created, err := parseSQLiteEventTime(stmt.ColumnText(4))
			if err != nil {
				return err
			}
			var write any
			if stmt.ColumnType(5) != sqlite.TypeNull {
				write = orisun.WriteID(stmt.ColumnInt64(0), stmt.ColumnInt64(5))
			}
			data, err := eventdata.WithFields(stmt.ColumnText(2), map[string]any{
				"__commitPosition": stmt.ColumnInt64(0), "__preparePosition": stmt.ColumnInt64(1), "__writeId": write,
				"__metadata": eventdata.MetadataValue(stmt.ColumnText(3)), "__dateCreated": created.UTC().Format(time.RFC3339Nano),
			})
			if err != nil {
				return err
			}
			rows = append(rows, row{stmt.ColumnInt64(1), data})
			return nil
		}})
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if err := sqlitex.ExecuteTransient(conn, "INSERT INTO orisun_es_event_document(data) VALUES (?)", &sqlitex.ExecOptions{Args: []any{row.data}}); err != nil {
				return err
			}
			after = row.id
		}
		first = false
	}
	if err := sqlitex.ExecuteScript(conn, "DROP TABLE orisun_es_event; ALTER TABLE orisun_es_event_document RENAME TO orisun_es_event;", nil); err != nil {
		return err
	}
	for _, ddl := range schemaObjects {
		if err := sqlitex.ExecuteTransient(conn, ddl, nil); err != nil {
			return err
		}
	}
	return nil
}
