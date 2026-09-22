package sqlite

import (
	"fmt"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func migrateEventID(conn *sqlite.Conn) error {
	var collision bool
	if err := sqlitex.Execute(conn, `SELECT EXISTS (SELECT 1 FROM orisun_es_event WHERE json_type(data, '$.__eventId') IS NOT NULL)`, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { collision = stmt.ColumnInt64(0) != 0; return nil }}); err != nil {
		return err
	}
	if collision {
		return fmt.Errorf("legacy event data conflicts with reserved __eventId")
	}
	return sqlitex.ExecuteScript(conn, `
UPDATE orisun_es_event SET data = json_set(data, '$.__eventId', event_id);
ALTER TABLE orisun_es_event DROP COLUMN event_id;
`, nil)
}
