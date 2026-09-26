package sqlite

import (
	"strings"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Called inside the same write transaction as DDL and metadata insertion.
// Existing definitions are immutable until DropBoundaryIndex removes them.
func verifyBoundaryIndexDefinition(conn *sqlite.Conn, name, ddl string) error {
	if err := sqlitex.Execute(conn, `SELECT fields, conditions, combinator FROM orisun_boundary_index_metadata WHERE name = ?`, &sqlitex.ExecOptions{
		Args: []any{name}, ResultFunc: func(stmt *sqlite.Stmt) error {
			existing, err := sqliteBoundaryIndexFromRow(name, stmt.ColumnText(0), stmt.ColumnText(1), stmt.ColumnText(2))
			if err != nil {
				return err
			}
			existingDDL, _, err := buildSQLiteBoundaryIndexDDL(name, existing.Fields, existing.Conditions, existing.Combinator)
			if err != nil {
				return err
			}
			if existingDDL != ddl {
				return statuscode.Errorf(statuscode.AlreadyExists, "index %q already exists with a different definition; drop it before replacing it", name)
			}
			return nil
		},
	}); err != nil {
		return err
	}
	return sqlitex.Execute(conn, `SELECT tbl_name, sql FROM sqlite_schema WHERE name = ?`, &sqlitex.ExecOptions{
		Args: []any{name + "_idx"}, ResultFunc: func(stmt *sqlite.Stmt) error {
			// sqlite_schema omits IF NOT EXISTS from the original CREATE statement.
			expected := strings.Replace(ddl, "CREATE INDEX IF NOT EXISTS ", "CREATE INDEX ", 1)
			if stmt.ColumnText(0) != "orisun_es_event" || stmt.ColumnText(1) != expected {
				return statuscode.Errorf(statuscode.AlreadyExists, "index %q already exists with a different physical definition; drop it before replacing it", name)
			}
			return nil
		},
	})
}
