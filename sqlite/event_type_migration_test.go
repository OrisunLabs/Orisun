package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestReservedEventTypeMigration(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "legacy.db"))
	require.NoError(t, applyVersionedMigrations(conn, eventMigrations[:2]))
	require.NoError(t, sqlitex.ExecuteScript(conn, `
INSERT INTO orisun_es_write VALUES (0, '[{"query":{"criteria":[{"eventType":"Created"}]},"position":{"transaction_id":-1,"global_id":-1}}]');
INSERT INTO orisun_es_event (transaction_id,global_id,event_id,data,write_id) VALUES
(1,0,'id','{"eventType":"Created","number":9223372036854775807,"nested":{"eventType":"domain"}}',0);
CREATE INDEX legacy_idx ON orisun_es_event (json_extract(data, '$."eventType"'), transaction_id DESC, global_id DESC);
INSERT INTO orisun_boundary_index_metadata (name,fields,conditions,combinator) VALUES
('legacy','[{"JsonKey":"eventType","ValueType":"text"}]','[{"Key":"eventType","Operator":"=","Value":"Created"}]','AND');
`, nil))
	require.NoError(t, applyMigrations(conn))
	require.NoError(t, applyMigrations(conn))
	text := func(query string) string {
		var value string
		require.NoError(t, sqlitex.ExecuteTransient(conn, query, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { value = stmt.ColumnText(0); return nil }}))
		return value
	}
	data := text("SELECT data FROM orisun_es_event")
	require.Contains(t, data, `"__eventType":"Created"`)
	require.Contains(t, data, `"number":9223372036854775807`)
	require.Contains(t, data, `"nested":{"eventType":"domain"}`)
	require.Contains(t, text("SELECT consistency FROM orisun_es_write"), `"__eventType":"Created"`)
	require.Contains(t, text("SELECT fields FROM orisun_boundary_index_metadata"), `"JsonKey":"__eventType"`)
	require.Contains(t, text("SELECT conditions FROM orisun_boundary_index_metadata"), `"Key":"__eventType"`)
	require.Contains(t, text("SELECT sql FROM sqlite_master WHERE name = 'legacy_idx'"), "__eventType")
	require.Contains(t, text("SELECT sql FROM sqlite_master WHERE name = 'idx_event_type_order'"), "__eventType")
	require.Equal(t, "id", text(`SELECT json_extract(data, '$.__eventId') FROM orisun_es_event WHERE json_extract(data, '$."__eventType"') = 'Created'`))
	require.Equal(t, len(eventMigrations), connSchemaVersion(t, conn))
}

func TestReservedEventTypeMigrationCollisionRollsBack(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "collision.db"))
	require.NoError(t, applyVersionedMigrations(conn, eventMigrations[:2]))
	require.NoError(t, sqlitex.ExecuteScript(conn, `
INSERT INTO orisun_es_event (transaction_id,global_id,event_id,data) VALUES
(1,0,'first','{"eventType":"Created"}'),
(1,1,'collision','{"eventType":"Created","__eventType":"application value"}');
`, nil))
	require.ErrorContains(t, applyMigrations(conn), "conflicts")
	require.Equal(t, 2, connSchemaVersion(t, conn))
	require.NoError(t, sqlitex.Execute(conn, "SELECT data FROM orisun_es_event WHERE global_id = 0", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		require.Equal(t, `{"eventType":"Created"}`, stmt.ColumnText(0))
		return nil
	}}))
}
