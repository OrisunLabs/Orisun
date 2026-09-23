package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestEventEnvelopeMigrationPreservesHistoryAndIndexes(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "envelope.db"))
	require.NoError(t, applyVersionedMigrations(conn, eventMigrations[:4]))
	require.NoError(t, sqlitex.ExecuteScript(conn, `
 INSERT INTO orisun_es_write VALUES(9007199254740993,'[]');
 INSERT INTO orisun_es_event(transaction_id,global_id,write_id,data,metadata,date_created) VALUES
 (9007199254740993,9007199254740993,9007199254740993,'{"__eventId":"old","__eventType":"Created","number":9223372036854775807}','{"__trace":9223372036854775807}','2026-01-02T03:04:05.123456789Z');
 CREATE INDEX custom_envelope_index ON orisun_es_event((json_extract(data,'$.number')),global_id) WHERE transaction_id > 0;
 `, nil))
	require.NoError(t, applyMigrations(conn))
	require.NoError(t, applyMigrations(conn))
	require.NoError(t, sqlitex.Execute(conn, "SELECT data, metadata, date_created, transaction_id, global_id, write_id FROM orisun_es_event", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		require.Contains(t, stmt.ColumnText(0), `"__commitPosition":9007199254740993`)
		require.Contains(t, stmt.ColumnText(0), `"__writeId":"9007199254740993:9007199254740993"`)
		require.Contains(t, stmt.ColumnText(0), `9223372036854775807`)
		require.Equal(t, `{"__trace":9223372036854775807}`, stmt.ColumnText(1))
		require.Equal(t, "2026-01-02T03:04:05.123456789Z", stmt.ColumnText(2))
		for i := 3; i <= 5; i++ {
			require.EqualValues(t, 9007199254740993, stmt.ColumnInt64(i))
		}
		return nil
	}}))
	generated := 0
	require.NoError(t, sqlitex.Execute(conn, "PRAGMA table_xinfo(orisun_es_event)", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		if stmt.ColumnInt(6) == 2 {
			generated++
		}
		return nil
	}}))
	require.Equal(t, 5, generated)
	require.NoError(t, sqlitex.Execute(conn, "SELECT COUNT(*) FROM sqlite_schema WHERE name='custom_envelope_index'", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { require.Equal(t, 1, stmt.ColumnInt(0)); return nil }}))
	require.Error(t, sqlitex.Execute(conn, "UPDATE orisun_es_event SET global_id=1", nil))
}

func TestEventEnvelopeMigrationRejectsCollisionAtomically(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "collision.db"))
	require.NoError(t, applyVersionedMigrations(conn, eventMigrations[:4]))
	require.NoError(t, sqlitex.Execute(conn, `INSERT INTO orisun_es_event(transaction_id,global_id,data) VALUES(1,1,'{"__metadata":null}')`, nil))
	require.ErrorContains(t, applyMigrations(conn), "conflicts")
	require.Equal(t, 4, connSchemaVersion(t, conn))
	require.NoError(t, sqlitex.Execute(conn, "UPDATE orisun_es_event SET data='{}'", nil))
	require.NoError(t, applyMigrations(conn))
}
