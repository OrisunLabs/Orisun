package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/storagecontract"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestBackendOwnsEventEnvelope(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)
	storagecontract.Envelope(t, saver, getter, NewSqliteAdminDB(pools, "test", logger), "test", func(fields string) {
		conn, err := pools["test"].Write.Take(t.Context())
		require.NoError(t, err)
		defer pools["test"].Write.Put(conn)
		require.NoError(t, sqlitex.Execute(conn, "UPDATE orisun_es_event SET data = json_set(data, '$.__future', json('null'), '$.__', json_extract(?, '$.__'), '$.___internal', json_extract(?, '$.___internal'))", &sqlitex.ExecOptions{Args: []any{fields, fields}}))
	})
	conn, err := pools["test"].Read.Take(t.Context())
	require.NoError(t, err)
	defer pools["test"].Read.Put(conn)
	require.NoError(t, sqlitex.Execute(conn, "SELECT data FROM orisun_es_event", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		require.Contains(t, stmt.ColumnText(0), `"__eventId":`)
		require.Contains(t, stmt.ColumnText(0), `"__eventType":"EnvelopeTest"`)
		return nil
	}}))
}

func TestEventIDMigrationPreservesHistory(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "id.db"))
	require.NoError(t, applyVersionedMigrations(conn, eventMigrations[:3]))
	require.NoError(t, sqlitex.Execute(conn, `INSERT INTO orisun_es_event(transaction_id,global_id,event_id,data) VALUES (5,4,'legacy','{"__eventType":"Created","eventId":"application","number":9223372036854775807}')`, nil))
	require.NoError(t, applyMigrations(conn))
	require.NoError(t, applyMigrations(conn))
	require.NoError(t, sqlitex.Execute(conn, "SELECT transaction_id, global_id, data FROM orisun_es_event", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		require.EqualValues(t, 5, stmt.ColumnInt64(0))
		require.EqualValues(t, 4, stmt.ColumnInt64(1))
		require.Contains(t, stmt.ColumnText(2), `"__eventId":"legacy"`)
		require.Contains(t, stmt.ColumnText(2), `"eventId":"application"`)
		require.Contains(t, stmt.ColumnText(2), `9223372036854775807`)
		return nil
	}}))
	require.NoError(t, sqlitex.Execute(conn, "PRAGMA table_info(orisun_es_event)", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		require.NotEqual(t, "event_id", stmt.ColumnText(1))
		return nil
	}}))
}

func TestEventIDMigrationRejectsCollision(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "id-conflict.db"))
	require.NoError(t, applyVersionedMigrations(conn, eventMigrations[:3]))
	require.NoError(t, sqlitex.Execute(conn, `INSERT INTO orisun_es_event(transaction_id,global_id,event_id,data) VALUES (5,4,'legacy','{"__eventId":null}')`, nil))
	require.ErrorContains(t, applyMigrations(conn), "conflicts")
	require.Equal(t, 3, connSchemaVersion(t, conn))
}
