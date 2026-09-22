package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReservedEventTypeMigration(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.container.Terminate(context.Background()) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
ALTER TABLE test_boundary_orisun_es_event DROP CONSTRAINT test_boundary_event_id_valid;
ALTER TABLE test_boundary_orisun_es_event ADD COLUMN event_id UUID NOT NULL;
UPDATE test_boundary_orisun_schema_version SET version = 0;
INSERT INTO test_boundary_orisun_es_write VALUES (0, '[{"query":{"criteria":[{"eventType":"Created"}]},"position":{"transaction_id":-1,"global_id":-1}}]');
INSERT INTO test_boundary_orisun_es_event (transaction_id,global_id,event_id,data,write_id) VALUES
(1,0,'00000000-0000-0000-0000-000000000001','{"eventType":"Created","number":9223372036854775807,"nested":{"eventType":"domain"}}',0);
CREATE INDEX test_boundary_legacy_idx ON test_boundary_orisun_es_event ((data->>'eventType')) WHERE data->>'eventType' = 'Created';
INSERT INTO test_boundary_orisun_boundary_index_metadata (name,fields,conditions,combinator) VALUES
('legacy','[{"JsonKey":"eventType","ValueType":"text"}]','[{"Key":"eventType","Operator":"=","Value":"Created"}]','AND');
`)
	require.NoError(t, err)
	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()))
	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()))
	text := func(query string) string {
		var value string
		require.NoError(t, db.QueryRow(query).Scan(&value))
		return value
	}
	require.Equal(t, "00000000-0000-0000-0000-000000000001", text("SELECT data->>'__eventId' FROM test_boundary_orisun_es_event"))
	require.Equal(t, "Created", text("SELECT data->>'__eventType' FROM test_boundary_orisun_es_event"))
	require.Equal(t, "9223372036854775807", text("SELECT data->>'number' FROM test_boundary_orisun_es_event"))
	require.Equal(t, "domain", text("SELECT data->'nested'->>'eventType' FROM test_boundary_orisun_es_event"))
	require.Contains(t, text("SELECT consistency FROM test_boundary_orisun_es_write"), "__eventType")
	require.Contains(t, text("SELECT fields FROM test_boundary_orisun_boundary_index_metadata WHERE name = 'legacy'"), "__eventType")
	require.Contains(t, text("SELECT conditions FROM test_boundary_orisun_boundary_index_metadata WHERE name = 'legacy'"), "__eventType")
	require.Contains(t, text("SELECT indexdef FROM pg_indexes WHERE indexname = 'test_boundary_legacy_idx'"), "__eventType")
	require.Contains(t, text("SELECT indexdef FROM pg_indexes WHERE indexname = 'test_boundary_idx_event_type_order'"), "__eventType")
	// After migration, eventType is application data. Reopening must not rename it.
	_, err = db.Exec(`UPDATE test_boundary_orisun_es_event SET data = data || '{"eventType":"domain"}'::jsonb`)
	require.NoError(t, err)
	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()))
	require.Equal(t, "domain", text("SELECT data->>'eventType' FROM test_boundary_orisun_es_event"))
	// A conflicting legacy document must roll back the entire migration.
	_, err = db.Exec(`UPDATE test_boundary_orisun_schema_version SET version = 0`)
	require.NoError(t, err)
	require.ErrorContains(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()), "conflicts")
	require.Equal(t, "0", text("SELECT version FROM test_boundary_orisun_schema_version"))
	require.Equal(t, "domain", text("SELECT data->>'eventType' FROM test_boundary_orisun_es_event"))
}

func TestEventIDMigrationRejectsCollision(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.container.Terminate(context.Background()) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
ALTER TABLE test_boundary_orisun_es_event DROP CONSTRAINT test_boundary_event_id_valid;
ALTER TABLE test_boundary_orisun_es_event ADD COLUMN event_id UUID NOT NULL;
UPDATE test_boundary_orisun_schema_version SET version = 1;
INSERT INTO test_boundary_orisun_es_event(transaction_id,global_id,event_id,data) VALUES
(5,4,'00000000-0000-0000-0000-000000000001','{"__eventId":null,"__eventType":"Created"}');`)
	require.NoError(t, err)
	require.ErrorContains(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()), "conflicts")
	var version int
	require.NoError(t, db.QueryRow("SELECT version FROM test_boundary_orisun_schema_version").Scan(&version))
	require.Equal(t, 1, version)
	var id string
	require.NoError(t, db.QueryRow("SELECT event_id::text FROM test_boundary_orisun_es_event").Scan(&id))
	require.Equal(t, "00000000-0000-0000-0000-000000000001", id)
}
