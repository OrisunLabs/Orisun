package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
)

func TestOrderedContextIndexMigration(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.container.Terminate(context.Background())) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	exec := func(query string) { _, err := db.Exec(query); require.NoError(t, err) }
	oid := func(name string) int64 {
		var id int64
		require.NoError(t, db.QueryRow("SELECT to_regclass($1)::oid::bigint", name).Scan(&id))
		return id
	}
	version := func() int {
		var v int
		require.NoError(t, db.QueryRow("SELECT version FROM test_boundary_orisun_schema_version").Scan(&v))
		return v
	}
	exec(`INSERT INTO test_boundary_orisun_boundary_index_metadata(name, fields, conditions, combinator) VALUES
 ('guard', '[{"JsonKey":"context","ValueType":"text"}]', '[{"Key":"__eventType","Operator":"=","Value":"Fact"}]', 'AND');
 CREATE INDEX test_boundary_guard_idx ON test_boundary_orisun_es_event ((data->>'context')) WHERE data->>'__eventType' = 'Fact';
 CREATE INDEX manual_context_idx ON test_boundary_orisun_es_event ((data->>'manual'));
 UPDATE test_boundary_orisun_schema_version SET version = 3;`)
	old, manual := oid("test_boundary_guard_idx"), oid("manual_context_idx")
	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()))
	require.Equal(t, 4, version())
	upgraded := oid("test_boundary_guard_idx")
	require.NotEqual(t, old, upgraded)
	require.Equal(t, manual, oid("manual_context_idx"))
	var ddl string
	require.NoError(t, db.QueryRow("SELECT pg_get_indexdef('test_boundary_guard_idx'::regclass)").Scan(&ddl))
	require.Contains(t, ddl, "transaction_id DESC, global_id DESC")
	require.Contains(t, ddl, "WHERE")
	// Even before ANALYZE, a missing context should use the context index,
	// rather than scanning the global-order index and filtering the history.
	exec(`INSERT INTO test_boundary_orisun_es_event(data)
 SELECT orisun_event_document(jsonb_build_object(
   '__eventId', lpad(i::text, 32, '0')::uuid, '__eventType', 'Fact',
   'context', (i % 128)::text), '{}'::jsonb, i::bigint + 1, i::bigint, NULL, now())
 FROM generate_series(1, 5000) AS events(i)`)
	rows, err := db.Query(`EXPLAIN (COSTS OFF)
 SELECT transaction_id, global_id FROM test_boundary_orisun_es_event
 WHERE data->>'__eventType' = 'Fact' AND data->>'context' = 'missing'
 ORDER BY transaction_id DESC, global_id DESC LIMIT 1`)
	require.NoError(t, err)
	var plan []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan = append(plan, line)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Contains(t, strings.Join(plan, "\n"), "test_boundary_guard_idx")
	require.NotContains(t, strings.Join(plan, "\n"), "Sort")
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	admin := NewPostgresAdminDB(db, logger, "public", "test_boundary", map[string]config.BoundaryToPostgresSchemaMapping{"test_boundary": {Boundary: "test_boundary", Schema: "public"}})
	require.NoError(t, admin.CreateBoundaryIndex(t.Context(), "test_boundary", "guard", []orisun.BoundaryIndexField{{JsonKey: "context", ValueType: "text"}}, []orisun.BoundaryIndexCondition{{Key: "__eventType", Operator: "=", Value: "Fact"}}, "AND"))
	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()))
	require.Equal(t, upgraded, oid("test_boundary_guard_idx"), "migration must not rebuild current indexes")

	// A late ownership conflict must roll back earlier rebuilds and the version.
	exec(`INSERT INTO test_boundary_orisun_boundary_index_metadata(name, fields) VALUES
 ('a_old','[{"JsonKey":"context","ValueType":"text"}]'),
 ('z_conflict','[{"JsonKey":"context","ValueType":"text"}]');
 CREATE INDEX test_boundary_a_old_idx ON test_boundary_orisun_es_event ((data->>'context'));
 CREATE INDEX test_boundary_z_conflict_idx ON test_boundary_orisun_es_event ((data->>'unexpected'));
 UPDATE test_boundary_orisun_schema_version SET version = 3;`)
	old = oid("test_boundary_a_old_idx")
	conflict := oid("test_boundary_z_conflict_idx")
	require.ErrorContains(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()), "physical definition differs from metadata")
	require.Equal(t, 3, version())
	require.Equal(t, old, oid("test_boundary_a_old_idx"))
	require.Equal(t, conflict, oid("test_boundary_z_conflict_idx"))
	require.Equal(t, manual, oid("manual_context_idx"))
}
