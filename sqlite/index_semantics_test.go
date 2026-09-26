package sqlite

import (
	"path/filepath"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestIndexesPreserveCCCEqualityAcrossLifecycle(t *testing.T) {
	for _, valueType := range []string{"text", "numeric", "boolean", "timestamptz"} {
		t.Run(valueType, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			logger, err := logging.ZapLogger("error")
			require.NoError(t, err)
			var pool *BoundaryPools
			var saver *SqliteSaveEvents
			var getter *SqliteGetEvents
			var admin *SqliteAdminDB
			open := func() {
				var err error
				pool, err = OpenBoundaryPools(ctx, dir, "test", "test")
				require.NoError(t, err)
				pools := map[string]*BoundaryPools{"test": pool}
				saver = NewSqliteSaveEvents(pools, logger)
				getter = NewSqliteGetEvents(pools, logger)
				admin = NewSqliteAdminDB(pools, "test", logger)
			}
			close := func() { saver.close(); require.NoError(t, pool.Close()) }
			open()
			defer func() { close() }()
			values := []any{42, "42", "042", true, "true", false, "false", 0, "oops", nil, "2026-09-25T00:00:00Z"}
			events := make([]eventstore.EventWithMapTags, 0, len(values)+1)
			for _, value := range values {
				events = append(events, mustEvent(t, "Observed", map[string]any{"value": value}, nil))
			}
			events = append(events, mustEvent(t, "Missing", map[string]any{}, nil))
			_, _, err = saver.Save(ctx, events, "test", nil, nil)
			require.NoError(t, err)
			cases := []struct {
				value   string
				indexes []int
			}{
				{"42", []int{0, 1}}, {"042", []int{2}}, {"true", []int{3, 4}},
				{"false", []int{5, 6}}, {"0", []int{7}}, {"oops", []int{8}},
				{"null", nil}, {"2026-09-25T00:00:00Z", []int{10}},
			}
			verify := func(stage string) {
				t.Helper()
				for _, tc := range cases {
					t.Run(stage+"/"+tc.value, func(t *testing.T) {
						query := &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{{Key: "value", Value: tc.value}}}}}
						criteria := []eventstore.ReadCriterion{{Tags: []eventstore.ReadTag{{Key: "value", Value: tc.value}}}}
						rows, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
							Boundary: "test", Direction: eventstore.Direction_ASC, Count: 100,
							Query: &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{{Key: "value", Value: tc.value}}}}},
						})
						require.NoError(t, err)
						require.Len(t, rows, len(tc.indexes))
						for i, index := range tc.indexes {
							require.Equal(t, events[index].EventId, rows[i].EventId)
						}
						latest, err := getter.GetLatestByCriteria(ctx, eventstore.LatestByCriteriaQuery{Boundary: "test", Criteria: criteria})
						require.NoError(t, err)
						require.Len(t, latest.Matches, 1)
						position := eventstore.NotExistsPosition()
						if len(rows) > 0 {
							last := rows[len(rows)-1]
							require.True(t, latest.Matches[0].Found)
							require.Equal(t, last.EventId, latest.Matches[0].Event.EventId)
							position = eventstore.Position{CommitPosition: last.CommitPosition, PreparePosition: last.PreparePosition}
							// A stale observation must not become acceptable when an index hides a match.
							_, _, err = saver.Save(ctx, []eventstore.EventWithMapTags{mustEvent(t, "Rejected", map[string]any{}, nil)}, "test", nil, query)
							require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
						} else {
							require.False(t, latest.Matches[0].Found)
						}
						require.Equal(t, position.CommitPosition, latest.ContextCommitPosition)
						require.Equal(t, position.PreparePosition, latest.ContextPreparePosition)
						// The correct observation must not be rejected by numeric/boolean aliasing.
						_, _, err = saver.Save(ctx, []eventstore.EventWithMapTags{mustEvent(t, "Accepted", map[string]any{}, nil)}, "test", &position, query)
						require.NoError(t, err)
					})
				}
			}
			verify("without_index")
			require.NoError(t, admin.CreateBoundaryIndex(ctx, "test", "a_value", []eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: valueType}}, nil, ""))
			verify("created")
			require.NoError(t, admin.CreateBoundaryIndex(ctx, "test", "z_value", []eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: "numeric"}}, nil, ""))
			verify("overlapping_types")
			close()
			open()
			verify("reopened")
			require.NoError(t, admin.DropBoundaryIndex(ctx, "test", "a_value"))
			require.NoError(t, admin.DropBoundaryIndex(ctx, "test", "z_value"))
			verify("dropped")
		})
	}
}

func TestScalarTextIndexMigration(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "indexes.db"))
	require.NoError(t, applyVersionedMigrations(conn, eventMigrations[:5]))
	require.NoError(t, sqlitex.ExecuteScript(conn, `
 INSERT INTO orisun_boundary_index_metadata(name,fields,conditions,combinator)
 VALUES ('value','[{"JsonKey":"value","ValueType":"text"}]','[{"Key":"value","Operator":"=","Value":"42"}]','AND');
 CREATE INDEX value_idx ON orisun_es_event(json_extract(data, '$."value"'), transaction_id DESC, global_id DESC) WHERE json_extract(data, '$."value"') = '42';
 CREATE INDEX manual_idx ON orisun_es_event(global_id);
 INSERT INTO orisun_es_event(data) VALUES
 ('{"__commitPosition":1,"__preparePosition":1,"__dateCreated":"2026-09-25T00:00:00Z","value":42}'),
 ('{"__commitPosition":2,"__preparePosition":2,"__dateCreated":"2026-09-25T00:00:00Z","value":"42"}');
 `, nil))
	require.NoError(t, applyMigrations(conn))
	verify := func() {
		where, err := buildCriteriaSQL([]map[string]any{{"value": "42"}})
		require.NoError(t, err)
		var count int64
		require.NoError(t, sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM orisun_es_event INDEXED BY value_idx WHERE "+where, &sqlitex.ExecOptions{ResultFunc: func(s *sqlite.Stmt) error { count = s.ColumnInt64(0); return nil }}))
		require.EqualValues(t, 2, count)
		require.NoError(t, sqlitex.ExecuteTransient(conn, "SELECT global_id FROM orisun_es_event INDEXED BY manual_idx", nil))
	}
	verify()
	require.NoError(t, applyMigrations(conn))
	verify()
}

func TestScalarTextIndexMigrationRollsBack(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "rollback.db"))
	require.NoError(t, applyVersionedMigrations(conn, eventMigrations[:5]))
	require.NoError(t, sqlitex.ExecuteScript(conn, `
 INSERT INTO orisun_boundary_index_metadata(name,fields,conditions,combinator) VALUES
 ('a_valid','[{"JsonKey":"value","ValueType":"text"}]','[]','AND'),
 ('z_invalid','[{"JsonKey":"value","ValueType":"numeric"}]','[{"Key":"value","Operator":"=","Value":"invalid"}]','AND');
 CREATE INDEX a_valid_idx ON orisun_es_event(global_id);
 `, nil))
	require.Error(t, applyMigrations(conn))
	require.Equal(t, 5, connSchemaVersion(t, conn))
	var ddl string
	require.NoError(t, sqlitex.ExecuteTransient(conn, "SELECT sql FROM sqlite_schema WHERE name='a_valid_idx'", &sqlitex.ExecOptions{ResultFunc: func(s *sqlite.Stmt) error { ddl = s.ColumnText(0); return nil }}))
	require.Equal(t, "CREATE INDEX a_valid_idx ON orisun_es_event(global_id)", ddl)
}

func TestTypedIndexPreservesDistinctContextsInGroupCommit(t *testing.T) {
	saver, pool, cleanup := newGCTestSaver(t)
	defer cleanup()
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	admin := NewSqliteAdminDB(map[string]*BoundaryPools{"test": pool}, "test", logger)
	require.NoError(t, admin.CreateBoundaryIndex(t.Context(), "test", "value", []eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: "numeric"}}, nil, ""))
	var requests []*sqliteSaveRequest
	for _, value := range []string{"42", "042"} {
		requests = append(requests, &sqliteSaveRequest{
			ctx:         t.Context(),
			inserts:     preparedGCEvents(t, mustEvent(t, "Created", map[string]any{"value": value}, nil)),
			consistency: gcConsistency(eventstore.NotExistsPosition(), gcReadCriterion(eventstore.ReadTag{Key: "value", Value: value})),
			result:      make(chan sqliteSaveResult, 1),
		})
	}
	prepareGCWriteContexts(t, requests)
	saver.runFlush("test", pool, requests)
	for i, req := range requests {
		result := <-req.result
		require.NoError(t, result.err)
		require.EqualValues(t, i+1, result.globalID)
	}
	require.Equal(t, 1, countEventsMatching(t, pool, map[string]any{"value": "42"}))
	require.Equal(t, 1, countEventsMatching(t, pool, map[string]any{"value": "042"}))
}
