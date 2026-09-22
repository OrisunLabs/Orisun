package sqlite

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/storagecontract"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestWriteContextContract(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)
	storagecontract.WriteContext(t, saver, getter, getter, "test")
	conn, err := pools["test"].Read.Take(t.Context())
	require.NoError(t, err)
	defer pools["test"].Read.Put(conn)
	require.NoError(t, sqlitex.Execute(conn, "SELECT COUNT(*) FROM orisun_es_write", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		require.EqualValues(t, 3, stmt.ColumnInt64(0), "rejected save must leave no write record")
		return nil
	}}))
}

func TestWriteContextMigrationPreservesUnknownHistory(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "legacy.db"))
	require.NoError(t, sqlitex.ExecuteScript(conn, eventDDL, nil))
	require.NoError(t, sqlitex.Execute(conn, `INSERT INTO orisun_es_event(transaction_id, global_id, event_id, data) VALUES(1, 1, 'legacy', '{}')`, nil))
	require.NoError(t, applyMigrations(conn))
	require.NoError(t, applyMigrations(conn))
	require.NoError(t, sqlitex.Execute(conn, "SELECT write_id, (SELECT COUNT(*) FROM orisun_es_write) FROM orisun_es_event", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		require.Equal(t, sqlite.TypeNull, stmt.ColumnType(0))
		require.Zero(t, stmt.ColumnInt64(1))
		return nil
	}}))
}

func TestWriteContextRPCAndRestart(t *testing.T) {
	dir := t.TempDir()
	pool, err := OpenBoundaryPools(t.Context(), dir, "test", "test")
	require.NoError(t, err)
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(map[string]*BoundaryPools{"test": pool}, logger)
	getter := NewSqliteGetEvents(map[string]*BoundaryPools{"test": pool}, logger)
	api := grpcapi.AdaptEventStore(orisun.NewEventStoreServer(nil, saver, getter, nil, nil, orisun.EventStreamConfig{}, logger))
	observation := &grpcapi.ConsistencyObservation{
		Query:    &grpcapi.Query{Criteria: []*grpcapi.Criterion{{Tags: []*grpcapi.Tag{{Key: "key", Value: "value"}}}}},
		Position: &grpcapi.Position{CommitPosition: -1, PreparePosition: -1},
	}
	saved, err := api.SaveEventsV2(t.Context(), &grpcapi.SaveEventsV2Request{Boundary: "test", Consistency: []*grpcapi.ConsistencyObservation{observation}, Events: []*grpcapi.EventToSave{{EventId: uuid.NewString(), EventType: "Created", Data: `{"key":"value"}`}}})
	require.NoError(t, err)
	require.NotEmpty(t, saved.WriteId)
	recorded, err := api.GetWriteContext(t.Context(), &grpcapi.GetWriteContextRequest{Boundary: "test", WriteId: saved.WriteId})
	require.NoError(t, err)
	require.True(t, proto.Equal(observation, recorded.Consistency[0]))
	rows, err := api.GetEvents(t.Context(), &grpcapi.GetEventsRequest{Boundary: "test", Count: 10})
	require.NoError(t, err)
	require.Equal(t, saved.WriteId, rows.Events[0].WriteId)
	saver.close()
	require.NoError(t, pool.Close())
	reopened, err := OpenBoundaryPools(t.Context(), dir, "test", "test")
	require.NoError(t, err)
	defer reopened.Close()
	reader := NewSqliteGetEvents(map[string]*BoundaryPools{"test": reopened}, logger)
	after, err := reader.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: "test", WriteId: saved.WriteId})
	require.NoError(t, err)
	require.Equal(t, "value", after.Consistency[0].Query.Criteria[0].Tags[0].Value)
}

func TestWriteContextGroupCommitPaths(t *testing.T) {
	for _, path := range []sqliteFlushPath{sqliteFlushUnconditional, sqliteFlushIndependentCCC, sqliteFlushIsolated} {
		t.Run(path.String(), func(t *testing.T) {
			saver, pool, cleanup := newGCTestSaver(t)
			defer cleanup()
			requests := make([]*sqliteSaveRequest, 2*sqliteMaxWriteContextsPerInsert+3)
			for i := range requests {
				value := fmt.Sprint(i)
				events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
					{EventId: uuid.NewString(), EventType: "Created", Data: map[string]any{"context": value}},
					{EventId: uuid.NewString(), EventType: "Created", Data: map[string]any{"context": value}},
				})
				require.NoError(t, err)
				req := &sqliteSaveRequest{ctx: t.Context(), inserts: events, result: make(chan sqliteSaveResult, 1)}
				if path != sqliteFlushUnconditional {
					req.consistency = []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "context", Value: value}}}}, Position: orisun.NotExistsPosition()}}
					if i == 1 {
						req.consistency[0].Position = orisun.Position{CommitPosition: 123, PreparePosition: 123}
					}
				}
				data, err := orisun.MarshalConsistency(req.consistency)
				require.NoError(t, err)
				req.consistencyJSON = string(data)
				requests[i] = req
			}
			var predicates []string
			if path == sqliteFlushIndependentCCC {
				var ok bool
				predicates, ok = independentCCCContexts(requests, pool, "test")
				require.True(t, ok)
			}
			conn, err := pool.Write.Take(t.Context())
			require.NoError(t, err)
			accepted, err := saver.flushTx(conn, pool, "test", requests, path, predicates)
			pool.Write.Put(conn)
			require.NoError(t, err)
			want := len(requests) - 1
			if path == sqliteFlushUnconditional {
				want = len(requests)
			}
			require.Len(t, accepted, want)
			getter := NewSqliteGetEvents(map[string]*BoundaryPools{"test": pool}, saver.logger)
			ids := map[string]bool{}
			for _, saved := range accepted {
				commit, err := strconv.ParseInt(saved.transactionID, 10, 64)
				require.NoError(t, err)
				id := orisun.WriteID(commit, saved.globalID)
				require.False(t, ids[id], "separate saves in one flush must have separate write records")
				ids[id] = true
				ctx, err := getter.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: "test", WriteId: id})
				require.NoError(t, err)
				encoded, err := orisun.MarshalConsistency(saved.req.consistency)
				require.NoError(t, err)
				expected, err := orisun.DecodeWriteContext(id, encoded)
				require.NoError(t, err)
				require.Equal(t, expected, ctx)
			}
			rows, err := getter.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: "test", Count: uint32(want * 2)})
			require.NoError(t, err)
			require.Len(t, rows, want*2)
			for _, row := range rows {
				require.True(t, ids[row.WriteId])
			}
			conn, err = pool.Read.Take(t.Context())
			require.NoError(t, err)
			defer pool.Read.Put(conn)
			require.NoError(t, sqlitex.Execute(conn, "SELECT COUNT(*) FROM orisun_es_write", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
				require.EqualValues(t, want, stmt.ColumnInt64(0))
				return nil
			}}))
		})
	}
}

func TestWriteContextRollsBackOnEventInsertFailure(t *testing.T) {
	saver, pool, cleanup := newGCTestSaver(t)
	defer cleanup()
	requests := []*sqliteSaveRequest{
		{ctx: t.Context(), consistencyJSON: "[]", inserts: orisun.PreparedEventBatch{{EventId: "bad", EventType: "Bad", DataJSON: "invalid json", MetadataJSON: "{}"}}, result: make(chan sqliteSaveResult, 1)},
		{ctx: t.Context(), consistencyJSON: "[]", inserts: orisun.PreparedEventBatch{{EventId: "good", EventType: "Good", DataJSON: `{"__eventType":"Good"}`, MetadataJSON: "{}"}}, result: make(chan sqliteSaveResult, 1)},
	}
	conn, err := pool.Write.Take(t.Context())
	require.NoError(t, err)
	accepted, err := saver.flushTx(conn, pool, "test", requests, sqliteFlushIsolated, nil)
	pool.Write.Put(conn)
	require.NoError(t, err)
	require.Len(t, accepted, 1)
	require.Error(t, (<-requests[0].result).err)
	conn, err = pool.Read.Take(t.Context())
	require.NoError(t, err)
	defer pool.Read.Put(conn)
	require.NoError(t, sqlitex.Execute(conn, "SELECT COUNT(*) FROM orisun_es_write", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		require.EqualValues(t, 1, stmt.ColumnInt64(0))
		return nil
	}}))
}

func TestWriteContextBatchRollbackAcrossChunks(t *testing.T) {
	for _, failAt := range []string{"context", "event"} {
		t.Run(failAt, func(t *testing.T) {
			saver, pool, cleanup := newGCTestSaver(t)
			defer cleanup()
			requests := make([]*sqliteSaveRequest, sqliteMaxWriteContextsPerInsert+1)
			for i := range requests {
				requests[i] = &sqliteSaveRequest{
					ctx:             t.Context(),
					inserts:         orisun.PreparedEventBatch{{EventId: fmt.Sprint(i), DataJSON: "{}", MetadataJSON: "{}"}},
					consistencyJSON: "[]",
					result:          make(chan sqliteSaveResult, 1),
				}
			}
			conn, err := pool.Write.Take(t.Context())
			require.NoError(t, err)
			defer pool.Write.Put(conn)
			table, column := "orisun_es_write", "write_id"
			if failAt == "event" {
				table, column = "orisun_es_event", "global_id"
			}
			// Abort after at least one complete context chunk was inserted.
			require.NoError(t, sqlitex.Execute(conn, fmt.Sprintf(
				"CREATE TRIGGER reject_last BEFORE INSERT ON %s WHEN NEW.%s = %d BEGIN SELECT RAISE(ABORT, 'injected failure'); END",
				table, column, len(requests)), nil))
			_, err = saver.flushTx(conn, pool, "test", requests, sqliteFlushUnconditional, nil)
			require.ErrorContains(t, err, "injected failure")
			require.NoError(t, sqlitex.Execute(conn,
				"SELECT (SELECT COUNT(*) FROM orisun_es_write), (SELECT COUNT(*) FROM orisun_es_event), next_id FROM orisun_es_seq",
				&sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
					require.Zero(t, stmt.ColumnInt64(0))
					require.Zero(t, stmt.ColumnInt64(1))
					require.EqualValues(t, 1, stmt.ColumnInt64(2))
					return nil
				}}))
			require.NoError(t, sqlitex.Execute(conn, "DROP TRIGGER reject_last", nil))
			accepted, err := saver.flushTx(conn, pool, "test", requests, sqliteFlushUnconditional, nil)
			require.NoError(t, err)
			require.Len(t, accepted, len(requests))
		})
	}
}
