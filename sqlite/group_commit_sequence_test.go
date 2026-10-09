package sqlite

import (
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func sequenceRequest(t *testing.T, types ...string) *sqliteSaveRequest {
	t.Helper()
	events := make([]orisun.EventWithMapTags, len(types))
	for i, kind := range types {
		events[i] = mustEvent(t, kind, map[string]any{"context": kind}, nil)
	}
	return &sqliteSaveRequest{ctx: t.Context(), inserts: preparedGCEvents(t, events...), consistencyJSON: "[]", result: make(chan sqliteSaveResult, 1)}
}

func TestGroupCommitSequenceTracksOnlyAcceptedRequests(t *testing.T) {
	dir := t.TempDir()
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	pool, err := OpenBoundaryPools(t.Context(), dir, gcBoundary)
	require.NoError(t, err)
	saver := NewSqliteSaveEvents(map[string]*BoundaryPools{gcBoundary: pool}, logger)
	defer func() { saver.close(); require.NoError(t, pool.Close()) }()
	conn, err := pool.Write.Take(t.Context())
	require.NoError(t, err)
	require.NoError(t, sqlitex.Execute(conn, `CREATE TRIGGER reject_event BEFORE INSERT ON orisun_es_event WHEN json_extract(NEW.data,'$.__eventType') = 'Reject' BEGIN SELECT RAISE(ABORT,'rejected event'); END`, nil))
	pool.Write.Put(conn)
	first := sequenceRequest(t, "Observed", "Other")
	rejected := sequenceRequest(t, "RolledBack", "Reject")
	last := sequenceRequest(t, "Tail")
	last.consistency = []orisun.ConsistencyCheck{
		{Position: orisun.Position{CommitPosition: 2, PreparePosition: 1}, Criteria: []orisun.ReadCriterion{gcReadCriterion(orisun.ReadTag{Key: "context", Value: "Observed"})}},
		{Position: orisun.NotExistsPosition(), Criteria: []orisun.ReadCriterion{gcReadCriterion(orisun.ReadTag{Key: "context", Value: "RolledBack"})}},
	}
	requests := []*sqliteSaveRequest{first, rejected, last}
	prepareGCWriteContexts(t, requests)
	saver.runFlush(gcBoundary, pool, requests)
	a, b, c := <-first.result, <-rejected.result, <-last.result
	require.NoError(t, a.err)
	require.ErrorContains(t, b.err, "rejected event")
	require.NoError(t, c.err)
	require.EqualValues(t, 2, a.globalID)
	require.EqualValues(t, 3, c.globalID)
	require.EqualValues(t, 4, readSeqNextID(t, pool))
	require.Zero(t, countEventsMatching(t, pool, map[string]any{"context": "RolledBack"}))
	// Closing and reopening the database must preserve the committed cursor.
	saver.close()
	require.NoError(t, pool.Close())
	pool, err = OpenBoundaryPools(t.Context(), dir, gcBoundary)
	require.NoError(t, err)
	saver = NewSqliteSaveEvents(map[string]*BoundaryPools{gcBoundary: pool}, logger)
	_, gid, err := saver.Save(t.Context(), []orisun.EventWithMapTags{mustEvent(t, "AfterRestart", nil, nil)}, gcBoundary, nil)
	require.NoError(t, err)
	require.EqualValues(t, 4, gid)
}

func TestGroupCommitSequenceFailureRollsBackEntireFlush(t *testing.T) {
	saver, pool, cleanup := newGCTestSaver(t)
	defer cleanup()
	conn, err := pool.Write.Take(t.Context())
	require.NoError(t, err)
	require.NoError(t, sqlitex.Execute(conn, `CREATE TRIGGER reject_sequence BEFORE UPDATE ON orisun_es_seq BEGIN SELECT RAISE(ABORT,'sequence failed'); END`, nil))
	pool.Write.Put(conn)
	requests := []*sqliteSaveRequest{sequenceRequest(t, "First"), sequenceRequest(t, "Second")}
	saver.runFlush(gcBoundary, pool, requests)
	for _, req := range requests {
		result := <-req.result
		require.Equal(t, statuscode.Internal, statuscode.CodeOf(result.err))
		require.ErrorContains(t, result.err, "sequence failed")
	}
	require.EqualValues(t, 1, readSeqNextID(t, pool))
	conn, err = pool.Write.Take(t.Context())
	require.NoError(t, err)
	require.NoError(t, sqlitex.Execute(conn, `SELECT (SELECT COUNT(*) FROM orisun_es_event), (SELECT COUNT(*) FROM orisun_es_write)`, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		require.Zero(t, stmt.ColumnInt64(0))
		require.Zero(t, stmt.ColumnInt64(1))
		return nil
	}}))
	require.NoError(t, sqlitex.Execute(conn, "DROP TRIGGER reject_sequence", nil))
	pool.Write.Put(conn)
	_, gid, err := saver.Save(t.Context(), []orisun.EventWithMapTags{mustEvent(t, "Retry", nil, nil)}, gcBoundary, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, gid)
}
