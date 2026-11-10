package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// executeBatch owns the connection and committed outcomes, never caller delivery.
func (s *SqliteSaveEvents) executeBatch(ctx context.Context, boundary string, live []preparedSQLiteSave) ([]sqliteSaveResult, error) {
	pool, ok := s.registry.eventPool(boundary)
	if !ok {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "unknown boundary: %s", boundary)
	}
	if len(live) == 0 {
		return nil, nil
	}
	if len(live) == 1 {
		s.gcSingleFlushes.Add(1)
	} else {
		s.gcMultiFlushes.Add(1)
	}
	conn, err := pool.Write.Take(ctx)
	if err != nil {
		return nil, fmt.Errorf("take write conn: %w", err)
	}
	defer pool.Write.Put(conn)
	if s.gcTestFlushHook != nil {
		s.gcTestFlushHook(len(live))
	}
	start := time.Now()
	outcomes, err := s.executeTransaction(conn, live)
	if err != nil {
		return nil, err
	}
	accepted := 0
	for _, outcome := range outcomes {
		if outcome.err == nil {
			accepted++
		}
	}
	// executeTransaction returns only after its deferred commit succeeds.
	if accepted > 0 && s.notifier != nil {
		s.notifier.Notify(boundary)
	}
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("sqlite group commit: boundary=%s requests=%d accepted=%d rejected=%d duration=%s", boundary, len(live), accepted, len(live)-accepted, time.Since(start))
	}
	return outcomes, nil
}

// executeTransaction keeps all CCC reads and mutations under BEGIN IMMEDIATE.
// Savepoints isolate request failures. A transaction failure invalidates every
// provisional outcome; no caller is completed before the deferred commit.
func (s *SqliteSaveEvents) executeTransaction(
	conn *sqlite.Conn,
	live []preparedSQLiteSave,
) (outcomes []sqliteSaveResult, flushErr error) {
	endFn, beginErr := sqlitex.ImmediateTransaction(conn)
	if beginErr != nil {
		return nil, beginErr
	}
	defer endFn(&flushErr)

	// global_id is the rowid and events are never deleted, so the log's tail is
	// the sequence: no counter row to rewrite on every commit. The lookup seeks
	// the last rowid; rolled-back requests leave no rows and so allocate nothing.
	nextID, err := nextGlobalID(conn)
	if err != nil {
		return nil, err
	}
	outcomes = make([]sqliteSaveResult, len(live))
	for i := range live {
		req := &live[i]
		if ctxErr := req.ctx.Err(); ctxErr != nil {
			outcomes[i].err = statuscode.FromContextError(ctxErr)
			continue
		}
		txID, gid, err := s.saveSavepointed(conn, req, nextID)
		if err != nil {
			outcomes[i].err = err
			continue
		}
		nextID = gid + 1
		outcomes[i] = sqliteSaveResult{transactionID: txID, globalID: gid}
	}
	return outcomes, nil
}

func nextGlobalID(conn *sqlite.Conn) (int64, error) {
	var nextID int64
	err := sqlitex.Execute(conn, "SELECT COALESCE(MAX(global_id), 0) + 1 FROM orisun_es_event", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error { nextID = stmt.ColumnInt64(0); return nil },
	})
	return nextID, err
}

// saveSavepointed checks CCC under the flush's IMMEDIATE transaction, which
// retains the single-writer lock across both checks and writes. Read-only
// rejections need no savepoint; every insertion remains protected by one.
func (s *SqliteSaveEvents) saveSavepointed(
	conn *sqlite.Conn,
	req *preparedSQLiteSave,
	firstID int64,
) (transactionID string, globalID int64, err error) {
	for _, check := range req.preparedChecks {
		if check.err != nil {
			return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "invalid consistency criteria: %v", check.err)
		}
		latestTx, latestGid, checkErr := latestPreparedCriteriaPosition(conn, check.queries)
		if checkErr != nil {
			return "", 0, checkErr
		}
		if latestTx != check.position.CommitPosition || latestGid != check.position.PreparePosition {
			return "", 0, statuscode.Errorf(statuscode.AlreadyExists,
				"OptimisticConcurrencyException:StreamVersionConflict: Expected (%d, %d), Actual (%d, %d)",
				check.position.CommitPosition, check.position.PreparePosition, latestTx, latestGid)
		}
	}

	releaseFn := sqlitex.Save(conn)
	defer releaseFn(&err)
	return s.saveEventsOnConn(conn, req.inserts, req.consistencyJSON, firstID)
}
