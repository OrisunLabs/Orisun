package sqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Group commit coalesces concurrent Save calls per boundary into one SQLite
// transaction per flush. Every request uses queue-ordered CCC checks and a
// savepoint to isolate its result, including batches of one.
//
// Batching is opportunistic (maxDelay = 0): the worker never waits to fill a
// batch. Batches form naturally while a flush holds the single write
// connection and new requests queue behind it.
//
// Defaults; overridable via sqlite.groupCommit in config.yaml
// (ORISUN_SQLITE_GC_* env vars).
const (
	sqliteGroupCommitMaxBatchRequests = 128
	sqliteGroupCommitMaxBatchEvents   = 1024
	sqliteGroupCommitMaxDelay         = 0
	sqliteGroupCommitMaxPending       = 4096
	sqliteGroupCommitFlushTimeout     = 30 * time.Second
)

// normalizeGroupCommitConfig applies package defaults to zero values and
// rejects negatives. MaxDelay 0 is a meaningful setting (opportunistic
// batching), so it has no non-zero default.
func normalizeGroupCommitConfig(cfg config.SqliteGroupCommitConfig) (config.SqliteGroupCommitConfig, error) {
	if cfg.MaxBatchRequests == 0 {
		cfg.MaxBatchRequests = sqliteGroupCommitMaxBatchRequests
	}
	if cfg.MaxBatchEvents == 0 {
		cfg.MaxBatchEvents = sqliteGroupCommitMaxBatchEvents
	}
	if cfg.MaxPending == 0 {
		cfg.MaxPending = sqliteGroupCommitMaxPending
	}
	if cfg.FlushTimeout == 0 {
		cfg.FlushTimeout = sqliteGroupCommitFlushTimeout
	}
	switch {
	case cfg.MaxBatchRequests < 0:
		return cfg, fmt.Errorf("sqlite group commit maxBatchRequests must be >= 0, got %d", cfg.MaxBatchRequests)
	case cfg.MaxBatchEvents < 0:
		return cfg, fmt.Errorf("sqlite group commit maxBatchEvents must be >= 0, got %d", cfg.MaxBatchEvents)
	case cfg.MaxDelay < 0:
		return cfg, fmt.Errorf("sqlite group commit maxDelay must be >= 0, got %s", cfg.MaxDelay)
	case cfg.MaxPending < 0:
		return cfg, fmt.Errorf("sqlite group commit maxPending must be >= 0, got %d", cfg.MaxPending)
	case cfg.FlushTimeout < 0:
		return cfg, fmt.Errorf("sqlite group commit flushTimeout must be >= 0, got %s", cfg.FlushTimeout)
	}
	return cfg, nil
}

type sqliteSaveRequest struct {
	ctx         context.Context
	inserts     eventstore.PreparedEventBatch
	consistency []eventstore.ConsistencyCheck
	// consistencyJSON is prepared before enqueueing, outside the writer transaction.
	consistencyJSON string
	// result has capacity 1 so the worker's send never blocks on a caller
	// that abandoned its Save after inclusion in a flush.
	result chan sqliteSaveResult
	// delivered is touched only by the boundary worker goroutine.
	delivered bool
}

type sqliteSaveResult struct {
	transactionID string
	globalID      int64
	err           error
}

// deliver sends a request's result exactly once. Worker-goroutine only.
func (r *sqliteSaveRequest) deliver(res sqliteSaveResult) {
	if r.delivered {
		return
	}
	r.delivered = true
	r.result <- res
}

var errSaverClosed = statuscode.New(statuscode.Unavailable, "sqlite event saver is shut down")

// enqueue hands the request to the boundary worker and waits for its result.
// A context cancellation after the request may already be in a flush carries
// the same ambiguity as cancelling a direct write mid-transaction: the batch
// may still commit.
func (s *SqliteSaveEvents) enqueue(
	ctx context.Context,
	boundary string,
	inserts eventstore.PreparedEventBatch,
	consistency []eventstore.ConsistencyCheck,
) (string, int64, error) {
	data, err := eventstore.MarshalConsistency(consistency)
	if err != nil {
		return "", 0, err
	}
	s.enqueueMu.RLock()
	if s.isClosed() {
		s.enqueueMu.RUnlock()
		return "", 0, errSaverClosed
	}
	req := &sqliteSaveRequest{
		ctx:             ctx,
		inserts:         inserts,
		consistency:     consistency,
		consistencyJSON: string(data),
		result:          make(chan sqliteSaveResult, 1),
	}
	queue := s.queues[boundary]
	if queue == nil {
		s.enqueueMu.RUnlock()
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "unknown boundary: %s", boundary)
	}
	select {
	case queue <- req:
		s.enqueueMu.RUnlock()
	case <-ctx.Done():
		s.enqueueMu.RUnlock()
		return "", 0, statuscode.FromContextError(ctx.Err())
	case <-s.closed:
		s.enqueueMu.RUnlock()
		return "", 0, errSaverClosed
	}
	select {
	case res := <-req.result:
		return res.transactionID, res.globalID, res.err
	case <-ctx.Done():
		return "", 0, statuscode.FromContextError(ctx.Err())
	}
}

// runWorker is the per-boundary write loop: take one request, drain more up
// to the batch limits, flush, repeat. After close() it fails everything in
// (and arriving on) the queue and parks.
func (s *SqliteSaveEvents) runWorker(boundary string, pool *BoundaryPools, queue chan *sqliteSaveRequest) {
	defer s.workerWG.Done()

	var carry *sqliteSaveRequest
	for {
		req := carry
		carry = nil
		if req == nil {
			select {
			case <-s.closed:
				s.failFast(queue)
				return
			case next, ok := <-queue:
				if !ok {
					return
				}
				req = next
			}
		}
		if s.isClosed() {
			req.deliver(sqliteSaveResult{err: errSaverClosed})
			if carry != nil {
				carry.deliver(sqliteSaveResult{err: errSaverClosed})
			}
			s.failFast(queue)
			return
		}

		var batch []*sqliteSaveRequest
		batch, carry = s.drainBatch(queue, req)
		if s.isClosed() {
			failUndelivered(batch, errSaverClosed)
			if carry != nil {
				carry.deliver(sqliteSaveResult{err: errSaverClosed})
			}
			s.failFast(queue)
			return
		}
		s.runFlush(boundary, pool, batch)
		if s.isClosed() {
			if carry != nil {
				carry.deliver(sqliteSaveResult{err: errSaverClosed})
			}
			s.failFast(queue)
			return
		}
	}
}

// failFast serves the queue in shutdown mode: every remaining request fails
// immediately. close() guarantees no further sends and closes the queue, so
// the loop ends once the buffered requests are drained.
func (s *SqliteSaveEvents) failFast(queue chan *sqliteSaveRequest) {
	for req := range queue {
		req.deliver(sqliteSaveResult{err: errSaverClosed})
	}
}

// drainBatch greedily collects queued requests behind first, bounded by the
// request and event limits. With gcMaxDelay > 0 it waits up to that long for
// the batch to fill instead of flushing on the first empty read.
func (s *SqliteSaveEvents) drainBatch(queue chan *sqliteSaveRequest, first *sqliteSaveRequest) ([]*sqliteSaveRequest, *sqliteSaveRequest) {
	batch := []*sqliteSaveRequest{first}
	events := len(first.inserts)

	var delay <-chan time.Time
	if s.gcMaxDelay > 0 {
		timer := time.NewTimer(s.gcMaxDelay)
		defer timer.Stop()
		delay = timer.C
	}

	for len(batch) < s.gcMaxBatchRequests && events < s.gcMaxBatchEvents {
		if delay == nil {
			select {
			case req, ok := <-queue:
				if !ok {
					return batch, nil
				}
				if len(batch) > 0 && events+len(req.inserts) > s.gcMaxBatchEvents {
					return batch, req
				}
				batch = append(batch, req)
				events += len(req.inserts)
			default:
				return batch, nil
			}
		} else {
			select {
			case req, ok := <-queue:
				if !ok {
					return batch, nil
				}
				if len(batch) > 0 && events+len(req.inserts) > s.gcMaxBatchEvents {
					return batch, req
				}
				batch = append(batch, req)
				events += len(req.inserts)
			case <-delay:
				return batch, nil
			case <-s.closed:
				return batch, nil
			}
		}
	}
	return batch, nil
}

// runFlush writes one batch. Requests whose context is already cancelled are
// answered with their context error and excluded. All live requests share one
// transaction; each request has its own savepoint and CCC result.
// A panic anywhere in the flush is answered with INTERNAL for every
// undelivered request and the worker keeps serving.
func (s *SqliteSaveEvents) runFlush(
	boundary string,
	pool *BoundaryPools,
	batch []*sqliteSaveRequest,
) {
	defer func() {
		if p := recover(); p != nil {
			s.logger.Errorf("sqlite group commit: panic during flush for boundary %s: %v", boundary, p)
			failUndelivered(batch, statuscode.Errorf(statuscode.Internal, "flush panic: %v", p))
		}
	}()

	live := make([]*sqliteSaveRequest, 0, len(batch))
	for _, req := range batch {
		if ctxErr := req.ctx.Err(); ctxErr != nil {
			req.deliver(sqliteSaveResult{err: statuscode.FromContextError(ctxErr)})
			continue
		}
		live = append(live, req)
	}
	if len(live) == 0 {
		return
	}
	if len(live) == 1 {
		s.gcSingleFlushes.Add(1)
	} else {
		s.gcMultiFlushes.Add(1)
	}

	// The flush must not run under any single caller's context — one
	// cancellation would poison the whole batch. Pool.Take wires the flush
	// context's Done channel in as the connection's interrupt, bounding the
	// transaction by gcFlushTimeout.
	flushCtx, cancel := context.WithTimeout(context.Background(), s.gcFlushTimeout)
	defer cancel()

	conn, takeErr := pool.Write.Take(flushCtx)
	if takeErr != nil {
		err := statuscode.Errorf(statuscode.Internal, "take write conn: %v", takeErr)
		if s.isClosed() {
			err = errSaverClosed
		}
		failUndelivered(live, err)
		return
	}
	defer pool.Write.Put(conn)

	if s.gcTestFlushHook != nil {
		s.gcTestFlushHook(len(live))
	}

	start := time.Now()
	accepted, flushErr := s.flushTx(conn, live)
	if flushErr != nil {
		// The transaction failed: nothing persisted; every provisionally
		// accepted request reports the error. (endFn inside flushTx already
		// rolled back, so the connection returns to the pool clean.)
		failUndelivered(live, statuscode.Errorf(statuscode.Internal, "group commit flush: %v", flushErr))
		return
	}
	for _, a := range accepted {
		a.req.deliver(sqliteSaveResult{transactionID: a.transactionID, globalID: a.globalID})
	}
	if len(accepted) > 0 && s.notifier != nil {
		s.notifier.Notify(boundary)
	}
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("sqlite group commit: boundary=%s drained=%d accepted=%d rejected=%d duration=%s",
			boundary, len(batch), len(accepted), len(live)-len(accepted), time.Since(start))
	}
}

type acceptedSave struct {
	req           *sqliteSaveRequest
	transactionID string
	globalID      int64
}

// flushTx runs one IMMEDIATE transaction over the live requests, using one
// savepoint per request in queue order. A request-local failure rolls back
// its events and write context. The sequence advances once per flush, only for
// accepted requests. BEGIN, sequence-update, or COMMIT failures
// fail the whole flush.
//
// endFn is deferred, so it also converts a mid-flush panic into a rollback
// before re-panicking (recovered by runFlush).
func (s *SqliteSaveEvents) flushTx(
	conn *sqlite.Conn,
	live []*sqliteSaveRequest,
) (accepted []acceptedSave, flushErr error) {
	endFn, beginErr := sqlitex.ImmediateTransaction(conn)
	if beginErr != nil {
		return nil, beginErr
	}
	defer endFn(&flushErr)

	var nextID int64
	if err := sqlitex.Execute(conn, "SELECT next_id FROM orisun_es_seq WHERE id = 1", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error { nextID = stmt.ColumnInt64(0); return nil },
	}); err != nil {
		return nil, err
	}
	if nextID < 1 {
		return nil, fmt.Errorf("invalid event sequence: %d", nextID)
	}
	accepted = make([]acceptedSave, 0, len(live))
	for _, req := range live {
		if ctxErr := req.ctx.Err(); ctxErr != nil {
			req.deliver(sqliteSaveResult{err: statuscode.FromContextError(ctxErr)})
			continue
		}
		txID, gid, err := s.saveSavepointed(conn, req, nextID)
		if err != nil {
			req.deliver(sqliteSaveResult{err: err})
			continue
		}
		nextID = gid + 1
		accepted = append(accepted, acceptedSave{req: req, transactionID: txID, globalID: gid})
	}
	if len(accepted) > 0 {
		if err := sqlitex.Execute(conn, "UPDATE orisun_es_seq SET next_id = ? WHERE id = 1", &sqlitex.ExecOptions{Args: []any{nextID}}); err != nil {
			return nil, err
		}
	}
	return accepted, nil
}

// saveSavepointed wraps one request's CCC check + insert in a savepoint, so
// a rejection rolls back that request without poisoning the transaction.
func (s *SqliteSaveEvents) saveSavepointed(
	conn *sqlite.Conn,
	req *sqliteSaveRequest,
	firstID int64,
) (transactionID string, globalID int64, err error) {
	releaseFn := sqlitex.Save(conn)
	defer releaseFn(&err)
	return s.saveEventsOnConn(conn, req.inserts, req.consistency, req.consistencyJSON, firstID)
}

func failUndelivered(batch []*sqliteSaveRequest, err error) {
	for _, req := range batch {
		req.deliver(sqliteSaveResult{err: err})
	}
}
