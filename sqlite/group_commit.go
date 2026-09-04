package sqlite

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Group commit coalesces concurrent Save calls per boundary into one SQLite
// transaction per flush. Eligible requests use set-based paths; other requests
// retain savepoint-isolated CCC checks and results.
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
	s.enqueueMu.RLock()
	if s.isClosed() {
		s.enqueueMu.RUnlock()
		return "", 0, errSaverClosed
	}
	req := &sqliteSaveRequest{
		ctx:         ctx,
		inserts:     inserts,
		consistency: consistency,
		result:      make(chan sqliteSaveResult, 1),
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
// transaction; the selected execution path preserves each request's CCC result.
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

	live := batch[:0]
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
	path := sqliteFlushIsolated
	var independentPredicates []string
	if len(live) > 1 && !s.gcDisableSetPaths {
		if canUseUnconditionalFastPath(live) {
			path = sqliteFlushUnconditional
		} else if predicates, ok := independentCCCContexts(live, pool, boundary); ok {
			path = sqliteFlushIndependentCCC
			independentPredicates = predicates
		}
	}
	accepted, flushErr := s.flushTx(conn, pool, boundary, live, path, independentPredicates)
	if flushErr != nil {
		// Begin or commit failed: nothing persisted; every provisionally
		// accepted request reports the error. (endFn inside flushTx already
		// rolled back, so the connection returns to the pool clean.)
		failUndelivered(live, statuscode.Errorf(statuscode.Internal, "group commit flush: %v", flushErr))
		return
	}
	switch path {
	case sqliteFlushUnconditional:
		s.gcUnconditionalFlushes.Add(1)
	case sqliteFlushIndependentCCC:
		s.gcIndependentFlushes.Add(1)
	}
	for _, a := range accepted {
		a.req.deliver(sqliteSaveResult{transactionID: a.transactionID, globalID: a.globalID})
	}
	if len(accepted) > 0 && s.notifier != nil {
		s.notifier.Notify(boundary)
	}
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("sqlite group commit: boundary=%s path=%s drained=%d accepted=%d rejected=%d duration=%s",
			boundary, path.String(), len(batch), len(accepted), len(live)-len(accepted), time.Since(start))
	}
}

type acceptedSave struct {
	req           *sqliteSaveRequest
	transactionID string
	globalID      int64
}

type sqliteFlushPath uint8

const (
	sqliteFlushIsolated sqliteFlushPath = iota
	sqliteFlushUnconditional
	sqliteFlushIndependentCCC
)

func (p sqliteFlushPath) String() string {
	switch p {
	case sqliteFlushUnconditional:
		return "unconditional"
	case sqliteFlushIndependentCCC:
		return "independent-ccc"
	default:
		return "isolated"
	}
}

// flushTx runs one IMMEDIATE transaction over the live requests. Eligible
// batches allocate and insert set-wise; isolated multi-request batches use one
// savepoint per request in queue order.
// Per-request failures (CCC conflicts, invalid criteria) in the isolated path
// roll back only that request's savepoint, including its orisun_es_seq update,
// so rejected requests leave no position gaps. The returned flushErr is a
// whole-batch failure: BEGIN or COMMIT failed and nothing was persisted.
//
// endFn is deferred, so it also converts a mid-flush panic into a rollback
// before re-panicking (recovered by runFlush).
func (s *SqliteSaveEvents) flushTx(
	conn *sqlite.Conn,
	pool *BoundaryPools,
	boundary string,
	live []*sqliteSaveRequest,
	path sqliteFlushPath,
	independentPredicates []string,
) (accepted []acceptedSave, flushErr error) {
	endFn, beginErr := sqlitex.ImmediateTransaction(conn)
	if beginErr != nil {
		return nil, beginErr
	}
	defer endFn(&flushErr)
	switch path {
	case sqliteFlushUnconditional:
		return s.saveUnconditionalBatch(conn, live)
	case sqliteFlushIndependentCCC:
		return s.saveIndependentCCCBatch(conn, live, independentPredicates)
	}

	accepted = make([]acceptedSave, 0, len(live))
	for _, req := range live {
		if ctxErr := req.ctx.Err(); ctxErr != nil {
			req.deliver(sqliteSaveResult{err: statuscode.FromContextError(ctxErr)})
			continue
		}
		var txID string
		var gid int64
		var err error
		if len(live) == 1 {
			// Batch of one: the transaction itself is the rollback boundary,
			// so the savepoint pair is redundant. On error the transaction
			// rolls back whole (via the returned flushErr), which for a single
			// request is exactly the savepoint rollback.
			txID, gid, err = s.saveEventsOnConn(conn, pool, boundary, req.inserts, req.consistency)
			if err != nil {
				req.deliver(sqliteSaveResult{err: err})
				return nil, err
			}
		} else {
			txID, gid, err = s.saveSavepointed(conn, pool, boundary, req)
			if err != nil {
				req.deliver(sqliteSaveResult{err: err})
				continue
			}
		}
		accepted = append(accepted, acceptedSave{req: req, transactionID: txID, globalID: gid})
	}
	return accepted, nil
}

// canUseUnconditionalFastPath selects flushes without effective CCC criteria
// that cannot produce a request-local consistency error and whose event data
// satisfies SQLite's json_valid table constraint. Direct SavePrepared callers
// can still supply malformed prepared data, so those batches retain isolation.
func canUseUnconditionalFastPath(requests []*sqliteSaveRequest) bool {
	for _, req := range requests {
		if len(req.consistency) > 0 || len(req.inserts) == 0 {
			return false
		}
		for _, event := range req.inserts {
			if !json.Valid([]byte(event.DataJSON)) {
				return false
			}
		}
	}
	return true
}

// saveUnconditionalBatch allocates one contiguous ID range and inserts every
// event set-wise. Each request still receives the same position it would have
// received from queue-ordered calls to saveEventsOnConn: its last global ID is
// also its transaction ID.
func (s *SqliteSaveEvents) saveUnconditionalBatch(
	conn *sqlite.Conn,
	requests []*sqliteSaveRequest,
) ([]acceptedSave, error) {
	totalEvents := 0
	for _, req := range requests {
		totalEvents += len(req.inserts)
	}
	firstID, _, err := allocateGlobalIDs(conn, totalEvents)
	if err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "allocate ids: %v", err)
	}

	accepted := make([]acceptedSave, 0, len(requests))
	positioned := make([]positionedPreparedEvent, 0, totalEvents)
	nextID := firstID
	for _, req := range requests {
		transactionID := nextID + int64(len(req.inserts)) - 1
		for _, event := range req.inserts {
			positioned = append(positioned, positionedPreparedEvent{
				event:         event,
				globalID:      nextID,
				transactionID: transactionID,
			})
			nextID++
		}
		accepted = append(accepted, acceptedSave{
			req:           req,
			transactionID: strconv.FormatInt(transactionID, 10),
			globalID:      transactionID,
		})
	}

	if err := insertPositionedEventBatch(conn, positioned); err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "insert events: %v", err)
	}
	return accepted, nil
}

// independentCCCContexts recognizes a batch whose contexts cannot affect one
// another: one equality tag per request, one common text-like key, unique
// values, and every emitted event belongs to its request's context.
func independentCCCContexts(
	requests []*sqliteSaveRequest,
	pool *BoundaryPools,
	boundary string,
) ([]string, bool) {
	var criterionKey string
	values := make(map[string]struct{}, len(requests))
	predicates := make([]string, len(requests))
	for requestIndex, req := range requests {
		if len(req.consistency) != 1 ||
			len(req.consistency[0].Criteria) != 1 ||
			len(req.consistency[0].Criteria[0].Tags) != 1 ||
			len(req.inserts) == 0 {
			return nil, false
		}
		tag := req.consistency[0].Criteria[0].Tags[0]
		if tag.Key == "" {
			return nil, false
		}
		if criterionKey == "" {
			criterionKey = tag.Key
			valueType, _, _ := pool.indexes.fieldTypeInfo(boundary, criterionKey)
			if normalized := normalizeIndexValueType(valueType); normalized == "numeric" || normalized == "boolean" {
				// Distinct string values can compare equal after numeric or
				// boolean coercion, so only text-like contexts are independent.
				return nil, false
			}
		} else if tag.Key != criterionKey {
			return nil, false
		}
		if _, duplicate := values[tag.Value]; duplicate {
			return nil, false
		}
		values[tag.Value] = struct{}{}

		for _, event := range req.inserts {
			var data map[string]json.RawMessage
			if err := json.Unmarshal([]byte(event.DataJSON), &data); err != nil {
				return nil, false
			}
			rawValue, exists := data[tag.Key]
			if !exists {
				return nil, false
			}
			var eventValue string
			if err := json.Unmarshal(rawValue, &eventValue); err != nil || eventValue != tag.Value {
				return nil, false
			}
		}

		predicate, err := buildCriteriaSQLForBoundary(
			[]map[string]any{{tag.Key: tag.Value}},
			pool.indexes,
			boundary,
		)
		if err != nil {
			return nil, false
		}
		predicates[requestIndex] = predicate
	}
	return predicates, criterionKey != ""
}

// saveIndependentCCCBatch checks independent contexts against the transaction
// snapshot without per-request savepoints. Only matching requests are assigned
// IDs; their events are allocated and inserted together.
func (s *SqliteSaveEvents) saveIndependentCCCBatch(
	conn *sqlite.Conn,
	requests []*sqliteSaveRequest,
	predicates []string,
) ([]acceptedSave, error) {
	if len(predicates) != len(requests) {
		return nil, statuscode.New(statuscode.Internal, "independent CCC predicate count mismatch")
	}

	actualPositions := make([]eventstore.Position, len(requests))
	for requestIndex, predicate := range predicates {
		actualPositions[requestIndex] = eventstore.NotExistsPosition()
		checkSQL := "SELECT transaction_id, global_id FROM orisun_es_event WHERE " + predicate +
			" ORDER BY transaction_id DESC, global_id DESC LIMIT 1"
		if err := sqlitex.ExecuteTransient(conn, checkSQL, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				actualPositions[requestIndex] = eventstore.Position{
					CommitPosition:  stmt.ColumnInt64(0),
					PreparePosition: stmt.ColumnInt64(1),
				}
				return nil
			},
		}); err != nil {
			return nil, statuscode.Errorf(statuscode.Internal, "independent CCC check: %v", err)
		}
	}

	acceptedRequests := make([]*sqliteSaveRequest, 0, len(requests))
	for requestIndex, req := range requests {
		expected := req.consistency[0].Position
		actual := actualPositions[requestIndex]
		if expected != actual {
			req.deliver(sqliteSaveResult{err: statuscode.Errorf(
				statuscode.AlreadyExists,
				"OptimisticConcurrencyException:StreamVersionConflict: Expected (%d, %d), Actual (%d, %d)",
				expected.CommitPosition,
				expected.PreparePosition,
				actual.CommitPosition,
				actual.PreparePosition,
			)})
			continue
		}
		acceptedRequests = append(acceptedRequests, req)
	}
	if len(acceptedRequests) == 0 {
		return nil, nil
	}
	return s.saveUnconditionalBatch(conn, acceptedRequests)
}

// General CCC batches deliberately stay on the savepoint-isolated path. SQLite's
// set-wise dependency join grows with events times distinct criteria and regresses
// realistic high-cardinality workloads; the simple queue-ordered path is both faster
// and easier to verify.

// saveSavepointed wraps one request's CCC check + insert in a savepoint, so
// a rejection rolls back that request without poisoning the transaction.
func (s *SqliteSaveEvents) saveSavepointed(
	conn *sqlite.Conn,
	pool *BoundaryPools,
	boundary string,
	req *sqliteSaveRequest,
) (transactionID string, globalID int64, err error) {
	releaseFn := sqlitex.Save(conn)
	defer releaseFn(&err)
	return s.saveEventsOnConn(conn, pool, boundary, req.inserts, req.consistency)
}

func failUndelivered(batch []*sqliteSaveRequest, err error) {
	for _, req := range batch {
		req.deliver(sqliteSaveResult{err: err})
	}
}
