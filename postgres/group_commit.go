package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

// PostgreSQL group commit coalesces concurrent SaveEvents calls per boundary.
// A multi-request flush uses one database transaction. Requests that require
// failure isolation use subtransactions; canonical requests use narrower SQL
// paths. Every CCC request is evaluated in queue order, so later checks observe
// earlier accepted writes in the same flush. The SQL function's advisory lock
// remains the cross-process serialization boundary.
const (
	postgresGroupCommitMaxBatchRequests = 512
	postgresGroupCommitMaxBatchEvents   = 1024
	postgresGroupCommitMaxDelay         = 0
	postgresGroupCommitMaxPending       = 4096
	postgresGroupCommitFlushTimeout     = 30 * time.Second
)

func normalizePostgresGroupCommitConfig(cfg config.PostgresGroupCommitConfig) (config.PostgresGroupCommitConfig, error) {
	if cfg.MaxBatchRequests == 0 {
		cfg.MaxBatchRequests = postgresGroupCommitMaxBatchRequests
	}
	if cfg.MaxBatchEvents == 0 {
		cfg.MaxBatchEvents = postgresGroupCommitMaxBatchEvents
	}
	if cfg.MaxPending == 0 {
		cfg.MaxPending = postgresGroupCommitMaxPending
	}
	if cfg.FlushTimeout == 0 {
		cfg.FlushTimeout = postgresGroupCommitFlushTimeout
	}
	switch {
	case cfg.MaxBatchRequests < 0:
		return cfg, fmt.Errorf("postgres group commit maxBatchRequests must be >= 0, got %d", cfg.MaxBatchRequests)
	case cfg.MaxBatchEvents < 0:
		return cfg, fmt.Errorf("postgres group commit maxBatchEvents must be >= 0, got %d", cfg.MaxBatchEvents)
	case cfg.MaxDelay < 0:
		return cfg, fmt.Errorf("postgres group commit maxDelay must be >= 0, got %s", cfg.MaxDelay)
	case cfg.MaxPending < 0:
		return cfg, fmt.Errorf("postgres group commit maxPending must be >= 0, got %d", cfg.MaxPending)
	case cfg.FlushTimeout < 0:
		return cfg, fmt.Errorf("postgres group commit flushTimeout must be >= 0, got %s", cfg.FlushTimeout)
	}
	return cfg, nil
}

type postgresGroupCommit struct {
	maxBatchRequests int
	maxBatchEvents   int
	maxDelay         time.Duration
	maxPending       int
	flushTimeout     time.Duration

	queues    map[string]chan *postgresSaveRequest
	closed    chan struct{}
	closeOnce sync.Once
	enqueueMu sync.RWMutex
	workerWG  sync.WaitGroup

	multiFlushes       atomic.Int64
	singleFlushes      atomic.Int64
	fastFlushes        atomic.Int64
	canonicalFlushes   atomic.Int64
	independentFlushes atomic.Int64
	testFlushHook      func(batchSize int)
}

func newPostgresGroupCommit(cfg config.PostgresGroupCommitConfig) postgresGroupCommit {
	return postgresGroupCommit{
		maxBatchRequests: cfg.MaxBatchRequests,
		maxBatchEvents:   cfg.MaxBatchEvents,
		maxDelay:         cfg.MaxDelay,
		maxPending:       cfg.MaxPending,
		flushTimeout:     cfg.FlushTimeout,
		queues:           make(map[string]chan *postgresSaveRequest),
		closed:           make(chan struct{}),
	}
}

type postgresSaveRequest struct {
	ctx       context.Context
	events    eventstore.PreparedEventBatch
	expected  *eventstore.Position
	query     *eventstore.Query
	result    chan postgresSaveResult
	delivered bool
}

type postgresSaveResult struct {
	transactionID string
	globalID      int64
	err           error
}

func (r *postgresSaveRequest) deliver(result postgresSaveResult) {
	if r.delivered {
		return
	}
	r.delivered = true
	r.result <- result
}

var errPostgresSaverClosed = statuscode.New(statuscode.Unavailable, "postgres event saver is shut down")

func (s *PostgresSaveEvents) ensureQueue(boundary string) chan *postgresSaveRequest {
	s.gc.enqueueMu.Lock()
	defer s.gc.enqueueMu.Unlock()
	if s.isClosed() {
		return nil
	}
	if queue := s.gc.queues[boundary]; queue != nil {
		return queue
	}
	queue := make(chan *postgresSaveRequest, s.gc.maxPending)
	s.gc.queues[boundary] = queue
	s.gc.workerWG.Add(1)
	go s.runWorker(boundary, queue)
	return queue
}

func (s *PostgresSaveEvents) enqueue(
	ctx context.Context,
	boundary string,
	events eventstore.PreparedEventBatch,
	expected *eventstore.Position,
	query *eventstore.Query,
) (string, int64, error) {
	s.gc.enqueueMu.RLock()
	if s.isClosed() {
		s.gc.enqueueMu.RUnlock()
		return "", 0, errPostgresSaverClosed
	}
	queue := s.gc.queues[boundary]
	s.gc.enqueueMu.RUnlock()
	if queue == nil {
		queue = s.ensureQueue(boundary)
		if queue == nil {
			return "", 0, errPostgresSaverClosed
		}
	}

	req := &postgresSaveRequest{
		ctx:      ctx,
		events:   events,
		expected: expected,
		query:    query,
		result:   make(chan postgresSaveResult, 1),
	}

	s.gc.enqueueMu.RLock()
	if s.isClosed() {
		s.gc.enqueueMu.RUnlock()
		return "", 0, errPostgresSaverClosed
	}
	select {
	case queue <- req:
		s.gc.enqueueMu.RUnlock()
	case <-ctx.Done():
		s.gc.enqueueMu.RUnlock()
		return "", 0, statuscode.FromContextError(ctx.Err())
	case <-s.gc.closed:
		s.gc.enqueueMu.RUnlock()
		return "", 0, errPostgresSaverClosed
	}

	select {
	case result := <-req.result:
		return result.transactionID, result.globalID, result.err
	case <-ctx.Done():
		return "", 0, statuscode.FromContextError(ctx.Err())
	}
}

func (s *PostgresSaveEvents) runWorker(boundary string, queue chan *postgresSaveRequest) {
	defer s.gc.workerWG.Done()

	var carry *postgresSaveRequest
	for {
		req := carry
		carry = nil
		if req == nil {
			select {
			case <-s.gc.closed:
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
			req.deliver(postgresSaveResult{err: errPostgresSaverClosed})
			s.failFast(queue)
			return
		}

		var batch []*postgresSaveRequest
		batch, carry = s.drainBatch(queue, req)
		if s.isClosed() {
			failPostgresUndelivered(batch, errPostgresSaverClosed)
			if carry != nil {
				carry.deliver(postgresSaveResult{err: errPostgresSaverClosed})
			}
			s.failFast(queue)
			return
		}
		s.runFlush(boundary, batch)
		if s.isClosed() {
			if carry != nil {
				carry.deliver(postgresSaveResult{err: errPostgresSaverClosed})
			}
			s.failFast(queue)
			return
		}
	}
}

func (s *PostgresSaveEvents) failFast(queue chan *postgresSaveRequest) {
	for req := range queue {
		req.deliver(postgresSaveResult{err: errPostgresSaverClosed})
	}
}

func (s *PostgresSaveEvents) drainBatch(
	queue chan *postgresSaveRequest,
	first *postgresSaveRequest,
) ([]*postgresSaveRequest, *postgresSaveRequest) {
	batch := []*postgresSaveRequest{first}
	events := len(first.events)

	var delay <-chan time.Time
	if s.gc.maxDelay > 0 {
		timer := time.NewTimer(s.gc.maxDelay)
		defer timer.Stop()
		delay = timer.C
	}

	for len(batch) < s.gc.maxBatchRequests && events < s.gc.maxBatchEvents {
		if delay == nil {
			select {
			case req, ok := <-queue:
				if !ok {
					return batch, nil
				}
				if events+len(req.events) > s.gc.maxBatchEvents {
					return batch, req
				}
				batch = append(batch, req)
				events += len(req.events)
			default:
				return batch, nil
			}
			continue
		}
		select {
		case req, ok := <-queue:
			if !ok {
				return batch, nil
			}
			if events+len(req.events) > s.gc.maxBatchEvents {
				return batch, req
			}
			batch = append(batch, req)
			events += len(req.events)
		case <-delay:
			return batch, nil
		case <-s.gc.closed:
			return batch, nil
		}
	}
	return batch, nil
}

func (s *PostgresSaveEvents) runFlush(boundary string, batch []*postgresSaveRequest) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Errorf("postgres group commit: panic during flush for boundary %s: %v", boundary, recovered)
			failPostgresUndelivered(batch, statuscode.Errorf(statuscode.Internal, "flush panic: %v", recovered))
		}
	}()

	live := batch[:0]
	for _, req := range batch {
		if err := req.ctx.Err(); err != nil {
			req.deliver(postgresSaveResult{err: statuscode.FromContextError(err)})
			continue
		}
		live = append(live, req)
	}
	if len(live) == 0 {
		return
	}
	if len(live) == 1 {
		s.gc.singleFlushes.Add(1)
	} else {
		s.gc.multiFlushes.Add(1)
	}

	flushCtx, cancel := context.WithTimeout(context.Background(), s.gc.flushTimeout)
	defer cancel()

	if s.gc.testFlushHook != nil {
		s.gc.testFlushHook(len(live))
	}
	start := time.Now()
	outcomes, flushErr := s.executeBatch(flushCtx, boundary, live)
	if flushErr != nil {
		failPostgresUndelivered(live, statuscode.Errorf(statuscode.Internal, "group commit flush: %v", flushErr))
		return
	}
	accepted := 0
	for _, outcome := range outcomes {
		if outcome.err == nil {
			accepted++
		}
		outcome.req.deliver(postgresSaveResult{
			transactionID: outcome.transactionID,
			globalID:      outcome.globalID,
			err:           outcome.err,
		})
	}
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf(
			"postgres group commit: boundary=%s drained=%d accepted=%d rejected=%d duration=%s",
			boundary,
			len(batch),
			accepted,
			len(live)-accepted,
			time.Since(start),
		)
	}
}

type postgresBatchOutcome struct {
	req           *postgresSaveRequest
	transactionID string
	globalID      int64
	err           error
}

type postgresBatchPayload struct {
	Query  json.RawMessage `json:"query"`
	Events json.RawMessage `json:"events"`
}

func (s *PostgresSaveEvents) executeBatch(
	ctx context.Context,
	boundary string,
	live []*postgresSaveRequest,
) ([]postgresBatchOutcome, error) {
	entry, ok := s.registry.lookup(boundary)
	if !ok {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "no schema found for boundary: %s", boundary)
	}

	payloads := make([]postgresBatchPayload, 0, len(live))
	requests := make([]*postgresSaveRequest, 0, len(live))
	outcomes := make([]postgresBatchOutcome, 0, len(live))
	for _, req := range live {
		consistencyJSON, err := json.Marshal(getStreamSectionAsMap(req.expected, req.query))
		if err != nil {
			outcomes = append(outcomes, postgresBatchOutcome{
				req: req,
				err: statuscode.Errorf(
					statuscode.Internal,
					"failed to marshal consistency condition: %v",
					err,
				),
			})
			continue
		}
		eventsJSON, err := json.Marshal(req.events)
		if err != nil {
			outcomes = append(outcomes, postgresBatchOutcome{
				req: req,
				err: statuscode.Errorf(
					statuscode.Internal,
					"failed to marshal events: %v",
					err,
				),
			})
			continue
		}
		payloads = append(payloads, postgresBatchPayload{
			Query:  json.RawMessage(consistencyJSON),
			Events: json.RawMessage(eventsJSON),
		})
		requests = append(requests, req)
	}
	if len(payloads) == 0 {
		return outcomes, nil
	}

	payloadJSON, err := json.Marshal(payloads)
	if err != nil {
		return nil, fmt.Errorf("marshal group commit payload: %w", err)
	}
	insertQuery := entry.insertEventRequests
	selectedPath := "isolated"
	fastPath := canUseUnconditionalFastPath(requests)
	independentKey, independentPath := independentCCCKey(requests)
	canonicalPath := !fastPath && !independentPath && canUseCanonicalFastPath(requests)
	queryArgs := []any{boundary, entry.mapping.Schema, payloadJSON}
	if fastPath {
		insertQuery = entry.insertUnconditional
		selectedPath = "unconditional"
	} else if independentPath {
		insertQuery = entry.insertIndependent
		queryArgs = []any{boundary, entry.mapping.Schema, independentKey, payloadJSON}
		selectedPath = "independent-ccc"
	} else if canonicalPath {
		insertQuery = entry.insertCanonical
		selectedPath = "criterion-state"
	}
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf(
			"postgres group commit: boundary=%s path=%s requests=%d",
			boundary,
			selectedPath,
			len(requests),
		)
	}
	rows, err := s.db.QueryContext(
		ctx,
		insertQuery,
		queryArgs...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	seen := make([]bool, len(requests))
	sqlOutcomes := make([]postgresBatchOutcome, len(requests))
	for rows.Next() {
		var (
			index         int
			newGlobalID   sql.NullInt64
			transactionID sql.NullInt64
			globalID      sql.NullInt64
			errorCode     sql.NullString
			errorMessage  sql.NullString
		)
		if err := rows.Scan(
			&index,
			&newGlobalID,
			&transactionID,
			&globalID,
			&errorCode,
			&errorMessage,
		); err != nil {
			return nil, fmt.Errorf("scan group commit result: %w", err)
		}
		if index < 0 || index >= len(requests) {
			return nil, fmt.Errorf("group commit returned request index %d for batch of %d", index, len(requests))
		}
		if seen[index] {
			return nil, fmt.Errorf("group commit returned duplicate request index %d", index)
		}
		seen[index] = true

		outcome := postgresBatchOutcome{req: requests[index]}
		if errorMessage.Valid {
			outcome.err = s.mapSaveError(errors.New(errorMessage.String))
		} else if !transactionID.Valid || !globalID.Valid {
			return nil, fmt.Errorf(
				"group commit request %d returned neither a position nor an error (SQLSTATE %q)",
				index,
				errorCode.String,
			)
		} else {
			outcome.transactionID = strconv.FormatInt(transactionID.Int64, 10)
			outcome.globalID = globalID.Int64
		}
		sqlOutcomes[index] = outcome
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for index, wasSeen := range seen {
		if !wasSeen {
			return nil, fmt.Errorf("group commit returned no result for request index %d", index)
		}
	}
	if fastPath {
		s.gc.fastFlushes.Add(1)
	} else if independentPath {
		s.gc.independentFlushes.Add(1)
	} else if canonicalPath {
		s.gc.canonicalFlushes.Add(1)
	}
	return append(outcomes, sqlOutcomes...), nil
}

// Canonical requests with no CCC query have no dependency on earlier requests,
// so PostgreSQL can assign positions and insert every event in the flush
// set-wise. The criterion-state path handles ordered observation for queried
// saves; requests that could raise a request-local validation error in
// PostgreSQL stay on the subtransaction-isolated path.
func canUseUnconditionalFastPath(requests []*postgresSaveRequest) bool {
	for _, req := range requests {
		if !isUnconditionalFastPathRequest(req.events, req.query) {
			return false
		}
	}
	return true
}

func canUseCanonicalFastPath(requests []*postgresSaveRequest) bool {
	for _, req := range requests {
		if !isCanonicalEventBatchRequest(req.events) {
			return false
		}
	}
	return true
}

// independentCCCKey recognizes a common, fully independent CCC batch:
// every request has one equality tag on the same field, all values are unique,
// and each event belongs to its request's context. No accepted event can then
// change another request's result, so the database can check every context
// against one locked snapshot and bulk-insert the accepted rows.
func independentCCCKey(requests []*postgresSaveRequest) (string, bool) {
	var criterionKey string
	values := make(map[string]struct{}, len(requests))
	for _, req := range requests {
		if !isCanonicalEventBatchRequest(req.events) ||
			req.query == nil ||
			len(req.query.Criteria) != 1 ||
			req.query.Criteria[0] == nil ||
			len(req.query.Criteria[0].Tags) != 1 ||
			req.query.Criteria[0].Tags[0] == nil {
			return "", false
		}
		tag := req.query.Criteria[0].Tags[0]
		if tag.Key == "" {
			return "", false
		}
		if criterionKey == "" {
			criterionKey = tag.Key
		} else if tag.Key != criterionKey {
			return "", false
		}
		if _, duplicate := values[tag.Value]; duplicate {
			return "", false
		}
		values[tag.Value] = struct{}{}

		for _, event := range req.events {
			var data map[string]json.RawMessage
			if err := json.Unmarshal([]byte(event.DataJSON), &data); err != nil {
				return "", false
			}
			rawValue, exists := data[tag.Key]
			if !exists {
				return "", false
			}
			var eventValue string
			if err := json.Unmarshal(rawValue, &eventValue); err != nil || eventValue != tag.Value {
				return "", false
			}
		}
	}
	return criterionKey, criterionKey != ""
}

func isUnconditionalFastPathRequest(events eventstore.PreparedEventBatch, query *eventstore.Query) bool {
	return query == nil && isCanonicalEventBatchRequest(events)
}

func isCanonicalEventBatchRequest(events eventstore.PreparedEventBatch) bool {
	if len(events) == 0 {
		return false
	}
	for _, event := range events {
		if event.EventType == "" {
			return false
		}
		if _, err := uuid.Parse(event.EventId); err != nil {
			return false
		}
	}
	return true
}

func (s *PostgresSaveEvents) mapSaveError(err error) error {
	if strings.Contains(err.Error(), "OptimisticConcurrencyException") {
		return statuscode.New(statuscode.AlreadyExists, err.Error())
	}
	s.logger.Errorf("Error inserting events: %v", err)
	return statuscode.Errorf(statuscode.Internal, "failed to insert events: %v", err)
}

func (s *PostgresSaveEvents) close() {
	s.gc.closeOnce.Do(func() {
		close(s.gc.closed)

		s.gc.enqueueMu.Lock()
		queues := make([]chan *postgresSaveRequest, 0, len(s.gc.queues))
		for _, queue := range s.gc.queues {
			queues = append(queues, queue)
		}
		s.gc.enqueueMu.Unlock()

		for _, queue := range queues {
			close(queue)
		}
		s.gc.workerWG.Wait()
	})
}

func (s *PostgresSaveEvents) isClosed() bool {
	select {
	case <-s.gc.closed:
		return true
	default:
		return false
	}
}

func failPostgresUndelivered(requests []*postgresSaveRequest, err error) {
	for _, req := range requests {
		req.deliver(postgresSaveResult{err: err})
	}
}
