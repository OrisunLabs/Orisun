package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

// PostgreSQL group commit coalesces concurrent SaveEvents calls per boundary.
// Every flush uses one canonical SQL implementation and one transaction.
// Malformed requests are rejected before constructing the SQL batch.
// Every CCC request is evaluated in queue order, so later checks observe
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

	testFlushHook func(batchSize int)
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
	ctx         context.Context
	events      eventstore.PreparedEventBatch
	consistency []eventstore.ConsistencyCheck
	result      chan postgresSaveResult
	delivered   bool
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
	consistency []eventstore.ConsistencyCheck,
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
		ctx:         ctx,
		events:      events,
		consistency: consistency,
		result:      make(chan postgresSaveResult, 1),
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
	Consistency json.RawMessage `json:"consistency"`
	Events      json.RawMessage `json:"events"`
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
		if err := validatePostgresSaveRequest(req); err != nil {
			outcomes = append(outcomes, postgresBatchOutcome{req: req, err: s.mapSaveError(err)})
			continue
		}
		consistencyJSON, err := eventstore.MarshalConsistency(req.consistency)
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
			Consistency: json.RawMessage(consistencyJSON),
			Events:      json.RawMessage(eventsJSON),
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
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("postgres group commit: boundary=%s requests=%d", boundary, len(requests))
	}
	rows, err := s.db.QueryContext(ctx, entry.insertEventRequests, boundary, entry.mapping.Schema, payloadJSON)
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
	return append(outcomes, sqlOutcomes...), nil
}

// Validate backend-facing requests before sharing a transaction. A malformed
// request must never abort valid neighbors or advance their criterion state.
func validatePostgresSaveRequest(req *postgresSaveRequest) error {
	if len(req.events) == 0 {
		return errors.New("events cannot be empty")
	}
	for _, event := range req.events {
		if event.EventType == "" {
			return errors.New("event_type cannot be empty")
		}
		if _, err := uuid.Parse(event.EventId); err != nil {
			return fmt.Errorf("invalid event_id: %w", err)
		}
		data := strings.TrimSpace(event.DataJSON)
		if len(data) == 0 || data[0] != '{' || !json.Valid([]byte(data)) {
			return errors.New("event data must be a JSON object")
		}
	}
	for _, check := range req.consistency {
		if err := eventstore.ValidateReadCriteria(check.Criteria); err != nil {
			return err
		}
		if len(check.Criteria) == 0 {
			return errors.New("consistency query has no criteria")
		}
		for _, criterion := range check.Criteria {
			if len(criterion.Tags) == 0 {
				return errors.New("consistency criterion has no tags")
			}
		}
	}
	return nil
}

func (s *PostgresSaveEvents) mapSaveError(err error) error {
	if code, _, ok := statuscode.FromError(err); ok && code != statuscode.Unknown {
		return err
	}
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
