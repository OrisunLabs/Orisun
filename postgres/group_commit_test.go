package postgres

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func TestPostgresGroupCommit_UnconditionalFastPath(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, container.container.Terminate(context.Background()))
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {Boundary: "test_boundary", Schema: "public"},
	}
	const requestCount = 32
	saver, err := NewPostgresSaveEventsWithConfig(
		t.Context(),
		db,
		logger,
		mapping,
		config.PostgresGroupCommitConfig{
			MaxBatchRequests: requestCount,
			MaxBatchEvents:   requestCount,
			MaxDelay:         250 * time.Millisecond,
		},
	)
	require.NoError(t, err)
	defer saver.close()

	type fastResult struct {
		eventID       string
		transactionID int64
		globalID      int64
		err           error
	}
	events := make([]orisun.EventWithMapTags, requestCount)
	for i := range events {
		events[i] = postgresGroupCommitEvent(t, fmt.Sprintf("Fast%02d", i), "fast-context")
	}
	start := make(chan struct{})
	results := make(chan fastResult, requestCount)
	var wg sync.WaitGroup
	for i := range events {
		event := events[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			transactionID, globalID, saveErr := saver.Save(
				context.Background(),
				[]orisun.EventWithMapTags{event},
				"test_boundary",
				nil,
				nil,
			)
			parsedTransactionID, parseErr := strconv.ParseInt(transactionID, 10, 64)
			if saveErr == nil && parseErr != nil {
				saveErr = parseErr
			}
			results <- fastResult{
				eventID:       event.EventId,
				transactionID: parsedTransactionID,
				globalID:      globalID,
				err:           saveErr,
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	returned := make(map[string]fastResult, requestCount)
	for result := range results {
		require.NoError(t, result.err)
		require.Equal(t, result.globalID+1, result.transactionID)
		returned[result.eventID] = result
	}
	require.Len(t, returned, requestCount)
	require.Equal(t, int64(1), saver.gc.fastFlushes.Load())

	rows, err := db.QueryContext(
		t.Context(),
		`SELECT event_id::text, transaction_id, global_id, pg_xact_id
		 FROM public.test_boundary_orisun_es_event
		 WHERE data->>'aggregate' = 'fast-context'
		 ORDER BY global_id`,
	)
	require.NoError(t, err)
	defer rows.Close()

	var (
		persisted     int
		batchPGXactID int64 = -1
		lastGlobalID  int64 = -1
	)
	for rows.Next() {
		var eventID string
		var transactionID, globalID, pgXactID int64
		require.NoError(t, rows.Scan(&eventID, &transactionID, &globalID, &pgXactID))
		result, ok := returned[eventID]
		require.True(t, ok, "unexpected persisted event %s", eventID)
		require.Equal(t, result.transactionID, transactionID)
		require.Equal(t, result.globalID, globalID)
		require.Greater(t, globalID, lastGlobalID)
		if batchPGXactID == -1 {
			batchPGXactID = pgXactID
		} else {
			require.Equal(t, batchPGXactID, pgXactID)
		}
		lastGlobalID = globalID
		persisted++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, requestCount, persisted)
	require.NoError(t, rows.Close())

	// A request that PostgreSQL would reject locally must force the isolated
	// path so it cannot poison an otherwise valid request in the same flush.
	saver.close()
	fallbackSaver, err := NewPostgresSaveEventsWithConfig(
		t.Context(),
		db,
		logger,
		mapping,
		config.PostgresGroupCommitConfig{
			MaxBatchRequests: 2,
			MaxBatchEvents:   2,
			MaxDelay:         250 * time.Millisecond,
		},
	)
	require.NoError(t, err)
	defer fallbackSaver.close()

	invalid := postgresGroupCommitEvent(t, "InvalidUUID", "fallback-context")
	invalid.EventId = "not-a-uuid"
	valid := postgresGroupCommitEvent(t, "ValidUUID", "fallback-context")
	fallbackStart := make(chan struct{})
	fallbackResults := make(chan fastResult, 2)
	for _, event := range []orisun.EventWithMapTags{invalid, valid} {
		event := event
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-fallbackStart
			transactionID, globalID, saveErr := fallbackSaver.Save(
				context.Background(),
				[]orisun.EventWithMapTags{event},
				"test_boundary",
				nil,
				nil,
			)
			parsedTransactionID, _ := strconv.ParseInt(transactionID, 10, 64)
			fallbackResults <- fastResult{
				eventID:       event.EventId,
				transactionID: parsedTransactionID,
				globalID:      globalID,
				err:           saveErr,
			}
		}()
	}
	close(fallbackStart)
	wg.Wait()
	close(fallbackResults)

	for result := range fallbackResults {
		if result.eventID == invalid.EventId {
			require.Equal(t, statuscode.Internal, statuscode.CodeOf(result.err))
			continue
		}
		require.NoError(t, result.err)
		require.Equal(t, result.globalID+1, result.transactionID)
	}
	require.Zero(t, fallbackSaver.gc.fastFlushes.Load())

	var fallbackPersisted int
	require.NoError(t, db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*) FROM public.test_boundary_orisun_es_event
		 WHERE data->>'aggregate' = 'fallback-context'`,
	).Scan(&fallbackPersisted))
	require.Equal(t, 1, fallbackPersisted)
}

func TestPostgresGroupCommit_UnconditionalFastPathMultipleEventsPerRequest(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, container.container.Terminate(context.Background()))
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	saver := NewPostgresSaveEvents(
		t.Context(),
		db,
		logger,
		map[string]config.BoundaryToPostgresSchemaMapping{
			"test_boundary": {Boundary: "test_boundary", Schema: "public"},
		},
	)
	defer saver.close()

	const aggregate = "unconditional-multi-request"
	eventCounts := []int{3, 1, 4}
	requests := make([]*postgresSaveRequest, 0, len(eventCounts))
	expectedEventTypes := make([]string, 0, 8)
	for requestIndex, eventCount := range eventCounts {
		events := make([]orisun.EventWithMapTags, 0, eventCount)
		for eventIndex := range eventCount {
			eventType := fmt.Sprintf("MultiRequest%02dEvent%02d", requestIndex, eventIndex)
			events = append(events, postgresGroupCommitEvent(t, eventType, aggregate))
			expectedEventTypes = append(expectedEventTypes, eventType)
		}
		prepared, prepareErr := orisun.PrepareEventsForSave(events)
		require.NoError(t, prepareErr)
		requests = append(requests, &postgresSaveRequest{
			ctx:    t.Context(),
			events: prepared,
		})
	}

	outcomes, err := saver.executeBatch(t.Context(), "test_boundary", requests)
	require.NoError(t, err)
	require.Len(t, outcomes, len(requests))
	require.Equal(t, int64(1), saver.gc.fastFlushes.Load())

	rows, err := db.QueryContext(
		t.Context(),
		`SELECT transaction_id, global_id, pg_xact_id, data->>'eventType'
		 FROM public.test_boundary_orisun_es_event
		 WHERE data->>'aggregate' = $1
		 ORDER BY global_id`,
		aggregate,
	)
	require.NoError(t, err)
	defer rows.Close()

	type persistedEvent struct {
		transactionID int64
		globalID      int64
		pgXactID      int64
		eventType     string
	}
	persisted := make([]persistedEvent, 0, len(expectedEventTypes))
	for rows.Next() {
		var event persistedEvent
		require.NoError(t, rows.Scan(
			&event.transactionID,
			&event.globalID,
			&event.pgXactID,
			&event.eventType,
		))
		persisted = append(persisted, event)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Len(t, persisted, len(expectedEventTypes))

	persistedEventTypes := make([]string, 0, len(persisted))
	eventOffset := 0
	batchPGXactID := persisted[0].pgXactID
	for requestIndex, eventCount := range eventCounts {
		outcome := outcomes[requestIndex]
		require.Same(t, requests[requestIndex], outcome.req)
		require.NoError(t, outcome.err)

		requestEvents := persisted[eventOffset : eventOffset+eventCount]
		lastEvent := requestEvents[len(requestEvents)-1]
		returnedTransactionID, parseErr := strconv.ParseInt(outcome.transactionID, 10, 64)
		require.NoError(t, parseErr)
		require.Equal(t, lastEvent.globalID, outcome.globalID)
		require.Equal(t, lastEvent.globalID+1, returnedTransactionID)

		for eventIndex, event := range requestEvents {
			require.Equal(t, returnedTransactionID, event.transactionID)
			require.Equal(t, batchPGXactID, event.pgXactID)
			if eventOffset+eventIndex > 0 {
				require.Equal(
					t,
					persisted[eventOffset+eventIndex-1].globalID+1,
					event.globalID,
				)
			}
			persistedEventTypes = append(persistedEventTypes, event.eventType)
		}
		eventOffset += eventCount
	}
	require.Equal(t, expectedEventTypes, persistedEventTypes)
}

func TestPostgresGroupCommit_InBatchWriteInvalidatesLaterCCCCheck(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, container.container.Terminate(context.Background()))
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("warn")
	require.NoError(t, err)
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {Boundary: "test_boundary", Schema: "public"},
	}

	seedSaver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	seedTransactionID, seedGlobalID, err := seedSaver.Save(
		t.Context(),
		[]orisun.EventWithMapTags{postgresGroupCommitEvent(t, "Seed", "same-context")},
		"test_boundary",
		nil,
		nil,
	)
	require.NoError(t, err)
	seedSaver.close()

	seedCommitPosition, err := strconv.ParseInt(seedTransactionID, 10, 64)
	require.NoError(t, err)
	expected := &orisun.Position{
		CommitPosition:  seedCommitPosition,
		PreparePosition: seedGlobalID,
	}
	query := &orisun.Query{Criteria: []*orisun.Criterion{{
		Tags: []*orisun.Tag{{Key: "aggregate", Value: "same-context"}},
	}}}

	saver, err := NewPostgresSaveEventsWithConfig(
		t.Context(),
		db,
		logger,
		mapping,
		config.PostgresGroupCommitConfig{
			MaxBatchRequests: 3,
			MaxBatchEvents:   3,
			MaxDelay:         250 * time.Millisecond,
		},
	)
	require.NoError(t, err)
	defer saver.close()

	var multiFlushes atomic.Int64
	saver.gc.testFlushHook = func(batchSize int) {
		if batchSize > 1 {
			multiFlushes.Add(1)
		}
	}

	type outcome struct {
		eventType string
		err       error
	}
	start := make(chan struct{})
	results := make(chan outcome, 3)
	var wg sync.WaitGroup
	events := []orisun.EventWithMapTags{
		postgresGroupCommitEvent(t, "First", "same-context"),
		postgresGroupCommitEvent(t, "Second", "same-context"),
		postgresGroupCommitEvent(t, "Independent", "independent-context"),
	}
	for _, event := range events {
		event := event
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			requestExpected := expected
			requestQuery := query
			if event.EventType == "Independent" {
				requestExpected = nil
				requestQuery = nil
			}
			_, _, saveErr := saver.Save(
				context.Background(),
				[]orisun.EventWithMapTags{event},
				"test_boundary",
				requestExpected,
				requestQuery,
			)
			results <- outcome{eventType: event.EventType, err: saveErr}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var succeeded, conflicted int
	for result := range results {
		switch statuscode.CodeOf(result.err) {
		case statuscode.OK:
			succeeded++
		case statuscode.AlreadyExists:
			conflicted++
		default:
			t.Fatalf("%s returned unexpected save result: %v", result.eventType, result.err)
		}
	}
	require.Equal(t, 2, succeeded)
	require.Equal(t, 1, conflicted)
	require.Equal(t, int64(1), multiFlushes.Load(), "all three requests must share one transaction")
	require.Equal(t, int64(1), saver.gc.canonicalFlushes.Load())

	var matchingEvents int
	err = db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*) FROM public.test_boundary_orisun_es_event WHERE data->>'aggregate' = 'same-context'`,
	).Scan(&matchingEvents)
	require.NoError(t, err)
	require.Equal(t, 2, matchingEvents, "seed plus exactly one competing write should persist")

	var transactionCount int
	err = db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(DISTINCT pg_xact_id)
		 FROM public.test_boundary_orisun_es_event
		 WHERE data->>'eventType' IN ('First', 'Second', 'Independent')`,
	).Scan(&transactionCount)
	require.NoError(t, err)
	require.Equal(t, 1, transactionCount, "successful requests in the flush must share one database transaction")
}

func TestPostgresSavePreparedValidatesEveryQueryObservation(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() { require.NoError(t, container.container.Terminate(context.Background())) }()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	saver := NewPostgresSaveEvents(t.Context(), db, logger, map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {Boundary: "test_boundary", Schema: "public"},
	})
	defer saver.close()

	accountTx, accountGID, err := saver.Save(t.Context(), []orisun.EventWithMapTags{
		postgresGroupCommitEvent(t, "AccountOpened", "account-a"),
	}, "test_boundary", nil, nil)
	require.NoError(t, err)
	customerTx, customerGID, err := saver.Save(t.Context(), []orisun.EventWithMapTags{
		postgresGroupCommitEvent(t, "CustomerOpened", "customer-a"),
	}, "test_boundary", nil, nil)
	require.NoError(t, err)
	accountCommit, err := strconv.ParseInt(accountTx, 10, 64)
	require.NoError(t, err)
	customerCommit, err := strconv.ParseInt(customerTx, 10, 64)
	require.NoError(t, err)

	checks := []orisun.ConsistencyCheck{
		{
			Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "aggregate", Value: "account-a"}}}},
			Position: orisun.Position{CommitPosition: accountCommit, PreparePosition: accountGID},
		},
		{
			Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "aggregate", Value: "customer-a"}}}},
			Position: orisun.Position{CommitPosition: customerCommit, PreparePosition: customerGID},
		},
		{
			Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "aggregate", Value: "missing"}}}},
			Position: orisun.NotExistsPosition(),
		},
	}
	prepared, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresGroupCommitEvent(t, "Decision", "unrelated"),
	})
	require.NoError(t, err)
	_, _, err = saver.SavePrepared(t.Context(), prepared, "test_boundary", checks)
	require.NoError(t, err)

	orCheck := orisun.ConsistencyCheck{
		Criteria: []orisun.ReadCriterion{
			{Tags: []orisun.ReadTag{{Key: "eventType", Value: "AccountOpened"}, {Key: "aggregate", Value: "account-a"}}},
			{Tags: []orisun.ReadTag{{Key: "eventType", Value: "CustomerOpened"}, {Key: "aggregate", Value: "customer-a"}}},
		},
		Position: checks[1].Position,
	}
	_, _, err = saver.SavePrepared(t.Context(), prepared, "test_boundary", []orisun.ConsistencyCheck{orCheck})
	require.NoError(t, err)

	accountTx, accountGID, err = saver.Save(t.Context(), []orisun.EventWithMapTags{
		postgresGroupCommitEvent(t, "AccountOpened", "account-a"),
	}, "test_boundary", nil, nil)
	require.NoError(t, err)
	accountCommit, err = strconv.ParseInt(accountTx, 10, 64)
	require.NoError(t, err)
	checks[0].Position = orisun.Position{CommitPosition: accountCommit, PreparePosition: accountGID}

	rejectedOR, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresGroupCommitEvent(t, "RejectedORFirst", "atomic-or-rejected"),
		postgresGroupCommitEvent(t, "RejectedORSecond", "atomic-or-rejected"),
	})
	require.NoError(t, err)
	_, _, err = saver.SavePrepared(t.Context(), rejectedOR, "test_boundary", []orisun.ConsistencyCheck{orCheck})
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
	var rejectedCount int
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM public.test_boundary_orisun_es_event WHERE data->>'aggregate' = 'atomic-or-rejected'`,
	).Scan(&rejectedCount))
	require.Zero(t, rejectedCount, "a stale OR observation must reject the complete event batch")

	_, _, err = saver.Save(t.Context(), []orisun.EventWithMapTags{
		postgresGroupCommitEvent(t, "CustomerUpdated", "customer-a"),
	}, "test_boundary", nil, nil)
	require.NoError(t, err)
	rejectedSecond, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresGroupCommitEvent(t, "RejectedSecondFirst", "atomic-second-rejected"),
		postgresGroupCommitEvent(t, "RejectedSecondSecond", "atomic-second-rejected"),
	})
	require.NoError(t, err)
	_, _, err = saver.SavePrepared(t.Context(), rejectedSecond, "test_boundary", checks)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM public.test_boundary_orisun_es_event WHERE data->>'aggregate' = 'atomic-second-rejected'`,
	).Scan(&rejectedCount))
	require.Zero(t, rejectedCount, "a stale later observation must reject the complete event batch")

	_, _, err = saver.Save(t.Context(), []orisun.EventWithMapTags{
		postgresGroupCommitEvent(t, "Appeared", "missing"),
	}, "test_boundary", nil, nil)
	require.NoError(t, err)
	_, _, err = saver.SavePrepared(t.Context(), prepared, "test_boundary", []orisun.ConsistencyCheck{checks[2]})
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))

	api := grpcapi.AdaptEventStore(orisun.NewEventStoreServer(
		nil, saver, nil, nil, nil, orisun.EventStreamConfig{}, logger,
	))
	rpcConsistency := []*grpcapi.ConsistencyObservation{
		{
			Query: &grpcapi.Query{Criteria: []*grpcapi.Criterion{{Tags: []*grpcapi.Tag{
				{Key: "aggregate", Value: "rpc-context"},
			}}}},
			Position: &grpcapi.Position{CommitPosition: -1, PreparePosition: -1},
		},
		{
			Query: &grpcapi.Query{Criteria: []*grpcapi.Criterion{{Tags: []*grpcapi.Tag{
				{Key: "aggregate", Value: "account-a"},
			}}}},
			Position: &grpcapi.Position{
				CommitPosition: checks[0].Position.CommitPosition, PreparePosition: checks[0].Position.PreparePosition,
			},
		},
	}
	rpcRequest := func() *grpcapi.SaveEventsV2Request {
		return &grpcapi.SaveEventsV2Request{
			Boundary: "test_boundary", Consistency: rpcConsistency,
			Events: []*grpcapi.EventToSave{{
				EventId: uuid.NewString(), EventType: "RPCWinner",
				Data: `{"aggregate":"rpc-context"}`, Metadata: `{}`,
			}},
		}
	}
	if _, err = api.SaveEventsV2(t.Context(), rpcRequest()); err != nil {
		t.Fatalf("SaveEventsV2 RPC backed by PostgreSQL: %v", err)
	}
	if _, err = api.SaveEventsV2(t.Context(), rpcRequest()); grpcstatus.Code(err) != codes.AlreadyExists {
		t.Fatalf("stale SaveEventsV2 RPC error = %v", err)
	}
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM public.test_boundary_orisun_es_event WHERE data->>'aggregate' = 'rpc-context'`,
	).Scan(&rejectedCount))
	require.Equal(t, 1, rejectedCount, "stale RPC retry must not persist")
}

func TestPostgresGroupCommit_CheckedBatchMatchesSequentialSemantics(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() { require.NoError(t, container.container.Terminate(context.Background())) }()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, RunDbScripts(db, "reference_boundary", "public", false, t.Context()))

	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	saver := NewPostgresSaveEvents(t.Context(), db, logger, map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary":      {Boundary: "test_boundary", Schema: "public"},
		"reference_boundary": {Boundary: "reference_boundary", Schema: "public"},
	})
	defer saver.close()

	seed := func(boundary, aggregate string) orisun.Position {
		tx, gid, saveErr := saver.Save(t.Context(), []orisun.EventWithMapTags{
			postgresGroupCommitEvent(t, "Seed", aggregate),
		}, boundary, nil, nil)
		require.NoError(t, saveErr)
		commit, parseErr := strconv.ParseInt(tx, 10, 64)
		require.NoError(t, parseErr)
		return orisun.Position{CommitPosition: commit, PreparePosition: gid}
	}
	a := seed("test_boundary", "a")
	b := seed("test_boundary", "b")
	require.Equal(t, a, seed("reference_boundary", "a"))
	require.Equal(t, b, seed("reference_boundary", "b"))

	check := func(position orisun.Position, aggregates ...string) orisun.ConsistencyCheck {
		criteria := make([]orisun.ReadCriterion, len(aggregates))
		for i, aggregate := range aggregates {
			criteria[i] = orisun.ReadCriterion{Tags: []orisun.ReadTag{{Key: "aggregate", Value: aggregate}}}
		}
		return orisun.ConsistencyCheck{Criteria: criteria, Position: position}
	}
	request := func(eventType, aggregate string, checks ...orisun.ConsistencyCheck) *postgresSaveRequest {
		prepared, prepareErr := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
			postgresGroupCommitEvent(t, eventType, aggregate),
		})
		require.NoError(t, prepareErr)
		return &postgresSaveRequest{ctx: t.Context(), events: prepared, consistency: checks}
	}

	// This matrix mixes multiple observations, an OR query, stale positions,
	// and two no-match observations invalidated by earlier accepted requests.
	requests := []*postgresSaveRequest{
		request("CreatesMissing", "missing", check(a, "a"), check(b, "b")),
		request("MissingMustRemainAbsent", "unrelated", check(orisun.NotExistsPosition(), "missing")),
		request("ReadsEitherSeed", "unrelated", check(b, "a", "b")),
		request("StaleSeed", "unrelated", check(orisun.NotExistsPosition(), "a")),
		request("CreatesOther", "other", check(orisun.NotExistsPosition(), "other")),
		request("OtherMustRemainAbsent", "unrelated", check(orisun.NotExistsPosition(), "other")),
	}

	batched, err := saver.executeBatch(t.Context(), "test_boundary", requests)
	require.NoError(t, err)
	require.Len(t, batched, len(requests))
	require.Equal(t, int64(1), saver.gc.canonicalFlushes.Load(), "multi-observation batch must use the set-based criterion-state path")

	sequential := make([]postgresBatchOutcome, 0, len(requests))
	for _, req := range requests {
		outcomes, executeErr := saver.executeBatch(t.Context(), "reference_boundary", []*postgresSaveRequest{req})
		require.NoError(t, executeErr)
		require.Len(t, outcomes, 1)
		sequential = append(sequential, outcomes[0])
	}

	for i := range requests {
		require.Equal(t, statuscode.CodeOf(sequential[i].err), statuscode.CodeOf(batched[i].err), "request %d status", i)
		require.Equal(t, sequential[i].transactionID, batched[i].transactionID, "request %d transaction", i)
		require.Equal(t, sequential[i].globalID, batched[i].globalID, "request %d global id", i)
	}

	var batchedEvents, sequentialEvents int
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM public.test_boundary_orisun_es_event`).Scan(&batchedEvents))
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM public.reference_boundary_orisun_es_event`).Scan(&sequentialEvents))
	require.Equal(t, sequentialEvents, batchedEvents)
	require.Equal(t, 5, batchedEvents, "two seeds plus three accepted requests")
}

func TestPostgresGroupCommit_TwoSaversContendOnSameContexts(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, container.container.Terminate(context.Background()))
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	secondDB, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer secondDB.Close()

	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {Boundary: "test_boundary", Schema: "public"},
	}
	groupCommitConfig := config.PostgresGroupCommitConfig{
		MaxBatchRequests: 64,
		MaxBatchEvents:   64,
		MaxDelay:         25 * time.Millisecond,
	}
	firstSaver, err := NewPostgresSaveEventsWithConfig(
		t.Context(),
		db,
		logger,
		mapping,
		groupCommitConfig,
	)
	require.NoError(t, err)
	defer firstSaver.close()
	secondSaver, err := NewPostgresSaveEventsWithConfig(
		t.Context(),
		secondDB,
		logger,
		mapping,
		groupCommitConfig,
	)
	require.NoError(t, err)
	defer secondSaver.close()
	guardTx, guardGID, err := firstSaver.Save(t.Context(), []orisun.EventWithMapTags{
		postgresIndependentCCCEvent(t, "Guard", "global-guard"),
	}, "test_boundary", nil, nil)
	require.NoError(t, err)
	guardCommit, err := strconv.ParseInt(guardTx, 10, 64)
	require.NoError(t, err)
	guardPosition := orisun.Position{CommitPosition: guardCommit, PreparePosition: guardGID}

	const (
		contextCount       = 32
		attemptsPerContext = 8
	)
	type contentionResult struct {
		contextValue string
		globalID     int64
		err          error
	}
	start := make(chan struct{})
	results := make(
		chan contentionResult,
		contextCount*attemptsPerContext,
	)
	var waitGroup sync.WaitGroup
	for contextIndex := range contextCount {
		contextValue := fmt.Sprintf("contention-%02d", contextIndex)
		for attempt := range attemptsPerContext {
			saver := firstSaver
			if attempt%2 == 1 {
				saver = secondSaver
			}
			event := postgresIndependentCCCEvent(
				t,
				"Contention",
				contextValue,
			)
			prepared, prepareErr := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{event})
			require.NoError(t, prepareErr)
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				<-start
				_, globalID, saveErr := saver.SavePrepared(
					context.Background(),
					prepared,
					"test_boundary",
					[]orisun.ConsistencyCheck{
						{
							Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "stream_id", Value: contextValue}}}},
							Position: orisun.NotExistsPosition(),
						},
						{
							Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "stream_id", Value: "global-guard"}}}},
							Position: guardPosition,
						},
					},
				)
				results <- contentionResult{
					contextValue: contextValue,
					globalID:     globalID,
					err:          saveErr,
				}
			}()
		}
	}
	close(start)
	waitGroup.Wait()
	close(results)

	successes := make(map[string]int, contextCount)
	globalIDs := make(map[int64]struct{}, contextCount)
	var conflicts int
	for result := range results {
		switch statuscode.CodeOf(result.err) {
		case statuscode.OK:
			successes[result.contextValue]++
			globalIDs[result.globalID] = struct{}{}
		case statuscode.AlreadyExists:
			conflicts++
		default:
			t.Fatalf(
				"context %s returned unexpected result: %v",
				result.contextValue,
				result.err,
			)
		}
	}
	require.Len(t, successes, contextCount)
	require.Len(t, globalIDs, contextCount)
	require.Equal(
		t,
		contextCount*(attemptsPerContext-1),
		conflicts,
	)
	for contextIndex := range contextCount {
		require.Equal(
			t,
			1,
			successes[fmt.Sprintf("contention-%02d", contextIndex)],
		)
	}

	var persistedCount, distinctContextCount, maxGlobalID int64
	require.NoError(t, db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*),
		        COUNT(DISTINCT data->>'stream_id'),
		        MAX(global_id)
		 FROM public.test_boundary_orisun_es_event
		 WHERE data->>'eventType' = 'Contention'`,
	).Scan(&persistedCount, &distinctContextCount, &maxGlobalID))
	require.Equal(t, int64(contextCount), persistedCount)
	require.Equal(t, int64(contextCount), distinctContextCount)
	require.Equal(t, int64(contextCount), maxGlobalID)
}

func TestPostgresGroupCommit_CancellationAndFlushTimeout(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, container.container.Terminate(context.Background()))
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {Boundary: "test_boundary", Schema: "public"},
	}
	saver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)

	blocker, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = blocker.ExecContext(
		t.Context(),
		`SELECT pg_advisory_xact_lock(hashtext('public.test_boundary::position_draw'))`,
	)
	require.NoError(t, err)

	flushStarted := make(chan struct{}, 1)
	saver.gc.testFlushHook = func(int) {
		select {
		case flushStarted <- struct{}{}:
		default:
		}
	}
	blockingEvent := postgresGroupCommitEvent(t, "Blocker", "cancellation-context")
	firstResult := make(chan error, 1)
	go func() {
		_, _, saveErr := saver.Save(
			context.Background(),
			[]orisun.EventWithMapTags{blockingEvent},
			"test_boundary",
			nil,
			nil,
		)
		firstResult <- saveErr
	}()
	<-flushStarted

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancelledEvent := postgresGroupCommitEvent(t, "Cancelled", "cancellation-context")
	cancelledResult := make(chan error, 1)
	go func() {
		_, _, saveErr := saver.Save(
			cancelledCtx,
			[]orisun.EventWithMapTags{cancelledEvent},
			"test_boundary",
			nil,
			nil,
		)
		cancelledResult <- saveErr
	}()
	require.Eventually(t, func() bool {
		saver.gc.enqueueMu.RLock()
		defer saver.gc.enqueueMu.RUnlock()
		return len(saver.gc.queues["test_boundary"]) == 1
	}, time.Second, time.Millisecond)
	cancel()
	require.Equal(t, statuscode.Canceled, statuscode.CodeOf(<-cancelledResult))

	require.NoError(t, blocker.Commit())
	require.NoError(t, <-firstResult)
	require.Eventually(t, func() bool {
		var count int
		err := db.QueryRowContext(
			t.Context(),
			`SELECT COUNT(*) FROM public.test_boundary_orisun_es_event
			 WHERE data->>'aggregate' = 'cancellation-context'`,
		).Scan(&count)
		return err == nil && count == 1
	}, time.Second, time.Millisecond)
	saver.close()

	timeoutSaver, err := NewPostgresSaveEventsWithConfig(
		t.Context(),
		db,
		logger,
		mapping,
		config.PostgresGroupCommitConfig{FlushTimeout: 100 * time.Millisecond},
	)
	require.NoError(t, err)
	defer timeoutSaver.close()

	timeoutBlocker, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = timeoutBlocker.ExecContext(
		t.Context(),
		`SELECT pg_advisory_xact_lock(hashtext('public.test_boundary::position_draw'))`,
	)
	require.NoError(t, err)

	timedOutEvent := postgresGroupCommitEvent(t, "TimedOut", "timeout-context")
	timeoutResult := make(chan error, 1)
	go func() {
		_, _, saveErr := timeoutSaver.Save(
			context.Background(),
			[]orisun.EventWithMapTags{timedOutEvent},
			"test_boundary",
			nil,
			nil,
		)
		timeoutResult <- saveErr
	}()
	require.Equal(t, statuscode.Internal, statuscode.CodeOf(<-timeoutResult))
	require.NoError(t, timeoutBlocker.Rollback())

	_, followupGlobalID, err := timeoutSaver.Save(
		t.Context(),
		[]orisun.EventWithMapTags{postgresGroupCommitEvent(t, "AfterTimeout", "timeout-context")},
		"test_boundary",
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), followupGlobalID, "timed-out lock wait must not allocate a position")
}

func TestNormalizePostgresGroupCommitConfig(t *testing.T) {
	cfg, err := normalizePostgresGroupCommitConfig(config.PostgresGroupCommitConfig{})
	require.NoError(t, err)
	require.Equal(t, postgresGroupCommitMaxBatchRequests, cfg.MaxBatchRequests)
	require.Equal(t, postgresGroupCommitMaxBatchEvents, cfg.MaxBatchEvents)
	require.Equal(t, postgresGroupCommitMaxPending, cfg.MaxPending)
	require.Equal(t, postgresGroupCommitFlushTimeout, cfg.FlushTimeout)

	_, err = normalizePostgresGroupCommitConfig(config.PostgresGroupCommitConfig{MaxBatchRequests: -1})
	require.ErrorContains(t, err, "maxBatchRequests")
	_, err = normalizePostgresGroupCommitConfig(config.PostgresGroupCommitConfig{MaxDelay: -time.Nanosecond})
	require.ErrorContains(t, err, "maxDelay")
}

func TestPostgresGroupCommit_ShutdownRejectsNewSaves(t *testing.T) {
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	saver := NewPostgresSaveEvents(
		context.Background(),
		nil,
		logger,
		map[string]config.BoundaryToPostgresSchemaMapping{
			"test_boundary": {Boundary: "test_boundary", Schema: "public"},
		},
	)
	saver.close()

	_, _, err = saver.Save(
		t.Context(),
		[]orisun.EventWithMapTags{postgresGroupCommitEvent(t, "AfterClose", "closed")},
		"test_boundary",
		nil,
		nil,
	)
	require.Equal(t, statuscode.Unavailable, statuscode.CodeOf(err))
}

func TestCanUseUnconditionalFastPathRejectsUnsafeShapes(t *testing.T) {
	valid := orisun.PreparedEventBatch{{
		EventId:      uuid.Must(uuid.NewV7()).String(),
		EventType:    "Valid",
		DataJSON:     `{"eventType":"Valid"}`,
		MetadataJSON: `{}`,
	}}
	require.True(t, canUseUnconditionalFastPath([]*postgresSaveRequest{{events: valid}}))
	require.False(t, canUseUnconditionalFastPath([]*postgresSaveRequest{{
		events: valid,
		consistency: []orisun.ConsistencyCheck{{
			Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "id", Value: "1"}}}},
			Position: orisun.NotExistsPosition(),
		}},
	}}))

	multipleEvents := append(orisun.PreparedEventBatch(nil), valid...)
	multipleEvents = append(multipleEvents, valid[0])
	multipleEvents[1].EventId = uuid.Must(uuid.NewV7()).String()
	require.True(t, canUseUnconditionalFastPath([]*postgresSaveRequest{{events: multipleEvents}}))

	invalidSecondEvent := append(orisun.PreparedEventBatch(nil), multipleEvents...)
	invalidSecondEvent[1].EventId = "not-a-uuid"
	require.False(t, canUseUnconditionalFastPath([]*postgresSaveRequest{{events: invalidSecondEvent}}))

	invalidUUID := append(orisun.PreparedEventBatch(nil), valid...)
	invalidUUID[0].EventId = "not-a-uuid"
	require.False(t, canUseUnconditionalFastPath([]*postgresSaveRequest{{events: invalidUUID}}))

	require.False(t, canUseUnconditionalFastPath([]*postgresSaveRequest{{}}))
}

func TestPostgresGroupCommit_IndependentCCCFastPathV2(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() { require.NoError(t, container.container.Terminate(context.Background())) }()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary": {Boundary: "test_boundary", Schema: "public"},
	}
	seedSaver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	seedPosition := func(contextValue string) orisun.Position {
		transactionID, globalID, saveErr := seedSaver.Save(
			t.Context(),
			[]orisun.EventWithMapTags{postgresIndependentCCCEvent(t, "Seed", contextValue)},
			"test_boundary",
			nil,
			nil,
		)
		require.NoError(t, saveErr)
		commitPosition, parseErr := strconv.ParseInt(transactionID, 10, 64)
		require.NoError(t, parseErr)
		return orisun.Position{CommitPosition: commitPosition, PreparePosition: globalID}
	}
	betaPosition := seedPosition("beta")
	_ = seedPosition("gamma")
	seedSaver.close()

	saver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	defer saver.close()
	request := func(contextValue string, position orisun.Position) *postgresSaveRequest {
		prepared, prepareErr := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
			postgresIndependentCCCEvent(t, "IndependentV2", contextValue),
		})
		require.NoError(t, prepareErr)
		return &postgresSaveRequest{
			ctx:    t.Context(),
			events: prepared,
			consistency: []orisun.ConsistencyCheck{{
				Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "stream_id", Value: contextValue}}}},
				Position: position,
			}},
		}
	}
	requests := []*postgresSaveRequest{
		request("alpha", orisun.NotExistsPosition()),
		request("beta", betaPosition),
		request("gamma", orisun.NotExistsPosition()),
		request("delta", orisun.NotExistsPosition()),
	}

	outcomes, err := saver.executeBatch(t.Context(), "test_boundary", requests)
	require.NoError(t, err)
	require.Len(t, outcomes, len(requests))
	require.NoError(t, outcomes[0].err)
	require.NoError(t, outcomes[1].err)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(outcomes[2].err))
	require.NoError(t, outcomes[3].err)
	require.Equal(t, int64(1), saver.gc.independentFlushes.Load())
	require.Zero(t, saver.gc.canonicalFlushes.Load())
	require.Zero(t, saver.gc.fastFlushes.Load())

	var persisted, transactions int
	require.NoError(t, db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*), COUNT(DISTINCT pg_xact_id)
		 FROM public.test_boundary_orisun_es_event
		 WHERE data->>'eventType' = 'IndependentV2'`,
	).Scan(&persisted, &transactions))
	require.Equal(t, 3, persisted, "the stale request must not persist")
	require.Equal(t, 1, transactions, "accepted requests must share one database transaction")
}

func TestPostgresGroupCommitFastPathSelectorsSupportSaveEventsV2Shapes(t *testing.T) {
	prepared := func(contextValue string) orisun.PreparedEventBatch {
		batch, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
			postgresIndependentCCCEvent(t, "Selector", contextValue),
		})
		require.NoError(t, err)
		return batch
	}
	check := func(position orisun.Position, values ...string) orisun.ConsistencyCheck {
		criteria := make([]orisun.ReadCriterion, len(values))
		for index, value := range values {
			criteria[index] = orisun.ReadCriterion{Tags: []orisun.ReadTag{{Key: "stream_id", Value: value}}}
		}
		return orisun.ConsistencyCheck{Criteria: criteria, Position: position}
	}

	independent := []*postgresSaveRequest{
		{events: prepared("a"), consistency: []orisun.ConsistencyCheck{check(orisun.NotExistsPosition(), "a")}},
		{events: prepared("b"), consistency: []orisun.ConsistencyCheck{check(orisun.NotExistsPosition(), "b")}},
	}
	key, ok := independentCCCKey(independent)
	require.True(t, ok)
	require.Equal(t, "stream_id", key)
	require.True(t, canUseCanonicalFastPath(independent))

	duplicateContext := []*postgresSaveRequest{independent[0], {
		events: prepared("a"), consistency: []orisun.ConsistencyCheck{check(orisun.NotExistsPosition(), "a")},
	}}
	_, ok = independentCCCKey(duplicateContext)
	require.False(t, ok)

	multipleObservations := []*postgresSaveRequest{{
		events: prepared("a"),
		consistency: []orisun.ConsistencyCheck{
			check(orisun.NotExistsPosition(), "a"),
			check(orisun.NotExistsPosition(), "guard"),
		},
	}}
	_, ok = independentCCCKey(multipleObservations)
	require.False(t, ok)
	require.True(t, canUseCanonicalFastPath(multipleObservations))

	orQuery := []*postgresSaveRequest{{
		events:      prepared("a"),
		consistency: []orisun.ConsistencyCheck{check(orisun.NotExistsPosition(), "a", "b")},
	}}
	_, ok = independentCCCKey(orQuery)
	require.False(t, ok)
	require.True(t, canUseCanonicalFastPath(orQuery))

	eventOutsideContext := []*postgresSaveRequest{{
		events: prepared("different"), consistency: []orisun.ConsistencyCheck{check(orisun.NotExistsPosition(), "a")},
	}}
	_, ok = independentCCCKey(eventOutsideContext)
	require.False(t, ok)

	require.True(t, canUseCanonicalFastPath([]*postgresSaveRequest{{events: prepared("queryless")}, multipleObservations[0]}))
	require.False(t, canUseCanonicalFastPath([]*postgresSaveRequest{{
		events: prepared("a"), consistency: []orisun.ConsistencyCheck{{Position: orisun.NotExistsPosition()}},
	}}))
	require.False(t, canUseCanonicalFastPath([]*postgresSaveRequest{{
		events: prepared("a"), consistency: []orisun.ConsistencyCheck{{
			Criteria: []orisun.ReadCriterion{{}}, Position: orisun.NotExistsPosition(),
		}},
	}}))
}

func postgresGroupCommitEvent(t *testing.T, eventType, aggregate string) orisun.EventWithMapTags {
	t.Helper()
	eventID, err := uuid.NewV7()
	require.NoError(t, err)
	return orisun.EventWithMapTags{
		EventId:   eventID.String(),
		EventType: eventType,
		Data:      `{"aggregate":"` + aggregate + `"}`,
		Metadata:  `{}`,
	}
}

func postgresIndependentCCCEvent(t *testing.T, eventType, contextValue string) orisun.EventWithMapTags {
	t.Helper()
	eventID, err := uuid.NewV7()
	require.NoError(t, err)
	return orisun.EventWithMapTags{
		EventId:   eventID.String(),
		EventType: eventType,
		Data:      `{"stream_id":"` + contextValue + `"}`,
		Metadata:  `{}`,
	}
}
