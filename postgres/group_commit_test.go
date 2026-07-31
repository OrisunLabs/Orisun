package postgres

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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
	require.Zero(t, saver.gc.canonicalFlushes.Load())

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

func TestPostgresGroupCommit_IndependentCCCFastPath(t *testing.T) {
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

	seedSaver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	seedPosition := make(map[string]orisun.Position, 2)
	for _, contextValue := range []string{"fast-b", "fast-c"} {
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
		seedPosition[contextValue] = orisun.Position{
			CommitPosition:  commitPosition,
			PreparePosition: globalID,
		}
	}
	seedSaver.close()

	const requestCount = 4
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

	type result struct {
		contextValue  string
		transactionID string
		globalID      int64
		err           error
	}
	contexts := []string{"fast-a", "fast-b", "fast-c", "fast-d"}
	start := make(chan struct{})
	results := make(chan result, requestCount)
	var wg sync.WaitGroup
	for _, contextValue := range contexts {
		contextValue := contextValue
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			expected := orisun.NotExistsPosition()
			if contextValue == "fast-b" {
				expected = seedPosition[contextValue]
			}
			transactionID, globalID, saveErr := saver.Save(
				context.Background(),
				[]orisun.EventWithMapTags{
					postgresIndependentCCCEvent(t, "IndependentCCC", contextValue),
				},
				"test_boundary",
				&expected,
				postgresIndependentCCCQuery("stream_id", contextValue),
			)
			results <- result{
				contextValue:  contextValue,
				transactionID: transactionID,
				globalID:      globalID,
				err:           saveErr,
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	for saveResult := range results {
		if saveResult.contextValue == "fast-c" {
			require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(saveResult.err))
			continue
		}
		require.NoError(t, saveResult.err)
		commitPosition, parseErr := strconv.ParseInt(saveResult.transactionID, 10, 64)
		require.NoError(t, parseErr)
		require.Equal(t, saveResult.globalID+1, commitPosition)
	}
	require.Equal(t, int64(1), saver.gc.independentFlushes.Load())
	require.Zero(t, saver.gc.canonicalFlushes.Load())

	var eventCount, maxGlobalID int64
	err = db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*), MAX(global_id) FROM public.test_boundary_orisun_es_event`,
	).Scan(&eventCount, &maxGlobalID)
	require.NoError(t, err)
	require.Equal(t, int64(5), eventCount)
	require.Equal(t, int64(4), maxGlobalID, "a rejected context must not consume a global ID")

	var physicalTransactions int
	err = db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(DISTINCT pg_xact_id)
		 FROM public.test_boundary_orisun_es_event
		 WHERE data->>'eventType' = 'IndependentCCC'`,
	).Scan(&physicalTransactions)
	require.NoError(t, err)
	require.Equal(t, 1, physicalTransactions)

	multiPrepared, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresIndependentCCCEvent(t, "IndependentMultiFirst", "fast-multi"),
		postgresIndependentCCCEvent(t, "IndependentMultiSecond", "fast-multi"),
	})
	require.NoError(t, err)
	multiExpected := orisun.NotExistsPosition()
	multiOutcomes, err := saver.executeBatch(t.Context(), "test_boundary", []*postgresSaveRequest{{
		ctx:      t.Context(),
		events:   multiPrepared,
		expected: &multiExpected,
		query:    postgresIndependentCCCQuery("stream_id", "fast-multi"),
	}})
	require.NoError(t, err)
	require.Len(t, multiOutcomes, 1)
	require.NoError(t, multiOutcomes[0].err)
	multiTransactionID, err := strconv.ParseInt(multiOutcomes[0].transactionID, 10, 64)
	require.NoError(t, err)
	require.Equal(t, multiOutcomes[0].globalID+1, multiTransactionID)
	require.Equal(t, int64(2), saver.gc.independentFlushes.Load())

	rows, err := db.QueryContext(
		t.Context(),
		`SELECT transaction_id, global_id, data->>'eventType'
		 FROM public.test_boundary_orisun_es_event
		 WHERE data->>'stream_id' = 'fast-multi'
		 ORDER BY global_id`,
	)
	require.NoError(t, err)
	defer rows.Close()
	var multiPositions [][2]int64
	var multiEventTypes []string
	for rows.Next() {
		var transactionID, globalID int64
		var eventType string
		require.NoError(t, rows.Scan(&transactionID, &globalID, &eventType))
		multiPositions = append(multiPositions, [2]int64{transactionID, globalID})
		multiEventTypes = append(multiEventTypes, eventType)
	}
	require.NoError(t, rows.Err())
	require.Len(t, multiPositions, 2)
	require.Equal(t, multiTransactionID, multiPositions[0][0])
	require.Equal(t, multiTransactionID, multiPositions[1][0])
	require.Equal(t, multiPositions[0][1]+1, multiPositions[1][1])
	require.Equal(t, multiOutcomes[0].globalID, multiPositions[1][1])
	require.Equal(t, []string{
		"IndependentMultiFirst",
		"IndependentMultiSecond",
	}, multiEventTypes)
	require.NoError(t, rows.Close())

	// Event A queries cross-a but writes cross-b. That could invalidate request
	// B, so this shape must bypass the independent path and run in queue order.
	crossExpected := orisun.NotExistsPosition()
	crossA, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresIndependentCCCEvent(t, "CrossA", "cross-b"),
	})
	require.NoError(t, err)
	crossB, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresIndependentCCCEvent(t, "CrossB", "cross-b"),
	})
	require.NoError(t, err)
	crossOutcomes, err := saver.executeBatch(t.Context(), "test_boundary", []*postgresSaveRequest{
		{
			ctx:      t.Context(),
			events:   crossA,
			expected: &crossExpected,
			query:    postgresIndependentCCCQuery("stream_id", "cross-a"),
		},
		{
			ctx:      t.Context(),
			events:   crossB,
			expected: &crossExpected,
			query:    postgresIndependentCCCQuery("stream_id", "cross-b"),
		},
	})
	require.NoError(t, err)
	require.Len(t, crossOutcomes, 2)
	require.NoError(t, crossOutcomes[0].err)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(crossOutcomes[1].err))
	require.Equal(t, int64(2), saver.gc.independentFlushes.Load())
	require.Equal(t, int64(1), saver.gc.canonicalFlushes.Load())

	// The set-based snapshot must be taken only after the position lock is
	// acquired. Insert lock-a while this batch waits; lock-a must conflict
	// after waking while the independent lock-b context still succeeds.
	blocker, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer blocker.Rollback()
	_, err = blocker.ExecContext(
		t.Context(),
		`SELECT pg_advisory_xact_lock(hashtext('public.test_boundary::position_draw'))`,
	)
	require.NoError(t, err)

	lockA, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresIndependentCCCEvent(t, "WaitingLockA", "lock-a"),
	})
	require.NoError(t, err)
	lockB, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresIndependentCCCEvent(t, "WaitingLockB", "lock-b"),
	})
	require.NoError(t, err)
	type batchExecution struct {
		outcomes []postgresBatchOutcome
		err      error
	}
	waitingBatch := make(chan batchExecution, 1)
	go func() {
		outcomes, executeErr := saver.executeBatch(t.Context(), "test_boundary", []*postgresSaveRequest{
			{
				ctx:      t.Context(),
				events:   lockA,
				expected: &crossExpected,
				query:    postgresIndependentCCCQuery("stream_id", "lock-a"),
			},
			{
				ctx:      t.Context(),
				events:   lockB,
				expected: &crossExpected,
				query:    postgresIndependentCCCQuery("stream_id", "lock-b"),
			},
		})
		waitingBatch <- batchExecution{outcomes: outcomes, err: executeErr}
	}()

	require.Eventually(t, func() bool {
		var waiting bool
		waitErr := db.QueryRowContext(t.Context(), `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE pid <> pg_backend_pid()
				  AND wait_event_type = 'Lock'
				  AND wait_event = 'advisory'
				  AND query LIKE '%insert_independent_event_requests_with_consistency_v1%'
			)
		`).Scan(&waiting)
		return waitErr == nil && waiting
	}, 3*time.Second, 10*time.Millisecond)

	invalidating, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresIndependentCCCEvent(t, "InvalidatingLockA", "lock-a"),
	})
	require.NoError(t, err)
	invalidatingJSON, err := json.Marshal(invalidating)
	require.NoError(t, err)
	var insertedGlobalID, insertedTransactionID, insertedPositionGlobalID int64
	err = blocker.QueryRowContext(
		t.Context(),
		fmt.Sprintf(insertEventsWithConsistency, "public"),
		"test_boundary",
		"public",
		[]byte(`{}`),
		invalidatingJSON,
	).Scan(&insertedGlobalID, &insertedTransactionID, &insertedPositionGlobalID)
	require.NoError(t, err)
	require.NoError(t, blocker.Commit())

	execution := <-waitingBatch
	require.NoError(t, execution.err)
	require.Len(t, execution.outcomes, 2)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(execution.outcomes[0].err))
	require.NoError(t, execution.outcomes[1].err)
	require.Equal(t, int64(3), saver.gc.independentFlushes.Load())
}

func TestPostgresGroupCommit_GeneralCriterionStateResolver(t *testing.T) {
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
	defer saver.close()

	prepare := func(eventType, data string) orisun.PreparedEventBatch {
		prepared, prepareErr := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{
			EventId:   uuid.NewString(),
			EventType: eventType,
			Data:      data,
			Metadata:  `{}`,
		}})
		require.NoError(t, prepareErr)
		return prepared
	}
	query := func(criteria ...map[string]string) *orisun.Query {
		result := &orisun.Query{Criteria: make([]*orisun.Criterion, 0, len(criteria))}
		for _, tags := range criteria {
			criterion := &orisun.Criterion{Tags: make([]*orisun.Tag, 0, len(tags))}
			for key, value := range tags {
				criterion.Tags = append(criterion.Tags, &orisun.Tag{Key: key, Value: value})
			}
			result.Criteria = append(result.Criteria, criterion)
		}
		return result
	}
	notExists := orisun.NotExistsPosition()

	// Request 0 updates the second OR arm of request 1. Request 2 then
	// invalidates request 3 through a completely different criterion key.
	outcomes, err := saver.executeBatch(t.Context(), "test_boundary", []*postgresSaveRequest{
		{
			ctx:      t.Context(),
			events:   prepare("GeneralAND", `{"account":"general-a","kind":"credit","customer":"customer-a"}`),
			expected: &notExists,
			query: query(map[string]string{
				"account": "general-a",
				"kind":    "credit",
			}),
		},
		{
			ctx:      t.Context(),
			events:   prepare("GeneralORConflict", `{"ignored":"or-conflict"}`),
			expected: &notExists,
			query: query(
				map[string]string{"user": "user-never"},
				map[string]string{"customer": "customer-a"},
			),
		},
		{
			ctx:      t.Context(),
			events:   prepare("GeneralMixedKey", `{"user":"user-a","account":"general-b"}`),
			expected: &notExists,
			query:    query(map[string]string{"user": "user-a"}),
		},
		{
			ctx:      t.Context(),
			events:   prepare("GeneralMixedKeyConflict", `{"ignored":"mixed-conflict"}`),
			expected: &notExists,
			query:    query(map[string]string{"account": "general-b"}),
		},
	})
	require.NoError(t, err)
	require.Len(t, outcomes, 4)
	require.NoError(t, outcomes[0].err)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(outcomes[1].err))
	require.NoError(t, outcomes[2].err)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(outcomes[3].err))
	require.Equal(t, int64(1), saver.gc.canonicalFlushes.Load())
	require.Zero(t, saver.gc.independentFlushes.Load())

	var count, maxGlobalID int64
	err = db.QueryRowContext(
		t.Context(),
		`SELECT COUNT(*), MAX(global_id) FROM public.test_boundary_orisun_es_event`,
	).Scan(&count, &maxGlobalID)
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	require.Equal(t, int64(1), maxGlobalID)

	// A query-less save is unconditional, but its event must still advance every
	// matching criterion used by later requests in the same batch.
	mixedOutcomes, err := saver.executeBatch(t.Context(), "test_boundary", []*postgresSaveRequest{
		{
			ctx:    t.Context(),
			events: prepare("GeneralUnconditional", `{"project":"project-a"}`),
		},
		{
			ctx:      t.Context(),
			events:   prepare("GeneralAfterUnconditional", `{"project":"project-a"}`),
			expected: &notExists,
			query:    query(map[string]string{"project": "project-a"}),
		},
	})
	require.NoError(t, err)
	require.Len(t, mixedOutcomes, 2)
	require.NoError(t, mixedOutcomes[0].err)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(mixedOutcomes[1].err))
	require.Equal(t, int64(2), saver.gc.canonicalFlushes.Load())

	// Initial state is also resolved per criterion shape. An OR query whose
	// second, multi-tag arm matches stored history must accept the exact stored
	// position even when its first arm is empty.
	seedTransactionID, seedGlobalID, err := saver.Save(
		t.Context(),
		[]orisun.EventWithMapTags{{
			EventId:   uuid.NewString(),
			EventType: "GeneralStoredSeed",
			Data:      `{"account":"stored-a","kind":"credit"}`,
			Metadata:  `{}`,
		}},
		"test_boundary",
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), saver.gc.fastFlushes.Load(), "a single request must use executeBatch path selection")
	seedCommitPosition, err := strconv.ParseInt(seedTransactionID, 10, 64)
	require.NoError(t, err)
	seedPosition := orisun.Position{
		CommitPosition:  seedCommitPosition,
		PreparePosition: seedGlobalID,
	}
	storedOutcomes, err := saver.executeBatch(t.Context(), "test_boundary", []*postgresSaveRequest{{
		ctx:      t.Context(),
		events:   prepare("GeneralStoredOR", `{"result":"stored-or"}`),
		expected: &seedPosition,
		query: query(
			map[string]string{"missing": "never"},
			map[string]string{"account": "stored-a", "kind": "credit"},
		),
	}})
	require.NoError(t, err)
	require.Len(t, storedOutcomes, 1)
	require.NoError(t, storedOutcomes[0].err)

	// An explicitly empty query means the whole boundary. It must use the
	// latest accepted position, including writes made through the resolver.
	globalExpected := orisun.Position{
		CommitPosition:  storedOutcomes[0].globalID + 1,
		PreparePosition: storedOutcomes[0].globalID,
	}
	globalOutcomes, err := saver.executeBatch(t.Context(), "test_boundary", []*postgresSaveRequest{{
		ctx:      t.Context(),
		events:   prepare("GeneralGlobal", `{"result":"global"}`),
		expected: &globalExpected,
		query:    &orisun.Query{},
	}})
	require.NoError(t, err)
	require.Len(t, globalOutcomes, 1)
	require.NoError(t, globalOutcomes[0].err)

	// A multi-event request has one shared transaction position, while each
	// criterion advances to the highest event in that request that matched it.
	// Conflicted requests between accepted ones must not consume sequence IDs.
	var sequenceLast int64
	require.NoError(t, db.QueryRowContext(
		t.Context(),
		`SELECT last_value
		 FROM public.test_boundary_orisun_es_event_global_id_seq`,
	).Scan(&sequenceLast))
	firstMultiGlobalID := sequenceLast + 1
	secondMultiGlobalID := sequenceLast + 2
	multiTransactionID := secondMultiGlobalID + 1
	phasePosition := orisun.Position{
		CommitPosition:  multiTransactionID,
		PreparePosition: firstMultiGlobalID,
	}
	projectPosition := orisun.Position{
		CommitPosition:  multiTransactionID,
		PreparePosition: secondMultiGlobalID,
	}
	multiEvents, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		{
			EventId:   uuid.NewString(),
			EventType: "GeneralMultiStart",
			Data:      `{"account":"multi-a","phase":"start","multi_group":"a"}`,
			Metadata:  `{}`,
		},
		{
			EventId:   uuid.NewString(),
			EventType: "GeneralMultiProject",
			Data:      `{"account":"multi-a","project":"multi-p","multi_group":"a"}`,
			Metadata:  `{}`,
		},
	})
	require.NoError(t, err)
	canonicalFlushesBefore := saver.gc.canonicalFlushes.Load()
	multiOutcomes, err := saver.executeBatch(t.Context(), "test_boundary", []*postgresSaveRequest{
		{
			ctx:      t.Context(),
			events:   multiEvents,
			expected: &notExists,
			query:    query(map[string]string{"account": "multi-a"}),
		},
		{
			ctx:      t.Context(),
			events:   prepare("GeneralMultiPhaseConflict", `{"ignored":"phase-conflict"}`),
			expected: &notExists,
			query:    query(map[string]string{"phase": "start"}),
		},
		{
			ctx:      t.Context(),
			events:   prepare("GeneralMultiProjectConflict", `{"ignored":"project-conflict"}`),
			expected: &notExists,
			query:    query(map[string]string{"project": "multi-p"}),
		},
		{
			ctx:      t.Context(),
			events:   prepare("GeneralMultiPhaseExact", `{"accepted":"phase-position"}`),
			expected: &phasePosition,
			query:    query(map[string]string{"phase": "start"}),
		},
		{
			ctx:      t.Context(),
			events:   prepare("GeneralMultiProjectExact", `{"accepted":"project-position"}`),
			expected: &projectPosition,
			query:    query(map[string]string{"project": "multi-p"}),
		},
	})
	require.NoError(t, err)
	require.Len(t, multiOutcomes, 5)
	require.NoError(t, multiOutcomes[0].err)
	require.Equal(t, strconv.FormatInt(multiTransactionID, 10), multiOutcomes[0].transactionID)
	require.Equal(t, secondMultiGlobalID, multiOutcomes[0].globalID)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(multiOutcomes[1].err))
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(multiOutcomes[2].err))
	require.NoError(t, multiOutcomes[3].err)
	require.Equal(t, secondMultiGlobalID+1, multiOutcomes[3].globalID)
	require.NoError(t, multiOutcomes[4].err)
	require.Equal(t, secondMultiGlobalID+2, multiOutcomes[4].globalID)
	require.Equal(t, canonicalFlushesBefore+1, saver.gc.canonicalFlushes.Load())

	rows, err := db.QueryContext(
		t.Context(),
		`SELECT transaction_id, global_id
		 FROM public.test_boundary_orisun_es_event
		 WHERE data->>'multi_group' = 'a'
		 ORDER BY global_id`,
	)
	require.NoError(t, err)
	defer rows.Close()
	var persistedMultiPositions [][2]int64
	for rows.Next() {
		var transactionID, globalID int64
		require.NoError(t, rows.Scan(&transactionID, &globalID))
		persistedMultiPositions = append(
			persistedMultiPositions,
			[2]int64{transactionID, globalID},
		)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, [][2]int64{
		{multiTransactionID, firstMultiGlobalID},
		{multiTransactionID, secondMultiGlobalID},
	}, persistedMultiPositions)

	// Single queried requests also pass through executeBatch and select the
	// general criterion-state resolver rather than the removed legacy branch.
	canonicalFlushesBefore = saver.gc.canonicalFlushes.Load()
	singleExpected := orisun.NotExistsPosition()
	_, _, err = saver.Save(
		t.Context(),
		[]orisun.EventWithMapTags{{
			EventId:   uuid.NewString(),
			EventType: "GeneralSingleQueried",
			Data:      `{"single":"queried","kind":"general"}`,
			Metadata:  `{}`,
		}},
		"test_boundary",
		&singleExpected,
		query(map[string]string{"single": "queried", "kind": "general"}),
	)
	require.NoError(t, err)
	require.Equal(t, canonicalFlushesBefore+1, saver.gc.canonicalFlushes.Load())
}

func TestPostgresGroupCommit_GeneralCriterionStateResolver_MultipleMultiEventRequests(t *testing.T) {
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
	defer saver.close()

	event := func(eventType, data string) orisun.EventWithMapTags {
		return orisun.EventWithMapTags{
			EventId:   uuid.NewString(),
			EventType: eventType,
			Data:      data,
			Metadata:  `{}`,
		}
	}
	prepare := func(events ...orisun.EventWithMapTags) orisun.PreparedEventBatch {
		prepared, prepareErr := orisun.PrepareEventsForSave(events)
		require.NoError(t, prepareErr)
		return prepared
	}
	query := func(criteria ...map[string]string) *orisun.Query {
		result := &orisun.Query{Criteria: make([]*orisun.Criterion, 0, len(criteria))}
		for _, tags := range criteria {
			criterion := &orisun.Criterion{Tags: make([]*orisun.Tag, 0, len(tags))}
			for key, value := range tags {
				criterion.Tags = append(criterion.Tags, &orisun.Tag{Key: key, Value: value})
			}
			result.Criteria = append(result.Criteria, criterion)
		}
		return result
	}

	var sequenceLast int64
	var sequenceCalled bool
	require.NoError(t, db.QueryRowContext(
		t.Context(),
		`SELECT last_value, is_called
		 FROM public.test_boundary_orisun_es_event_global_id_seq`,
	).Scan(&sequenceLast, &sequenceCalled))
	firstGlobalID := sequenceLast
	if sequenceCalled {
		firstGlobalID++
	}

	// Request 0 consumes two IDs. Requests 1, 3, and 4 conflict and consume
	// none. Request 2 then consumes three IDs, so its account/region criterion
	// points at its middle event rather than its final event.
	request2TransactionID := firstGlobalID + 5
	request2AccountPosition := orisun.Position{
		CommitPosition:  request2TransactionID,
		PreparePosition: firstGlobalID + 3,
	}
	notExists := orisun.NotExistsPosition()
	outcomes, err := saver.executeBatch(t.Context(), "test_boundary", []*postgresSaveRequest{
		{
			ctx: t.Context(),
			events: prepare(
				event("MultiR0Opened", `{"account":"a","phase":"opened","batch_request":"r0"}`),
				event("MultiR0Customer", `{"customer":"c1","project":"p1","batch_request":"r0"}`),
			),
			expected: &notExists,
			query: query(map[string]string{
				"account": "a",
				"phase":   "opened",
			}),
		},
		{
			ctx: t.Context(),
			events: prepare(
				event("MultiR1RejectedA", `{"user":"u2","kind":"credit","batch_request":"r1"}`),
				event("MultiR1RejectedB", `{"user":"u2","kind":"credit","batch_request":"r1"}`),
			),
			expected: &notExists,
			query: query(
				map[string]string{"missing": "never"},
				map[string]string{"customer": "c1"},
			),
		},
		{
			ctx: t.Context(),
			events: prepare(
				event("MultiR2User", `{"user":"u2","kind":"credit","batch_request":"r2"}`),
				event("MultiR2Account", `{"account":"b","region":"west","batch_request":"r2"}`),
				event("MultiR2Project", `{"project":"p2","customer":"c2","batch_request":"r2"}`),
			),
			expected: &notExists,
			query: query(map[string]string{
				"user": "u2",
				"kind": "credit",
			}),
		},
		{
			ctx: t.Context(),
			events: prepare(
				event("MultiR3RejectedA", `{"account":"b","region":"west","batch_request":"r3"}`),
				event("MultiR3RejectedB", `{"account":"b","region":"west","batch_request":"r3"}`),
			),
			expected: &notExists,
			query: query(map[string]string{
				"account": "b",
				"region":  "west",
			}),
		},
		{
			ctx: t.Context(),
			events: prepare(
				event("MultiR4RejectedA", `{"project":"p2","batch_request":"r4"}`),
				event("MultiR4RejectedB", `{"project":"p2","batch_request":"r4"}`),
			),
			expected: &notExists,
			query: query(
				map[string]string{"project": "p2"},
				map[string]string{"phase": "never"},
			),
		},
		{
			ctx: t.Context(),
			events: prepare(
				event("MultiR5AcceptedA", `{"result":"exact-position","batch_request":"r5"}`),
				event("MultiR5AcceptedB", `{"result":"exact-position","batch_request":"r5"}`),
			),
			expected: &request2AccountPosition,
			query: query(map[string]string{
				"account": "b",
				"region":  "west",
			}),
		},
	})
	require.NoError(t, err)
	require.Len(t, outcomes, 6)
	require.NoError(t, outcomes[0].err)
	require.Equal(t, firstGlobalID+1, outcomes[0].globalID)
	require.Equal(t, strconv.FormatInt(firstGlobalID+2, 10), outcomes[0].transactionID)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(outcomes[1].err))
	require.NoError(t, outcomes[2].err)
	require.Equal(t, firstGlobalID+4, outcomes[2].globalID)
	require.Equal(t, strconv.FormatInt(request2TransactionID, 10), outcomes[2].transactionID)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(outcomes[3].err))
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(outcomes[4].err))
	require.NoError(t, outcomes[5].err)
	require.Equal(t, firstGlobalID+6, outcomes[5].globalID)
	require.Equal(t, strconv.FormatInt(firstGlobalID+7, 10), outcomes[5].transactionID)
	require.Equal(t, int64(1), saver.gc.canonicalFlushes.Load())
	require.Zero(t, saver.gc.independentFlushes.Load())

	type persistedPosition struct {
		eventType     string
		transactionID int64
		globalID      int64
	}
	rows, err := db.QueryContext(
		t.Context(),
		`SELECT data->>'eventType', transaction_id, global_id
		 FROM public.test_boundary_orisun_es_event
		 WHERE data ? 'batch_request'
		 ORDER BY global_id`,
	)
	require.NoError(t, err)
	defer rows.Close()
	var persisted []persistedPosition
	for rows.Next() {
		var position persistedPosition
		require.NoError(t, rows.Scan(
			&position.eventType,
			&position.transactionID,
			&position.globalID,
		))
		persisted = append(persisted, position)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []persistedPosition{
		{"MultiR0Opened", firstGlobalID + 2, firstGlobalID},
		{"MultiR0Customer", firstGlobalID + 2, firstGlobalID + 1},
		{"MultiR2User", request2TransactionID, firstGlobalID + 2},
		{"MultiR2Account", request2TransactionID, firstGlobalID + 3},
		{"MultiR2Project", request2TransactionID, firstGlobalID + 4},
		{"MultiR5AcceptedA", firstGlobalID + 7, firstGlobalID + 5},
		{"MultiR5AcceptedB", firstGlobalID + 7, firstGlobalID + 6},
	}, persisted)

	require.NoError(t, db.QueryRowContext(
		t.Context(),
		`SELECT last_value
		 FROM public.test_boundary_orisun_es_event_global_id_seq`,
	).Scan(&sequenceLast))
	require.Equal(t, firstGlobalID+6, sequenceLast)
}

func TestPostgresGroupCommit_DifferentialAgainstSequentialWrites(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, container.container.Terminate(context.Background()))
	}()

	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, RunDbScripts(
		db,
		"reference_boundary",
		"public",
		false,
		t.Context(),
	))

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

	type storedEvent struct {
		data     map[string]string
		position orisun.Position
	}
	type persistedEvent struct {
		transactionID int64
		globalID      int64
		eventID       string
		data          string
		metadata      string
	}

	legacySave := func(
		boundary string,
		request *postgresSaveRequest,
	) postgresBatchOutcome {
		consistencyJSON, marshalErr := json.Marshal(
			getStreamSectionAsMap(request.expected, request.query),
		)
		require.NoError(t, marshalErr)
		eventsJSON, marshalErr := json.Marshal(request.events)
		require.NoError(t, marshalErr)

		var newGlobalID, transactionID, globalID int64
		saveErr := db.QueryRowContext(
			t.Context(),
			fmt.Sprintf(insertEventsWithConsistency, "public"),
			boundary,
			"public",
			consistencyJSON,
			eventsJSON,
		).Scan(&newGlobalID, &transactionID, &globalID)
		outcome := postgresBatchOutcome{req: request}
		if saveErr != nil {
			outcome.err = saver.mapSaveError(saveErr)
			return outcome
		}
		outcome.transactionID = strconv.FormatInt(transactionID, 10)
		outcome.globalID = globalID
		return outcome
	}

	readPersisted := func(boundary string) []persistedEvent {
		rows, queryErr := db.QueryContext(
			t.Context(),
			fmt.Sprintf(
				`SELECT transaction_id, global_id, event_id::TEXT, data::TEXT, metadata::TEXT
				 FROM public.%s_orisun_es_event
				 ORDER BY global_id`,
				boundary,
			),
		)
		require.NoError(t, queryErr)
		defer rows.Close()

		var events []persistedEvent
		for rows.Next() {
			var event persistedEvent
			require.NoError(t, rows.Scan(
				&event.transactionID,
				&event.globalID,
				&event.eventID,
				&event.data,
				&event.metadata,
			))
			events = append(events, event)
		}
		require.NoError(t, rows.Err())
		return events
	}

	queryFromMaps := func(criteria ...map[string]string) *orisun.Query {
		query := &orisun.Query{
			Criteria: make([]*orisun.Criterion, 0, len(criteria)),
		}
		for _, tags := range criteria {
			criterion := &orisun.Criterion{
				Tags: make([]*orisun.Tag, 0, len(tags)),
			}
			for key, value := range tags {
				criterion.Tags = append(
					criterion.Tags,
					&orisun.Tag{Key: key, Value: value},
				)
			}
			query.Criteria = append(query.Criteria, criterion)
		}
		return query
	}

	matches := func(data map[string]string, query *orisun.Query) bool {
		if query == nil {
			return false
		}
		hasNonEmptyCriterion := false
		for _, criterion := range query.Criteria {
			if criterion == nil || len(criterion.Tags) == 0 {
				continue
			}
			hasNonEmptyCriterion = true
			criterionMatches := true
			for _, tag := range criterion.Tags {
				if tag == nil || data[tag.Key] != tag.Value {
					criterionMatches = false
					break
				}
			}
			if criterionMatches {
				return true
			}
		}
		return !hasNonEmptyCriterion
	}

	latestPosition := func(
		stored []storedEvent,
		query *orisun.Query,
	) orisun.Position {
		latest := orisun.NotExistsPosition()
		for _, event := range stored {
			if matches(event.data, query) {
				latest = event.position
			}
		}
		return latest
	}

	const trials = 40
	random := rand.New(rand.NewSource(0x0C0C0C))
	keys := []string{"account", "kind", "region"}
	values := []string{"a", "b", "c"}

	for trial := range trials {
		t.Run(fmt.Sprintf("trial_%02d", trial), func(t *testing.T) {
			_, err := db.ExecContext(
				t.Context(),
				`TRUNCATE TABLE
					public.test_boundary_orisun_es_event,
					public.reference_boundary_orisun_es_event
					RESTART IDENTITY`,
			)
			require.NoError(t, err)

			var stored []storedEvent
			for seedIndex := range 4 {
				data := map[string]string{
					"account": values[seedIndex%len(values)],
					"kind":    values[(seedIndex+1)%len(values)],
					"region":  values[(seedIndex+2)%len(values)],
				}
				event := orisun.EventWithMapTags{
					EventId:   uuid.NewString(),
					EventType: fmt.Sprintf("Seed%02d", seedIndex),
					Data:      data,
					Metadata:  map[string]string{"source": "seed"},
				}
				prepared, prepareErr := orisun.PrepareEventsForSave(
					[]orisun.EventWithMapTags{event},
				)
				require.NoError(t, prepareErr)
				request := &postgresSaveRequest{
					ctx:    t.Context(),
					events: prepared,
				}
				groupSeed := legacySave("test_boundary", request)
				referenceSeed := legacySave("reference_boundary", request)
				require.NoError(t, groupSeed.err)
				require.NoError(t, referenceSeed.err)
				require.Equal(t, referenceSeed.transactionID, groupSeed.transactionID)
				require.Equal(t, referenceSeed.globalID, groupSeed.globalID)
				transactionID, parseErr := strconv.ParseInt(
					groupSeed.transactionID,
					10,
					64,
				)
				require.NoError(t, parseErr)
				stored = append(stored, storedEvent{
					data: data,
					position: orisun.Position{
						CommitPosition:  transactionID,
						PreparePosition: groupSeed.globalID,
					},
				})
			}

			requestCount := 2 + random.Intn(7)
			requests := make([]*postgresSaveRequest, 0, requestCount)
			mode := trial % 5
			for requestIndex := range requestCount {
				var query *orisun.Query
				switch mode {
				case 0:
					query = nil
				case 1:
					contextValue := fmt.Sprintf("independent-%02d", requestIndex)
					query = queryFromMaps(map[string]string{
						"account": contextValue,
					})
				case 2:
					query = queryFromMaps(map[string]string{
						"account": "overlapping",
					})
				default:
					switch random.Intn(5) {
					case 0:
						query = nil
					case 1:
						query = &orisun.Query{}
					case 2:
						query = queryFromMaps(map[string]string{
							keys[random.Intn(len(keys))]: values[random.Intn(len(values))],
						})
					case 3:
						firstKey := keys[random.Intn(len(keys))]
						secondKey := firstKey
						for secondKey == firstKey {
							secondKey = keys[random.Intn(len(keys))]
						}
						query = queryFromMaps(map[string]string{
							firstKey:  values[random.Intn(len(values))],
							secondKey: values[random.Intn(len(values))],
						})
					default:
						query = queryFromMaps(
							map[string]string{
								keys[random.Intn(len(keys))]: values[random.Intn(len(values))],
							},
							map[string]string{
								keys[random.Intn(len(keys))]: values[random.Intn(len(values))],
							},
						)
					}
				}

				eventCount := 1 + random.Intn(3)
				events := make([]orisun.EventWithMapTags, 0, eventCount)
				for eventIndex := range eventCount {
					data := map[string]string{
						"account": values[random.Intn(len(values))],
						"kind":    values[random.Intn(len(values))],
						"region":  values[random.Intn(len(values))],
					}
					if mode == 1 {
						data["account"] = fmt.Sprintf(
							"independent-%02d",
							requestIndex,
						)
					} else if mode == 2 {
						data["account"] = "overlapping"
					} else if query != nil &&
						len(query.Criteria) > 0 &&
						len(query.Criteria[0].Tags) > 0 &&
						(eventIndex == 0 || random.Intn(2) == 0) {
						for _, tag := range query.Criteria[0].Tags {
							data[tag.Key] = tag.Value
						}
					}
					events = append(events, orisun.EventWithMapTags{
						EventId: uuid.NewString(),
						EventType: fmt.Sprintf(
							"Trial%02dRequest%02dEvent%02d",
							trial,
							requestIndex,
							eventIndex,
						),
						Data: data,
						Metadata: map[string]any{
							"trial":   trial,
							"request": requestIndex,
							"event":   eventIndex,
						},
					})
				}
				prepared, prepareErr := orisun.PrepareEventsForSave(events)
				require.NoError(t, prepareErr)

				var expected *orisun.Position
				if query != nil {
					position := latestPosition(stored, query)
					switch random.Intn(5) {
					case 0:
						position = orisun.Position{
							CommitPosition:  999_999,
							PreparePosition: 999_998,
						}
					case 1:
						position = orisun.NotExistsPosition()
					}
					expected = &position
				}
				requests = append(requests, &postgresSaveRequest{
					ctx:      t.Context(),
					events:   prepared,
					expected: expected,
					query:    query,
				})
			}

			groupOutcomes, executeErr := saver.executeBatch(
				t.Context(),
				"test_boundary",
				requests,
			)
			require.NoError(t, executeErr)
			require.Len(t, groupOutcomes, len(requests))

			referenceOutcomes := make([]postgresBatchOutcome, 0, len(requests))
			for _, request := range requests {
				referenceOutcomes = append(
					referenceOutcomes,
					legacySave("reference_boundary", request),
				)
			}
			for outcomeIndex := range groupOutcomes {
				require.Equal(
					t,
					statuscode.CodeOf(referenceOutcomes[outcomeIndex].err),
					statuscode.CodeOf(groupOutcomes[outcomeIndex].err),
					"request %d returned a different status",
					outcomeIndex,
				)
				if referenceOutcomes[outcomeIndex].err == nil {
					require.Equal(
						t,
						referenceOutcomes[outcomeIndex].transactionID,
						groupOutcomes[outcomeIndex].transactionID,
					)
					require.Equal(
						t,
						referenceOutcomes[outcomeIndex].globalID,
						groupOutcomes[outcomeIndex].globalID,
					)
				}
			}
			require.Equal(
				t,
				readPersisted("reference_boundary"),
				readPersisted("test_boundary"),
			)
		})
	}

	require.Positive(t, saver.gc.fastFlushes.Load())
	require.Positive(t, saver.gc.independentFlushes.Load())
	require.Positive(t, saver.gc.canonicalFlushes.Load())
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
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				<-start
				expected := orisun.NotExistsPosition()
				_, globalID, saveErr := saver.Save(
					context.Background(),
					[]orisun.EventWithMapTags{event},
					"test_boundary",
					&expected,
					postgresIndependentCCCQuery(
						"stream_id",
						contextValue,
					),
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
	require.Equal(t, int64(contextCount-1), maxGlobalID)
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
	require.True(t, isUnconditionalFastPathRequest(valid, nil))

	require.False(t, isUnconditionalFastPathRequest(valid, &orisun.Query{}))

	multipleEvents := append(orisun.PreparedEventBatch(nil), valid...)
	multipleEvents = append(multipleEvents, valid[0])
	multipleEvents[1].EventId = uuid.Must(uuid.NewV7()).String()
	require.True(t, isUnconditionalFastPathRequest(multipleEvents, nil))

	invalidSecondEvent := append(orisun.PreparedEventBatch(nil), multipleEvents...)
	invalidSecondEvent[1].EventId = "not-a-uuid"
	require.False(t, isUnconditionalFastPathRequest(invalidSecondEvent, nil))

	invalidUUID := append(orisun.PreparedEventBatch(nil), valid...)
	invalidUUID[0].EventId = "not-a-uuid"
	require.False(t, isUnconditionalFastPathRequest(invalidUUID, nil))

	require.False(t, isUnconditionalFastPathRequest(nil, nil))
}

func TestCanUseCanonicalFastPathAcceptsValidMultiEventRequests(t *testing.T) {
	valid := orisun.PreparedEventBatch{
		{
			EventId:      uuid.Must(uuid.NewV7()).String(),
			EventType:    "First",
			DataJSON:     `{"eventType":"First"}`,
			MetadataJSON: `{}`,
		},
		{
			EventId:      uuid.Must(uuid.NewV7()).String(),
			EventType:    "Second",
			DataJSON:     `{"eventType":"Second"}`,
			MetadataJSON: `{}`,
		},
	}
	requests := []*postgresSaveRequest{{events: valid}}
	require.True(t, canUseCanonicalFastPath(requests))
	_, independent := independentCCCKey(requests)
	require.False(t, independent)

	require.False(t, canUseCanonicalFastPath([]*postgresSaveRequest{{events: nil}}))

	invalid := append(orisun.PreparedEventBatch(nil), valid...)
	invalid[1].EventId = "not-a-uuid"
	require.False(t, canUseCanonicalFastPath([]*postgresSaveRequest{{events: invalid}}))
}

func TestIndependentCCCKeyRejectsOverlappingOrAmbiguousShapes(t *testing.T) {
	request := func(key, queryValue, eventValue string) *postgresSaveRequest {
		prepared, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
			postgresIndependentCCCEvent(t, "Eligibility", eventValue),
		})
		require.NoError(t, err)
		return &postgresSaveRequest{
			events: prepared,
			query:  postgresIndependentCCCQuery(key, queryValue),
		}
	}

	key, ok := independentCCCKey([]*postgresSaveRequest{
		request("stream_id", "a", "a"),
		request("stream_id", "b", "b"),
	})
	require.True(t, ok)
	require.Equal(t, "stream_id", key)

	multi := request("stream_id", "multi", "multi")
	second, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		postgresIndependentCCCEvent(t, "EligibilitySecond", "multi"),
	})
	require.NoError(t, err)
	multi.events = append(multi.events, second...)
	key, ok = independentCCCKey([]*postgresSaveRequest{multi})
	require.True(t, ok)
	require.Equal(t, "stream_id", key)

	_, ok = independentCCCKey([]*postgresSaveRequest{
		request("stream_id", "same", "same"),
		request("stream_id", "same", "same"),
	})
	require.False(t, ok, "duplicate contexts can invalidate one another")

	_, ok = independentCCCKey([]*postgresSaveRequest{
		request("stream_id", "a", "b"),
		request("stream_id", "b", "b"),
	})
	require.False(t, ok, "an event that belongs to another context must fall back")

	_, ok = independentCCCKey([]*postgresSaveRequest{
		request("stream_id", "a", "a"),
		request("account_id", "b", "b"),
	})
	require.False(t, ok, "different criterion shapes may overlap through one event")

	complex := request("stream_id", "a", "a")
	complex.query.Criteria[0].Tags = append(
		complex.query.Criteria[0].Tags,
		&orisun.Tag{Key: "kind", Value: "credit"},
	)
	_, ok = independentCCCKey([]*postgresSaveRequest{complex})
	require.False(t, ok)

	numeric, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{
		EventId:   uuid.NewString(),
		EventType: "Numeric",
		Data:      `{"stream_id":42}`,
		Metadata:  `{}`,
	}})
	require.NoError(t, err)
	_, ok = independentCCCKey([]*postgresSaveRequest{{
		events: numeric,
		query:  postgresIndependentCCCQuery("stream_id", "42"),
	}})
	require.False(t, ok, "only exact string values use the specialized SQL path")
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

func postgresIndependentCCCQuery(key, value string) *orisun.Query {
	return &orisun.Query{Criteria: []*orisun.Criterion{{
		Tags: []*orisun.Tag{{Key: key, Value: value}},
	}}}
}
