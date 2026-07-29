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
	require.False(t, isUnconditionalFastPathRequest(multipleEvents, nil))

	invalidUUID := append(orisun.PreparedEventBatch(nil), valid...)
	invalidUUID[0].EventId = "not-a-uuid"
	require.False(t, isUnconditionalFastPathRequest(invalidUUID, nil))
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
