package sqlite

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	config "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

const gcBoundary = "test"

func newGCTestSaver(t *testing.T) (*SqliteSaveEvents, *BoundaryPools, func()) {
	t.Helper()
	dir := t.TempDir()
	bp, err := OpenBoundaryPools(context.Background(), dir, gcBoundary, gcBoundary)
	if err != nil {
		t.Fatalf("open pools: %v", err)
	}
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(map[string]*BoundaryPools{gcBoundary: bp}, logger)
	return saver, bp, func() {
		saver.close()
		_ = bp.Close()
	}
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// holdWriteConn checks out the boundary's single write connection so the
// worker stalls in its next flush, letting tests queue requests behind it.
func holdWriteConn(t *testing.T, bp *BoundaryPools) func() {
	t.Helper()
	conn, err := bp.Write.Take(context.Background())
	if err != nil {
		t.Fatalf("hold write conn: %v", err)
	}
	return func() { bp.Write.Put(conn) }
}

func readSeqNextID(t *testing.T, bp *BoundaryPools) int64 {
	t.Helper()
	conn, err := bp.Read.Take(context.Background())
	if err != nil {
		t.Fatalf("take read conn: %v", err)
	}
	defer bp.Read.Put(conn)
	var next int64
	if err := sqlitex.Execute(conn, "SELECT next_id FROM orisun_es_seq WHERE id = 1", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			next = stmt.ColumnInt64(0)
			return nil
		},
	}); err != nil {
		t.Fatalf("read seq: %v", err)
	}
	return next
}

func TestSQLiteStatementCacheIsBoundedAndUsesLRU(t *testing.T) {
	_, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	conn, err := bp.Write.Take(context.Background())
	if err != nil {
		t.Fatalf("take write conn: %v", err)
	}
	defer bp.Write.Put(conn)

	cache := newSQLiteStatementCache(2)
	execute := func(query string, want int64) {
		t.Helper()
		var got int64
		if err := cache.execute(conn, query, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				got = stmt.ColumnInt64(0)
				return nil
			},
		}); err != nil {
			t.Fatalf("execute %q: %v", query, err)
		}
		if got != want {
			t.Fatalf("execute %q = %d, want %d", query, got, want)
		}
	}

	execute("SELECT 1", 1)
	execute("SELECT 2", 2)
	execute("SELECT 1", 1) // make SELECT 2 least-recently used
	execute("SELECT 3", 3)
	if len(cache.entries) != 2 {
		t.Fatalf("cache size = %d, want 2", len(cache.entries))
	}
	if cache.entries["SELECT 1"] == nil || cache.entries["SELECT 3"] == nil {
		t.Fatalf("unexpected cache contents after eviction: %v", cache.entries)
	}
	if cache.entries["SELECT 2"] != nil {
		t.Fatal("least-recently used statement was not evicted")
	}

	// An evicted statement can be prepared and executed again, evicting the
	// current least-recently used entry without growing the cache.
	execute("SELECT 2", 2)
	if len(cache.entries) != 2 || cache.entries["SELECT 1"] != nil {
		t.Fatalf("cache did not remain bounded after reprepare: %v", cache.entries)
	}
}

func countEventsMatching(t *testing.T, bp *BoundaryPools, criteria map[string]any) int {
	t.Helper()
	where, err := buildCriteriaSQLForBoundary([]map[string]any{criteria}, bp.indexes, gcBoundary)
	if err != nil {
		t.Fatalf("build criteria SQL: %v", err)
	}
	conn, err := bp.Read.Take(context.Background())
	if err != nil {
		t.Fatalf("take read conn: %v", err)
	}
	defer bp.Read.Put(conn)

	var count int
	if err := sqlitex.ExecuteTransient(conn,
		"SELECT COUNT(*) FROM orisun_es_event WHERE "+where,
		&sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				count = int(stmt.ColumnInt64(0))
				return nil
			},
		}); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return count
}

type saveOutcome struct {
	tx  string
	gid int64
	err error
}

// saveBypassingQueue is the pre-group-commit write path, preserved verbatim
// for comparison only: one IMMEDIATE transaction per call under the caller's
// context, no queue, no savepoint. Production code never calls this — it
// exists so tests and benchmarks can measure the batched path against the
// old direct behavior.
func saveBypassingQueue(
	saver *SqliteSaveEvents,
	ctx context.Context,
	events []eventstore.EventWithMapTags,
	boundary string,
	expectedPosition *eventstore.Position,
	query *eventstore.Query,
) (transactionID string, globalID int64, err error) {
	pool, ok := saver.pools[boundary]
	if !ok {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "unknown boundary: %s", boundary)
	}
	prepared, err := eventstore.PrepareEventsForSave(events)
	if err != nil {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "invalid event data: %v", err)
	}
	inserts := prepared

	conn, takeErr := pool.Write.Take(ctx)
	if takeErr != nil {
		return "", 0, statuscode.Errorf(statuscode.Internal, "take write conn: %v", takeErr)
	}
	defer pool.Write.Put(conn)

	endFn, beginErr := sqlitex.ImmediateTransaction(conn)
	if beginErr != nil {
		return "", 0, statuscode.Errorf(statuscode.Internal, "begin tx: %v", beginErr)
	}
	defer func() {
		endFn(&err)
		if err == nil && saver.notifier != nil {
			saver.notifier.Notify(boundary)
		}
	}()

	return saver.saveEventsOnConn(conn, pool, boundary, inserts, expectedPosition, query)
}

// blockWorkerThenQueue occupies the worker with one blocking save, runs
// enqueue() to queue requests behind it, then releases the connection. It
// guarantees the queued requests are drained into a single batch (up to the
// saver's limits) once the blocker's flush completes.
func blockWorkerThenQueue(t *testing.T, saver *SqliteSaveEvents, bp *BoundaryPools, queued int, enqueue func()) {
	t.Helper()
	release := holdWriteConn(t, bp)
	singlesBeforeBlocker := saver.gcSingleFlushes.Load()

	blockerDone := make(chan error, 1)
	go func() {
		_, _, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
			mustEvent(t, "Blocker", map[string]any{"role": "blocker"}, map[string]any{}),
		}, gcBoundary, nil, nil)
		blockerDone <- err
	}()
	// The blocker is inside its (stalled) flush once the single-flush counter ticks.
	waitUntil(t, "worker to pick up blocker", func() bool {
		return saver.gcSingleFlushes.Load() > singlesBeforeBlocker
	})

	enqueue()
	waitUntil(t, "requests to queue behind blocker", func() bool {
		return len(saver.queues[gcBoundary]) >= queued
	})

	release()
	if err := <-blockerDone; err != nil {
		t.Fatalf("blocker save: %v", err)
	}
}

func TestGroupCommit_ConfigPlumbing(t *testing.T) {
	dir := t.TempDir()
	bp, err := OpenBoundaryPools(context.Background(), dir, gcBoundary, gcBoundary)
	if err != nil {
		t.Fatalf("open pools: %v", err)
	}
	defer bp.Close()
	pools := map[string]*BoundaryPools{gcBoundary: bp}
	logger, _ := logging.ZapLogger("error")

	custom, err := NewSqliteSaveEventsWithConfig(pools, logger, config.SqliteGroupCommitConfig{
		MaxBatchRequests: 7,
		MaxBatchEvents:   11,
		MaxDelay:         3 * time.Millisecond,
		MaxPending:       13,
		FlushTimeout:     17 * time.Second,
	})
	if err != nil {
		t.Fatalf("custom config: %v", err)
	}
	defer custom.close()
	if custom.gcMaxBatchRequests != 7 || custom.gcMaxBatchEvents != 11 ||
		custom.gcMaxDelay != 3*time.Millisecond || custom.gcFlushTimeout != 17*time.Second {
		t.Fatalf("custom values not applied: %+v", custom)
	}
	if cap(custom.queues[gcBoundary]) != 13 {
		t.Fatalf("expected queue cap 13, got %d", cap(custom.queues[gcBoundary]))
	}

	// Zero values fall back to package defaults.
	defaulted, err := NewSqliteSaveEventsWithConfig(pools, logger, config.SqliteGroupCommitConfig{})
	if err != nil {
		t.Fatalf("default config: %v", err)
	}
	defer defaulted.close()
	if defaulted.gcMaxBatchRequests != sqliteGroupCommitMaxBatchRequests ||
		defaulted.gcMaxBatchEvents != sqliteGroupCommitMaxBatchEvents ||
		defaulted.gcMaxDelay != sqliteGroupCommitMaxDelay ||
		defaulted.gcFlushTimeout != sqliteGroupCommitFlushTimeout ||
		cap(defaulted.queues[gcBoundary]) != sqliteGroupCommitMaxPending {
		t.Fatalf("defaults not applied: %+v", defaulted)
	}

	// Negatives rejected.
	if _, err := NewSqliteSaveEventsWithConfig(pools, logger, config.SqliteGroupCommitConfig{
		MaxBatchRequests: -1,
	}); err == nil {
		t.Fatal("expected error for negative maxBatchRequests")
	}
	if _, err := NewSqliteSaveEventsWithConfig(pools, logger, config.SqliteGroupCommitConfig{
		MaxDelay: -time.Millisecond,
	}); err == nil {
		t.Fatal("expected error for negative maxDelay")
	}
}

func TestGroupCommit_CoalescesConcurrentSavesIntoOneFlush(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	const n = 5
	outcomes := make([]saveOutcome, n)
	var wg sync.WaitGroup

	blockWorkerThenQueue(t, saver, bp, n, func() {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				tx, gid, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
					mustEvent(t, "Coalesced", map[string]any{"idx": strconv.Itoa(i)}, map[string]any{}),
				}, gcBoundary, nil, nil)
				outcomes[i] = saveOutcome{tx, gid, err}
			}(i)
		}
	})
	wg.Wait()

	if got := saver.gcMultiFlushes.Load(); got != 1 {
		t.Fatalf("expected exactly 1 multi-request flush, got %d", got)
	}
	if got := saver.gcUnconditionalFlushes.Load(); got != 1 {
		t.Fatalf("expected unconditional fast path, got %d fast flushes", got)
	}
	seenGids := make(map[int64]bool, n)
	for i, o := range outcomes {
		if o.err != nil {
			t.Fatalf("save %d: %v", i, o.err)
		}
		if o.tx != strconv.FormatInt(o.gid, 10) {
			t.Errorf("save %d: single-event request should have tx == gid, got tx=%s gid=%d", i, o.tx, o.gid)
		}
		if seenGids[o.gid] {
			t.Errorf("duplicate global id %d", o.gid)
		}
		seenGids[o.gid] = true
	}
	// Blocker took gid 1; the batch owns 2..n+1, gap-free.
	for gid := int64(2); gid <= int64(n+1); gid++ {
		if !seenGids[gid] {
			t.Errorf("missing global id %d in batch results", gid)
		}
	}
	if next := readSeqNextID(t, bp); next != int64(n+2) {
		t.Errorf("expected seq next_id %d, got %d", n+2, next)
	}
}

func TestGroupCommit_UnconditionalFastPathPreservesMultiEventRequestPositions(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	getter := NewSqliteGetEvents(map[string]*BoundaryPools{gcBoundary: bp}, logger)

	outcomes := make([]saveOutcome, 2)
	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, 2, func() {
		for requestIndex, eventCount := range []int{2, 3} {
			requestIndex, eventCount := requestIndex, eventCount
			wg.Add(1)
			go func() {
				defer wg.Done()
				events := make([]eventstore.EventWithMapTags, eventCount)
				for eventIndex := range events {
					events[eventIndex] = mustEvent(t, "Multi", map[string]any{
						"request": strconv.Itoa(requestIndex),
						"event":   strconv.Itoa(eventIndex),
					}, map[string]any{})
				}
				tx, gid, err := saver.Save(context.Background(), events, gcBoundary, nil, nil)
				outcomes[requestIndex] = saveOutcome{tx: tx, gid: gid, err: err}
			}()
		}
	})
	wg.Wait()

	if got := saver.gcUnconditionalFlushes.Load(); got != 1 {
		t.Fatalf("expected one unconditional fast flush, got %d", got)
	}
	for requestIndex, outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("request %d: %v", requestIndex, outcome.err)
		}
		if outcome.tx != strconv.FormatInt(outcome.gid, 10) {
			t.Fatalf("request %d: tx=%s gid=%d", requestIndex, outcome.tx, outcome.gid)
		}
		batch, err := getter.GetBatch(context.Background(), &eventstore.GetEventsRequest{
			Boundary:  gcBoundary,
			Direction: eventstore.Direction_ASC,
			Count:     10,
			Query: &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{
				{Key: "request", Value: strconv.Itoa(requestIndex)},
			}}}},
		})
		if err != nil {
			t.Fatalf("read request %d: %v", requestIndex, err)
		}
		wantEvents := requestIndex + 2
		if len(batch) != wantEvents {
			t.Fatalf("request %d: got %d events, want %d", requestIndex, len(batch), wantEvents)
		}
		for _, event := range batch {
			if event.CommitPosition != outcome.gid {
				t.Errorf("request %d: event gid=%d has tx=%d, want %d",
					requestIndex, event.PreparePosition, event.CommitPosition, outcome.gid)
			}
		}
		if batch[len(batch)-1].PreparePosition != outcome.gid {
			t.Errorf("request %d: last stored gid=%d, returned gid=%d",
				requestIndex, batch[len(batch)-1].PreparePosition, outcome.gid)
		}
	}
}

func TestCanUseUnconditionalFastPath(t *testing.T) {
	valid := eventstore.PreparedEventBatch{{DataJSON: `{}`}}
	requests := []*sqliteSaveRequest{
		{inserts: valid},
		{inserts: valid},
	}
	if !canUseUnconditionalFastPath(requests) {
		t.Fatal("expected valid query-less requests to use fast path")
	}

	withQuery := []*sqliteSaveRequest{{inserts: valid, query: &eventstore.Query{}}}
	if !canUseUnconditionalFastPath(withQuery) {
		t.Fatal("SQLite treats a present empty query as unconditional")
	}
	if canUseUnconditionalFastPath([]*sqliteSaveRequest{{inserts: nil}}) {
		t.Fatal("empty event batch must not use fast path")
	}
	if canUseUnconditionalFastPath([]*sqliteSaveRequest{{
		inserts: eventstore.PreparedEventBatch{{DataJSON: `{"broken":`}},
	}}) {
		t.Fatal("invalid event JSON must retain request-local isolation")
	}
}

func TestGroupCommit_UnconditionalFastPathFallsBackForInvalidPreparedData(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	valid, err := eventstore.PrepareEventsForSave([]eventstore.EventWithMapTags{
		mustEvent(t, "Valid", map[string]any{"valid": "yes"}, map[string]any{}),
	})
	if err != nil {
		t.Fatalf("prepare valid event: %v", err)
	}
	invalid := eventstore.PreparedEventBatch{{
		EventId:      "invalid-json",
		EventType:    "Invalid",
		DataJSON:     `{"broken":`,
		MetadataJSON: `{}`,
	}}

	var invalidErr, validErr error
	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, 2, func() {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, invalidErr = saver.SavePrepared(context.Background(), invalid, gcBoundary, nil, nil)
		}()
		go func() {
			defer wg.Done()
			_, _, validErr = saver.SavePrepared(context.Background(), valid, gcBoundary, nil, nil)
		}()
	})
	wg.Wait()

	if statuscode.CodeOf(invalidErr) != statuscode.Internal {
		t.Fatalf("invalid prepared request: expected Internal, got %v", invalidErr)
	}
	if validErr != nil {
		t.Fatalf("valid request should survive isolated fallback: %v", validErr)
	}
	if got := saver.gcUnconditionalFlushes.Load(); got != 0 {
		t.Fatalf("invalid batch must not use unconditional fast path, got %d", got)
	}
	if next := readSeqNextID(t, bp); next != 3 {
		t.Fatalf("expected blocker and valid request only, next_id=%d", next)
	}
}

func TestGroupCommit_IndependentCCCFastPath(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	type seededContext struct {
		position eventstore.Position
	}
	seeded := make(map[string]seededContext, 2)
	for _, contextValue := range []string{"seed-good", "seed-stale"} {
		tx, gid, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
			mustEvent(t, "Seed", map[string]any{"stream_id": contextValue}, map[string]any{}),
		}, gcBoundary, nil, nil)
		if err != nil {
			t.Fatalf("seed %s: %v", contextValue, err)
		}
		commitPosition, err := strconv.ParseInt(tx, 10, 64)
		if err != nil {
			t.Fatalf("parse seed position: %v", err)
		}
		seeded[contextValue] = seededContext{position: eventstore.Position{
			CommitPosition:  commitPosition,
			PreparePosition: gid,
		}}
	}

	type independentOutcome struct {
		contextValue string
		saveOutcome
	}
	contexts := []string{"new", "seed-good", "seed-stale"}
	outcomes := make([]independentOutcome, len(contexts))
	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, len(contexts), func() {
		for i, contextValue := range contexts {
			i, contextValue := i, contextValue
			wg.Add(1)
			go func() {
				defer wg.Done()
				var expected *eventstore.Position
				if contextValue == "seed-good" {
					position := seeded[contextValue].position
					expected = &position
				}
				query := &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{
					{Key: "stream_id", Value: contextValue},
				}}}}
				events := []eventstore.EventWithMapTags{
					mustEvent(t, "Independent", map[string]any{"stream_id": contextValue}, map[string]any{}),
				}
				if contextValue == "new" {
					events = append(events,
						mustEvent(t, "IndependentSecond", map[string]any{"stream_id": contextValue}, map[string]any{}))
				}
				tx, gid, err := saver.Save(context.Background(), events, gcBoundary, expected, query)
				outcomes[i] = independentOutcome{
					contextValue: contextValue,
					saveOutcome:  saveOutcome{tx: tx, gid: gid, err: err},
				}
			}()
		}
	})
	wg.Wait()

	for _, outcome := range outcomes {
		if outcome.contextValue == "seed-stale" {
			if statuscode.CodeOf(outcome.err) != statuscode.AlreadyExists {
				t.Fatalf("stale context: expected AlreadyExists, got %v", outcome.err)
			}
			continue
		}
		if outcome.err != nil {
			t.Fatalf("context %s: %v", outcome.contextValue, outcome.err)
		}
		if outcome.tx != strconv.FormatInt(outcome.gid, 10) {
			t.Errorf("context %s: tx=%s gid=%d", outcome.contextValue, outcome.tx, outcome.gid)
		}
	}
	if got := saver.gcIndependentFlushes.Load(); got != 1 {
		t.Fatalf("expected one independent CCC flush, got %d", got)
	}
	if next := readSeqNextID(t, bp); next != 7 {
		t.Fatalf("expected two seeds, blocker, and three accepted events, next_id=%d", next)
	}
	if count := countEventsMatching(t, bp, map[string]any{"stream_id": "seed-stale"}); count != 1 {
		t.Fatalf("stale context wrote an event, count=%d", count)
	}
	logger, _ := logging.ZapLogger("error")
	getter := NewSqliteGetEvents(map[string]*BoundaryPools{gcBoundary: bp}, logger)
	batch, err := getter.GetBatch(context.Background(), &eventstore.GetEventsRequest{
		Boundary:  gcBoundary,
		Direction: eventstore.Direction_ASC,
		Count:     10,
		Query: &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{
			{Key: "stream_id", Value: "new"},
		}}}},
	})
	if err != nil {
		t.Fatalf("read multi-event context: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("multi-event context: got %d events", len(batch))
	}
	newOutcome := outcomes[0]
	for _, event := range batch {
		if event.CommitPosition != newOutcome.gid {
			t.Errorf("multi-event context: event gid=%d tx=%d, want tx=%d",
				event.PreparePosition, event.CommitPosition, newOutcome.gid)
		}
	}
	if batch[1].PreparePosition != newOutcome.gid {
		t.Errorf("multi-event context: last gid=%d, returned=%d", batch[1].PreparePosition, newOutcome.gid)
	}
}

func TestIndependentCCCContextsRejectsOverlappingOrAmbiguousShapes(t *testing.T) {
	_, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	request := func(key, queryValue, eventValue string) *sqliteSaveRequest {
		prepared, err := eventstore.PrepareEventsForSave([]eventstore.EventWithMapTags{
			mustEvent(t, "Independent", map[string]any{key: eventValue}, map[string]any{}),
		})
		if err != nil {
			t.Fatalf("prepare event: %v", err)
		}
		return &sqliteSaveRequest{
			inserts: prepared,
			query: &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{
				{Key: key, Value: queryValue},
			}}}},
		}
	}

	if _, ok := independentCCCContexts([]*sqliteSaveRequest{
		request("stream_id", "a", "a"),
		request("stream_id", "b", "b"),
	}, bp, gcBoundary); !ok {
		t.Fatal("expected distinct matching string contexts to qualify")
	}
	if _, ok := independentCCCContexts([]*sqliteSaveRequest{
		request("stream_id", "same", "same"),
		request("stream_id", "same", "same"),
	}, bp, gcBoundary); ok {
		t.Fatal("duplicate contexts can invalidate one another")
	}
	if _, ok := independentCCCContexts([]*sqliteSaveRequest{
		request("stream_id", "a", "b"),
		request("stream_id", "b", "b"),
	}, bp, gcBoundary); ok {
		t.Fatal("cross-context event must use isolated path")
	}
	if _, ok := independentCCCContexts([]*sqliteSaveRequest{
		request("stream_id", "a", "a"),
		request("account_id", "b", "b"),
	}, bp, gcBoundary); ok {
		t.Fatal("different keys must use isolated path")
	}

	complex := request("stream_id", "a", "a")
	complex.query.Criteria[0].Tags = append(complex.query.Criteria[0].Tags,
		&eventstore.Tag{Key: "kind", Value: "credit"})
	if _, ok := independentCCCContexts([]*sqliteSaveRequest{complex}, bp, gcBoundary); ok {
		t.Fatal("multi-tag criterion must use isolated path")
	}

	bp.indexes.replaceBoundaryFields(gcBoundary, map[string]sqliteFieldInfo{
		"stream_id": {valueType: "numeric", declaredField: true},
	})
	if _, ok := independentCCCContexts([]*sqliteSaveRequest{
		request("stream_id", "42", "42"),
		request("stream_id", "43", "43"),
	}, bp, gcBoundary); ok {
		t.Fatal("numeric contexts can alias after casts and must use isolated path")
	}
}

func TestGroupCommit_ResultsRouteToTheRightCallers(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	getter := NewSqliteGetEvents(map[string]*BoundaryPools{gcBoundary: bp}, logger)

	const n = 8
	outcomes := make([]saveOutcome, n)
	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, n, func() {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				tx, gid, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
					mustEvent(t, "Routed", map[string]any{"router_key": strconv.Itoa(i)}, map[string]any{}),
				}, gcBoundary, nil, nil)
				outcomes[i] = saveOutcome{tx, gid, err}
			}(i)
		}
	})
	wg.Wait()

	// Each caller's returned position must be the position of its own event.
	for i, o := range outcomes {
		if o.err != nil {
			t.Fatalf("save %d: %v", i, o.err)
		}
		resp, err := getter.GetBatch(context.Background(), &eventstore.GetEventsRequest{
			Boundary:  gcBoundary,
			Direction: eventstore.Direction_ASC,
			Count:     10,
			Query: &eventstore.Query{Criteria: []*eventstore.Criterion{
				{Tags: []*eventstore.Tag{{Key: "router_key", Value: strconv.Itoa(i)}}},
			}},
		})
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if len(resp) != 1 {
			t.Fatalf("save %d: expected 1 event, got %d", i, len(resp))
		}
		if resp[0].PreparePosition != o.gid {
			t.Errorf("save %d: returned gid %d but stored event has gid %d",
				i, o.gid, resp[0].PreparePosition)
		}
	}
}

func TestGroupCommit_InBatchConflictEarlierWinsLaterAlreadyExists(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()
	saver.gcCriterionMinBatchRequests = 2

	criteria := &eventstore.Query{Criteria: []*eventstore.Criterion{
		{Tags: []*eventstore.Tag{{Key: "agg", Value: "conflict-1"}}},
	}}

	var errB, errC error
	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, 2, func() {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, errB = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "B", map[string]any{"agg": "conflict-1"}, map[string]any{}),
			}, gcBoundary, nil, criteria) // expects not-exists
		}()
		go func() {
			defer wg.Done()
			_, _, errC = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "C", map[string]any{"agg": "conflict-1"}, map[string]any{}),
			}, gcBoundary, nil, criteria) // expects not-exists
		}()
	})
	wg.Wait()

	if saver.gcMultiFlushes.Load() != 1 {
		t.Fatalf("expected the conflicting saves to share one flush, got %d", saver.gcMultiFlushes.Load())
	}
	if saver.gcCriterionFlushes.Load() != 1 {
		t.Fatalf("expected overlapping contexts to use criterion-state, got %d", saver.gcCriterionFlushes.Load())
	}
	succeeded, rejected := 0, 0
	for _, err := range []error{errB, errC} {
		switch statuscode.CodeOf(err) {
		case statuscode.OK:
			succeeded++
		case statuscode.AlreadyExists:
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 || rejected != 1 {
		t.Fatalf("expected exactly one success and one ALREADY_EXISTS, got %d/%d (errB=%v errC=%v)",
			succeeded, rejected, errB, errC)
	}
	// Rejected savepoint rolled back its seq update: blocker + winner = 2 events.
	if next := readSeqNextID(t, bp); next != 3 {
		t.Errorf("expected gap-free seq next_id 3, got %d", next)
	}
}

func TestGroupCommit_CriterionStateGeneralQueries(t *testing.T) {
	t.Run("OR of AND criteria preserves ordered multi-event state", func(t *testing.T) {
		saver, bp, cleanup := newGCTestSaver(t)
		defer cleanup()
		saver.gcCriterionMinBatchRequests = 2

		_, c1GID, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
			mustEvent(t, "SeedC1", map[string]any{
				"tenant_id": "t1",
				"kind":      "credit",
			}, map[string]any{}),
		}, gcBoundary, nil, nil)
		if err != nil {
			t.Fatalf("seed C1: %v", err)
		}
		_, c2GID, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
			mustEvent(t, "SeedC2", map[string]any{"order_id": "o9"}, map[string]any{}),
		}, gcBoundary, nil, nil)
		if err != nil {
			t.Fatalf("seed C2: %v", err)
		}

		c1 := &eventstore.Criterion{Tags: []*eventstore.Tag{
			{Key: "tenant_id", Value: "t1"},
			{Key: "kind", Value: "credit"},
		}}
		c2 := &eventstore.Criterion{Tags: []*eventstore.Tag{{Key: "order_id", Value: "o9"}}}
		firstEvents, err := eventstore.PrepareEventsForSave([]eventstore.EventWithMapTags{
			mustEvent(t, "FirstMatch", map[string]any{
				"tenant_id": "t1",
				"kind":      "credit",
			}, map[string]any{}),
			mustEvent(t, "FirstTail", map[string]any{"other": "value"}, map[string]any{}),
		})
		if err != nil {
			t.Fatalf("prepare first: %v", err)
		}
		secondEvents, err := eventstore.PrepareEventsForSave([]eventstore.EventWithMapTags{
			mustEvent(t, "Second", map[string]any{
				"tenant_id": "t1",
				"kind":      "credit",
			}, map[string]any{}),
		})
		if err != nil {
			t.Fatalf("prepare second: %v", err)
		}
		firstExpected := eventstore.Position{CommitPosition: c2GID, PreparePosition: c2GID}
		secondExpected := eventstore.Position{CommitPosition: c1GID, PreparePosition: c1GID}
		requests := []*sqliteSaveRequest{
			{
				ctx:      context.Background(),
				inserts:  firstEvents,
				expected: &firstExpected,
				query:    &eventstore.Query{Criteria: []*eventstore.Criterion{c1, c2}},
				result:   make(chan sqliteSaveResult, 1),
			},
			{
				ctx:      context.Background(),
				inserts:  secondEvents,
				expected: &secondExpected,
				query:    &eventstore.Query{Criteria: []*eventstore.Criterion{c1}},
				result:   make(chan sqliteSaveResult, 1),
			},
		}
		saver.runFlush(gcBoundary, bp, requests)
		firstResult := <-requests[0].result
		secondResult := <-requests[1].result

		if firstResult.err != nil {
			t.Fatalf("first request: %v", firstResult.err)
		}
		if firstResult.transactionID != "4" || firstResult.globalID != 4 {
			t.Fatalf("first position = (%s,%d), want (4,4)", firstResult.transactionID, firstResult.globalID)
		}
		if statuscode.CodeOf(secondResult.err) != statuscode.AlreadyExists {
			t.Fatalf("second request should observe first and conflict: %v", secondResult.err)
		}
		if !strings.Contains(secondResult.err.Error(), "Actual (4, 3)") {
			t.Fatalf("second request observed wrong criterion position: %v", secondResult.err)
		}
		if saver.gcCriterionFlushes.Load() != 1 {
			t.Fatalf("expected criterion-state flush, got %d", saver.gcCriterionFlushes.Load())
		}
		if next := readSeqNextID(t, bp); next != 5 {
			t.Fatalf("expected two seeds and two accepted events, next_id=%d", next)
		}
		if count := countEventsMatching(t, bp, map[string]any{
			"tenant_id": "t1",
			"kind":      "credit",
		}); count != 2 {
			t.Fatalf("expected seed plus first matching event, count=%d", count)
		}
	})

	t.Run("queryless write invalidates later queried request", func(t *testing.T) {
		saver, bp, cleanup := newGCTestSaver(t)
		defer cleanup()
		saver.gcCriterionMinBatchRequests = 2

		firstEvents, err := eventstore.PrepareEventsForSave([]eventstore.EventWithMapTags{
			mustEvent(t, "Unconditional", map[string]any{"account_id": "a1"}, map[string]any{}),
		})
		if err != nil {
			t.Fatalf("prepare first: %v", err)
		}
		secondEvents, err := eventstore.PrepareEventsForSave([]eventstore.EventWithMapTags{
			mustEvent(t, "Conditional", map[string]any{"account_id": "a1"}, map[string]any{}),
		})
		if err != nil {
			t.Fatalf("prepare second: %v", err)
		}
		requests := []*sqliteSaveRequest{
			{
				ctx:     context.Background(),
				inserts: firstEvents,
				result:  make(chan sqliteSaveResult, 1),
			},
			{
				ctx:     context.Background(),
				inserts: secondEvents,
				query: &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{
					{Key: "account_id", Value: "a1"},
				}}}},
				result: make(chan sqliteSaveResult, 1),
			},
		}
		saver.runFlush(gcBoundary, bp, requests)
		firstResult := <-requests[0].result
		secondResult := <-requests[1].result
		if firstResult.err != nil {
			t.Fatalf("queryless request: %v", firstResult.err)
		}
		if statuscode.CodeOf(secondResult.err) != statuscode.AlreadyExists {
			t.Fatalf("later request should conflict: %v", secondResult.err)
		}
		if saver.gcCriterionFlushes.Load() != 1 {
			t.Fatalf("expected criterion-state flush, got %d", saver.gcCriterionFlushes.Load())
		}
		if next := readSeqNextID(t, bp); next != 2 {
			t.Fatalf("only queryless request should allocate an ID, next_id=%d", next)
		}
	})

	t.Run("typed numeric dependency uses SQLite comparison semantics", func(t *testing.T) {
		saver, bp, cleanup := newGCTestSaver(t)
		defer cleanup()
		saver.gcCriterionMinBatchRequests = 2
		bp.indexes.replaceBoundaryFields(gcBoundary, map[string]sqliteFieldInfo{
			"score": {valueType: "numeric", declaredField: true},
		})

		firstEvents, err := eventstore.PrepareEventsForSave([]eventstore.EventWithMapTags{
			mustEvent(t, "Numeric", map[string]any{"score": 42}, map[string]any{}),
		})
		if err != nil {
			t.Fatalf("prepare first: %v", err)
		}
		secondEvents, err := eventstore.PrepareEventsForSave([]eventstore.EventWithMapTags{
			mustEvent(t, "NumericLater", map[string]any{"score": 42.0}, map[string]any{}),
		})
		if err != nil {
			t.Fatalf("prepare second: %v", err)
		}
		requests := []*sqliteSaveRequest{
			{ctx: context.Background(), inserts: firstEvents, result: make(chan sqliteSaveResult, 1)},
			{
				ctx:     context.Background(),
				inserts: secondEvents,
				query: &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{
					{Key: "score", Value: "42.0"},
				}}}},
				result: make(chan sqliteSaveResult, 1),
			},
		}
		saver.runFlush(gcBoundary, bp, requests)
		firstResult := <-requests[0].result
		secondResult := <-requests[1].result
		if firstResult.err != nil || statuscode.CodeOf(secondResult.err) != statuscode.AlreadyExists {
			t.Fatalf("typed dependency results: first=%v second=%v", firstResult.err, secondResult.err)
		}
		if saver.gcCriterionFlushes.Load() != 1 {
			t.Fatalf("expected criterion-state flush, got %d", saver.gcCriterionFlushes.Load())
		}
	})
}

func TestGroupCommit_InBatchSameExpectedPositionOnlyOneWins(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	criteria := &eventstore.Query{Criteria: []*eventstore.Criterion{
		{Tags: []*eventstore.Tag{{Key: "agg", Value: "same-expected-in-batch"}}},
	}}
	_, gid, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
		mustEvent(t, "Seed", map[string]any{"agg": "same-expected-in-batch"}, map[string]any{}),
	}, gcBoundary, nil, criteria)
	if err != nil {
		t.Fatalf("seed save: %v", err)
	}
	expected := &eventstore.Position{CommitPosition: gid, PreparePosition: gid}

	var errB, errC error
	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, 2, func() {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, errB = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "B", map[string]any{"agg": "same-expected-in-batch"}, map[string]any{}),
			}, gcBoundary, expected, criteria)
		}()
		go func() {
			defer wg.Done()
			_, _, errC = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "C", map[string]any{"agg": "same-expected-in-batch"}, map[string]any{}),
			}, gcBoundary, expected, criteria)
		}()
	})
	wg.Wait()

	succeeded, rejected := 0, 0
	for _, err := range []error{errB, errC} {
		switch statuscode.CodeOf(err) {
		case statuscode.OK:
			succeeded++
		case statuscode.AlreadyExists:
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 || rejected != 1 {
		t.Fatalf("expected exactly one success and one ALREADY_EXISTS, got %d/%d (errB=%v errC=%v)",
			succeeded, rejected, errB, errC)
	}
	if count := countEventsMatching(t, bp, map[string]any{"agg": "same-expected-in-batch"}); count != 2 {
		t.Fatalf("expected seed plus one winning update, got %d matching events", count)
	}
	// Seed + blocker + winner. The losing savepoint must not consume an ID.
	if next := readSeqNextID(t, bp); next != 4 {
		t.Errorf("expected gap-free seq next_id 4, got %d", next)
	}
}

func TestGroupCommit_CancelledWhileQueuedIsDroppedWithoutAllocatingIDs(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	ctxB, cancelB := context.WithCancel(context.Background())
	var errB, errC error
	bDone := make(chan struct{})
	var wg sync.WaitGroup

	blockWorkerThenQueue(t, saver, bp, 2, func() {
		go func() {
			_, _, errB = saver.Save(ctxB, []eventstore.EventWithMapTags{
				mustEvent(t, "B", map[string]any{"who": "b"}, map[string]any{}),
			}, gcBoundary, nil, nil)
			close(bDone)
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, errC = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "C", map[string]any{"who": "c"}, map[string]any{}),
			}, gcBoundary, nil, nil)
		}()
		// Cancel B only after both requests are queued behind the blocker.
		waitUntil(t, "both requests queued", func() bool { return len(saver.queues[gcBoundary]) == 2 })
		cancelB()
		<-bDone
	})
	wg.Wait()

	if statuscode.CodeOf(errB) != statuscode.Canceled {
		t.Fatalf("expected Canceled for the cancelled save, got %v", errB)
	}
	if errC != nil {
		t.Fatalf("unrelated save should succeed, got %v", errC)
	}
	// Blocker + C committed; the cancelled B never allocated an ID.
	if next := readSeqNextID(t, bp); next != 3 {
		t.Errorf("expected seq next_id 3, got %d", next)
	}
}

func TestGroupCommit_BatchLimitsAreRespected(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()
	saver.gcMaxBatchRequests = 2

	const n = 3
	errs := make([]error, n)
	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, n, func() {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _, errs[i] = saver.Save(context.Background(), []eventstore.EventWithMapTags{
					mustEvent(t, "Limited", map[string]any{"idx": strconv.Itoa(i)}, map[string]any{}),
				}, gcBoundary, nil, nil)
			}(i)
		}
	})
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	// 3 queued requests with a 2-request cap: one batch of 2, then a leftover
	// single-request flush (blocker was a single flush too).
	if multi := saver.gcMultiFlushes.Load(); multi != 1 {
		t.Errorf("expected 1 multi-request flush, got %d", multi)
	}
	if single := saver.gcSingleFlushes.Load(); single != 2 {
		t.Errorf("expected 2 single-request flushes (blocker + leftover), got %d", single)
	}
}

func TestGroupCommit_EventBatchLimitCarriesOverflowToNextFlush(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()
	saver.gcMaxBatchRequests = 10
	saver.gcMaxBatchEvents = 2

	const n = 3
	errs := make([]error, n)
	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, n, func() {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, _, errs[i] = saver.Save(context.Background(), []eventstore.EventWithMapTags{
					mustEvent(t, "EventLimited", map[string]any{"idx": strconv.Itoa(i)}, map[string]any{}),
				}, gcBoundary, nil, nil)
			}(i)
		}
	})
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	if multi := saver.gcMultiFlushes.Load(); multi != 1 {
		t.Errorf("expected 1 two-event multi-request flush, got %d", multi)
	}
	if single := saver.gcSingleFlushes.Load(); single != 2 {
		t.Errorf("expected 2 single-request flushes (blocker + overflow), got %d", single)
	}
}

func TestGroupCommit_PanicDuringFlushReturnsInternalAndWorkerSurvives(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()
	// Panic only in multi-request flushes so the blocker's single flush survives.
	saver.gcTestFlushHook = func(batchSize int) {
		if batchSize > 1 {
			panic("boom")
		}
	}

	var errB, errC error
	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, 2, func() {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, errB = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "B", map[string]any{"who": "b"}, map[string]any{}),
			}, gcBoundary, nil, nil)
		}()
		go func() {
			defer wg.Done()
			_, _, errC = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "C", map[string]any{"who": "c"}, map[string]any{}),
			}, gcBoundary, nil, nil)
		}()
	})
	wg.Wait()

	for _, err := range []error{errB, errC} {
		if statuscode.CodeOf(err) != statuscode.Internal {
			t.Fatalf("expected Internal for panicked flush, got %v", err)
		}
	}

	// Worker must keep serving: a follow-up save (direct path, no hook) works.
	saver.gcTestFlushHook = nil
	if _, _, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
		mustEvent(t, "After", map[string]any{"who": "after"}, map[string]any{}),
	}, gcBoundary, nil, nil); err != nil {
		t.Fatalf("save after panic: %v", err)
	}
	// Only blocker + follow-up persisted; the panicked batch allocated nothing.
	if next := readSeqNextID(t, bp); next != 3 {
		t.Errorf("expected seq next_id 3, got %d", next)
	}
}

func TestGroupCommit_NotifierFiresAfterBatchedCommit(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()
	// Hour-long tick interval: Wait below only returns via an actual Notify.
	notifier := NewSqliteEventNotifier(time.Hour)
	saver.notifier = notifier
	signal := notifier.Signal(gcBoundary)
	defer signal.Stop()

	var wg sync.WaitGroup
	blockWorkerThenQueue(t, saver, bp, 2, func() {
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if _, _, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
					mustEvent(t, "Notify", map[string]any{"idx": strconv.Itoa(i)}, map[string]any{}),
				}, gcBoundary, nil, nil); err != nil {
					t.Errorf("save %d: %v", i, err)
				}
			}(i)
		}
	})
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := signal.Wait(ctx); err != nil {
		t.Fatalf("expected notify after batched commit, got %v", err)
	}
}

func TestGroupCommit_ShutdownFailsQueuedRequestsAndRejectsNewOnes(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	var errB, errC error
	var wg sync.WaitGroup
	closeDone := make(chan struct{})
	blockWorkerThenQueue(t, saver, bp, 2, func() {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, errB = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "B", map[string]any{"who": "b"}, map[string]any{}),
			}, gcBoundary, nil, nil)
		}()
		go func() {
			defer wg.Done()
			_, _, errC = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "C", map[string]any{"who": "c"}, map[string]any{}),
			}, gcBoundary, nil, nil)
		}()
		waitUntil(t, "both requests queued", func() bool { return len(saver.queues[gcBoundary]) == 2 })
		go func() {
			saver.close()
			close(closeDone)
		}()
		waitUntil(t, "saver to close", saver.isClosed)
	})
	<-closeDone
	wg.Wait()

	// The blocker's in-progress flush finished; the queued-but-unflushed
	// requests fail with Unavailable.
	for _, err := range []error{errB, errC} {
		if statuscode.CodeOf(err) != statuscode.Unavailable {
			t.Fatalf("expected Unavailable for queued request after close, got %v", err)
		}
	}
	if _, _, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
		mustEvent(t, "D", map[string]any{"who": "d"}, map[string]any{}),
	}, gcBoundary, nil, nil); statuscode.CodeOf(err) != statuscode.Unavailable {
		t.Fatalf("expected Unavailable for new save after close, got %v", err)
	}
	if next := readSeqNextID(t, bp); next != 2 {
		t.Errorf("only the blocker should have committed, seq next_id = %d", next)
	}
}

func TestGroupCommit_ClosedPoolFailsCleanlyWithoutHangingTheWorker(t *testing.T) {
	saver, bp, _ := newGCTestSaver(t)
	_ = bp.Close()

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, err := saver.Save(ctx, []eventstore.EventWithMapTags{
			mustEvent(t, "X", map[string]any{"i": strconv.Itoa(i)}, map[string]any{}),
		}, gcBoundary, nil, nil)
		cancel()
		if statuscode.CodeOf(err) != statuscode.Internal {
			t.Fatalf("save %d: expected Internal on closed pool, got %v", i, err)
		}
	}
	saver.close()
}

func TestGroupCommit_ResultsMatchDirectModeForTheSameSequence(t *testing.T) {
	type saveFn func(ctx context.Context, events []eventstore.EventWithMapTags, boundary string, pos *eventstore.Position, query *eventstore.Query) (string, int64, error)

	runSequence := func(save saveFn) []saveOutcome {
		criteria := func(agg string) *eventstore.Query {
			return &eventstore.Query{Criteria: []*eventstore.Criterion{
				{Tags: []*eventstore.Tag{{Key: "agg", Value: agg}}},
			}}
		}
		var out []saveOutcome
		// 1: plain save, 2: CCC first write, 3: stale CCC (conflict), 4: multi-event batch.
		tx, gid, err := save(context.Background(), []eventstore.EventWithMapTags{
			mustEvent(t, "Plain", map[string]any{"k": "v"}, map[string]any{}),
		}, gcBoundary, nil, nil)
		out = append(out, saveOutcome{tx, gid, err})
		tx, gid, err = save(context.Background(), []eventstore.EventWithMapTags{
			mustEvent(t, "First", map[string]any{"agg": "a1"}, map[string]any{}),
		}, gcBoundary, nil, criteria("a1"))
		out = append(out, saveOutcome{tx, gid, err})
		tx, gid, err = save(context.Background(), []eventstore.EventWithMapTags{
			mustEvent(t, "Stale", map[string]any{"agg": "a1"}, map[string]any{}),
		}, gcBoundary, nil, criteria("a1"))
		out = append(out, saveOutcome{tx, gid, err})
		tx, gid, err = save(context.Background(), []eventstore.EventWithMapTags{
			mustEvent(t, "M1", map[string]any{"m": "1"}, map[string]any{}),
			mustEvent(t, "M2", map[string]any{"m": "2"}, map[string]any{}),
		}, gcBoundary, nil, nil)
		out = append(out, saveOutcome{tx, gid, err})
		return out
	}

	batchedSaver, _, cleanupBatched := newGCTestSaver(t)
	defer cleanupBatched()
	directSaver, _, cleanupDirect := newGCTestSaver(t)
	defer cleanupDirect()

	batched := runSequence(batchedSaver.Save)
	direct := runSequence(func(ctx context.Context, events []eventstore.EventWithMapTags, boundary string, pos *eventstore.Position, query *eventstore.Query) (string, int64, error) {
		return saveBypassingQueue(directSaver, ctx, events, boundary, pos, query)
	})

	for i := range direct {
		if statuscode.CodeOf(batched[i].err) != statuscode.CodeOf(direct[i].err) {
			t.Errorf("step %d: batched err %v, direct err %v", i, batched[i].err, direct[i].err)
		}
		if batched[i].tx != direct[i].tx || batched[i].gid != direct[i].gid {
			t.Errorf("step %d: batched (%s,%d), direct (%s,%d)",
				i, batched[i].tx, batched[i].gid, direct[i].tx, direct[i].gid)
		}
	}
}

func TestGroupCommit_ConcurrentBurstAllCommitGapFree(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()

	const n = 50
	errs := make([]error, n)
	gids := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, gid, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "Burst", map[string]any{"idx": strconv.Itoa(i)}, map[string]any{}),
			}, gcBoundary, nil, nil)
			errs[i], gids[i] = err, gid
		}(i)
	}
	wg.Wait()

	seen := make(map[int64]bool, n)
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("save %d: %v", i, errs[i])
		}
		if seen[gids[i]] {
			t.Fatalf("duplicate gid %d", gids[i])
		}
		seen[gids[i]] = true
	}
	for gid := int64(1); gid <= n; gid++ {
		if !seen[gid] {
			t.Errorf("missing gid %d", gid)
		}
	}
	if next := readSeqNextID(t, bp); next != n+1 {
		t.Errorf("expected seq next_id %d, got %d", n+1, next)
	}
}

func TestGroupCommit_ConcurrentSameExpectedPositionAcrossFlushesOnlyOneWins(t *testing.T) {
	saver, bp, cleanup := newGCTestSaver(t)
	defer cleanup()
	saver.gcMaxBatchRequests = 1

	criteria := &eventstore.Query{Criteria: []*eventstore.Criterion{
		{Tags: []*eventstore.Tag{{Key: "agg", Value: "same-expected-across-flushes"}}},
	}}
	_, gid, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
		mustEvent(t, "Seed", map[string]any{"agg": "same-expected-across-flushes"}, map[string]any{}),
	}, gcBoundary, nil, criteria)
	if err != nil {
		t.Fatalf("seed save: %v", err)
	}
	expected := &eventstore.Position{CommitPosition: gid, PreparePosition: gid}

	const n = 20
	start := make(chan struct{})
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, _, errs[i] = saver.Save(context.Background(), []eventstore.EventWithMapTags{
				mustEvent(t, "Raced", map[string]any{
					"agg":    "same-expected-across-flushes",
					"worker": strconv.Itoa(i),
				}, map[string]any{}),
			}, gcBoundary, expected, criteria)
		}(i)
	}
	close(start)
	wg.Wait()

	succeeded, rejected := 0, 0
	for i, err := range errs {
		switch statuscode.CodeOf(err) {
		case statuscode.OK:
			succeeded++
		case statuscode.AlreadyExists:
			rejected++
		default:
			t.Fatalf("save %d: unexpected error %v", i, err)
		}
	}
	if succeeded != 1 || rejected != n-1 {
		t.Fatalf("expected exactly one success and %d ALREADY_EXISTS, got %d/%d",
			n-1, succeeded, rejected)
	}
	if count := countEventsMatching(t, bp, map[string]any{"agg": "same-expected-across-flushes"}); count != 2 {
		t.Fatalf("expected seed plus one winning update, got %d matching events", count)
	}
	if next := readSeqNextID(t, bp); next != 3 {
		t.Errorf("expected only seed and winner to allocate IDs, seq next_id got %d", next)
	}
}
