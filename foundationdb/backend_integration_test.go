//go:build foundationdb

package foundationdb

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"

	config "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

// readEventPosition rebuilds a Position from packed scalar fields for
// cursor tracking and comparePositions in tests.
func readEventPosition(e eventstore.ReadEvent) *eventstore.Position {
	return &eventstore.Position{
		CommitPosition:  e.CommitPosition,
		PreparePosition: e.PreparePosition,
	}
}

func TestFoundationDBSaveGetCCCAndIndexes(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	_, _, err := backend.Save(ctx, []eventstore.EventWithMapTags{
		{
			EventId:   "evt-1",
			EventType: "OrderCreated",
			Data: map[string]any{
				"order_id":    "ord-1",
				"customer_id": "cust-1",
			},
			Metadata: map[string]any{},
		},
		{
			EventId:   "evt-2",
			EventType: "OrderPaid",
			Data: map[string]any{
				"order_id":    "ord-1",
				"customer_id": "cust-1",
			},
			Metadata: map[string]any{},
		},
	}, "test", nil, nil)
	if err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	resp, err := backend.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Count:     10,
		Direction: eventstore.Direction_ASC,
	})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if len(resp) != 2 {
		t.Fatalf("expected 2 events, got %d", len(resp))
	}
	// Positions are FDB versionstamps. Both events come from one Save so they
	// share a commit version and are ordered by consecutive user versions.
	if resp[0].CommitPosition != resp[1].CommitPosition {
		t.Fatalf("events from the same batch should share commit version: %v vs %v", readEventPosition(resp[0]), readEventPosition(resp[1]))
	}
	if resp[1].PreparePosition != resp[0].PreparePosition+1 {
		t.Fatalf("expected consecutive prepare positions, got %v then %v", readEventPosition(resp[0]), readEventPosition(resp[1]))
	}

	if err := backend.CreateBoundaryIndex(ctx, "test", "customer_id", []eventstore.BoundaryIndexField{
		{JsonKey: "customer_id", ValueType: "text"},
	}, nil, eventstore.IndexCombinatorAND); err != nil {
		t.Fatalf("CreateBoundaryIndex returned error: %v", err)
	}
	indexed, err := backend.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Count:     10,
		Direction: eventstore.Direction_ASC,
		Query: &eventstore.Query{Criteria: []*eventstore.Criterion{
			{Tags: []*eventstore.Tag{{Key: "customer_id", Value: "cust-1"}}},
		}},
	})
	if err != nil {
		t.Fatalf("indexed Get returned error: %v", err)
	}
	if len(indexed) != 2 {
		t.Fatalf("expected 2 indexed events, got %d", len(indexed))
	}

	latest, err := backend.GetLatestByCriteria(ctx, eventstore.LatestByCriteriaQuery{
		Boundary: "test",
		Criteria: []eventstore.ReadCriterion{
			{Tags: []eventstore.ReadTag{{Key: "customer_id", Value: "cust-1"}}},
			{Tags: []eventstore.ReadTag{{Key: "customer_id", Value: "missing"}}},
		},
	})
	if err != nil {
		t.Fatalf("GetLatestByCriteria returned error: %v", err)
	}
	if len(latest.Matches) != 2 || !latest.Matches[0].Found || latest.Matches[1].Found {
		t.Fatalf("unexpected latest matches: %+v", latest.Matches)
	}
	if latest.Matches[0].Event.EventType != "OrderPaid" {
		t.Fatalf("expected latest customer event OrderPaid, got %+v", latest.Matches[0].Event)
	}
	if latest.ContextCommitPosition != resp[1].CommitPosition || latest.ContextPreparePosition != resp[1].PreparePosition {
		t.Fatalf("unexpected latest context: (%d, %d)", latest.ContextCommitPosition, latest.ContextPreparePosition)
	}

	// A consistency condition must be covered by an index (fail-closed). Index
	// order_id so the condition below can be checked.
	if err := backend.CreateBoundaryIndex(ctx, "test", "order_id", []eventstore.BoundaryIndexField{
		{JsonKey: "order_id", ValueType: "text"},
	}, nil, eventstore.IndexCombinatorAND); err != nil {
		t.Fatalf("CreateBoundaryIndex(order_id) returned error: %v", err)
	}

	notExists := eventstore.NotExistsPosition()
	_, _, err = backend.Save(ctx, []eventstore.EventWithMapTags{
		{
			EventId:   "evt-3",
			EventType: "OrderShipped",
			Data:      map[string]any{"order_id": "ord-1", "customer_id": "cust-1"},
			Metadata:  map[string]any{},
		},
	}, "test", &notExists, &eventstore.Query{Criteria: []*eventstore.Criterion{
		{Tags: []*eventstore.Tag{{Key: "order_id", Value: "ord-1"}}},
	}})
	if statuscode.CodeOf(err) != statuscode.AlreadyExists {
		t.Fatalf("expected ALREADY_EXISTS conflict, got %v", err)
	}

	// An unindexed consistency condition must be rejected, not silently scanned.
	_, _, err = backend.Save(ctx, []eventstore.EventWithMapTags{
		{
			EventId:   "evt-4",
			EventType: "OrderNoted",
			Data:      map[string]any{"note": "x"},
			Metadata:  map[string]any{},
		},
	}, "test", &notExists, &eventstore.Query{Criteria: []*eventstore.Criterion{
		{Tags: []*eventstore.Tag{{Key: "uncovered_key", Value: "v"}}},
	}})
	if statuscode.CodeOf(err) != statuscode.FailedPrecondition {
		t.Fatalf("expected FAILED_PRECONDITION for unindexed consistency condition, got %v", err)
	}

	// FDB criteria reads also require a ready covering index. Falling back to a
	// boundary scan would make large boundaries unbounded and undermine the
	// backend's scaling contract.
	_, err = backend.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Count:     10,
		Direction: eventstore.Direction_ASC,
		Query: &eventstore.Query{Criteria: []*eventstore.Criterion{
			{Tags: []*eventstore.Tag{{Key: "uncovered_key", Value: "v"}}},
		}},
	})
	if statuscode.CodeOf(err) != statuscode.FailedPrecondition {
		t.Fatalf("expected FAILED_PRECONDITION for unindexed query, got %v", err)
	}
}

func TestFoundationDBAdminAndPublishingState(t *testing.T) {
	backend := newTestBackend(t)

	missing, err := backend.GetProjectorLastPosition(context.Background(), "missing-projector")
	if err != nil {
		t.Fatalf("GetProjectorLastPosition(missing): %v", err)
	}
	notExists := eventstore.NotExistsPosition()
	if missing.CommitPosition != notExists.CommitPosition || missing.PreparePosition != notExists.PreparePosition {
		t.Fatalf("missing projector position = %v, want %v", missing, &notExists)
	}

	user := eventstore.User{
		Id:             "user-1",
		Name:           "Admin",
		Username:       "admin",
		HashedPassword: "hash",
		Roles:          []eventstore.Role{eventstore.RoleAdmin},
	}
	if err := backend.UpsertUser(context.Background(), user); err != nil {
		t.Fatalf("UpsertUser returned error: %v", err)
	}
	got, err := backend.GetUserByUsername(context.Background(), "admin")
	if err != nil {
		t.Fatalf("GetUserByUsername returned error: %v", err)
	}
	if got.Id != user.Id {
		t.Fatalf("expected user id %q, got %q", user.Id, got.Id)
	}

	if err := backend.InsertLastPublishedEvent(context.Background(), "test", 4, 4); err != nil {
		t.Fatalf("InsertLastPublishedEvent returned error: %v", err)
	}
	pos, err := backend.GetLastPublishedEventPosition(context.Background(), "test")
	if err != nil {
		t.Fatalf("GetLastPublishedEventPosition returned error: %v", err)
	}
	if pos.CommitPosition != 4 || pos.PreparePosition != 4 {
		t.Fatalf("unexpected published position: %v", &pos)
	}
}

func TestFoundationDBUsernameMustBeUnique(t *testing.T) {
	backend := newTestBackend(t)

	if err := backend.UpsertUser(context.Background(), eventstore.User{
		Id:             "user-1",
		Name:           "Ada",
		Username:       "admin",
		HashedPassword: "hash-1",
		Roles:          []eventstore.Role{eventstore.RoleAdmin},
	}); err != nil {
		t.Fatalf("first UpsertUser: %v", err)
	}

	err := backend.UpsertUser(context.Background(), eventstore.User{
		Id:             "user-2",
		Name:           "Grace",
		Username:       "admin",
		HashedPassword: "hash-2",
		Roles:          []eventstore.Role{eventstore.RoleAdmin},
	})
	if statuscode.CodeOf(err) != statuscode.AlreadyExists {
		t.Fatalf("expected duplicate username to return ALREADY_EXISTS, got %v", err)
	}
}

func TestFoundationDBCanceledContextFailsFast(t *testing.T) {
	backend := newTestBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
		EventId:   uuid.NewString(),
		EventType: "Canceled",
		Data:      map[string]any{"id": "1"},
		Metadata:  map[string]any{},
	}}, "test", nil, nil)
	if statuscode.CodeOf(err) != statuscode.Canceled {
		t.Fatalf("Save with canceled context got %v, want CANCELED", err)
	}

	_, err = backend.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Count:     1,
		Direction: eventstore.Direction_ASC,
	})
	if statuscode.CodeOf(err) != statuscode.Canceled {
		t.Fatalf("Get with canceled context got %v, want CANCELED", err)
	}

	err = backend.CreateBoundaryIndex(ctx, "test", "canceled_idx", []eventstore.BoundaryIndexField{
		{JsonKey: "id", ValueType: "text"},
	}, nil, eventstore.IndexCombinatorAND)
	if statuscode.CodeOf(err) != statuscode.Canceled {
		t.Fatalf("CreateBoundaryIndex with canceled context got %v, want CANCELED", err)
	}
}

// TestFoundationDBPagingFromPosition walks a boundary in pages using the
// publisher's cursor convention (commit, prepare+1). Regression test for the
// scan that filtered by position AFTER applying the range limit: page 2 would
// come back empty because the limit was consumed by pre-cursor keys.
func TestFoundationDBPagingFromPosition(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	const batches = 3
	const perBatch = 10
	for i := 0; i < batches; i++ {
		events := make([]eventstore.EventWithMapTags, perBatch)
		for j := 0; j < perBatch; j++ {
			events[j] = eventstore.EventWithMapTags{
				EventId:   uuid.NewString(),
				EventType: "Paged",
				Data:      map[string]any{"batch": strconv.Itoa(i), "n": strconv.Itoa(j)},
				Metadata:  map[string]any{},
			}
		}
		if _, _, err := backend.Save(ctx, events, "test", nil, nil); err != nil {
			t.Fatalf("Save batch %d: %v", i, err)
		}
	}

	seen := map[string]struct{}{}
	var cursor *eventstore.Position
	pages := 0
	for {
		req := &eventstore.GetEventsRequest{
			Boundary:  "test",
			Count:     perBatch,
			Direction: eventstore.Direction_ASC,
		}
		if cursor != nil {
			req.FromPosition = &eventstore.Position{
				CommitPosition:  cursor.CommitPosition,
				PreparePosition: cursor.PreparePosition + 1,
			}
		}
		resp, err := backend.GetBatch(ctx, req)
		if err != nil {
			t.Fatalf("Get page %d: %v", pages, err)
		}
		if len(resp) == 0 {
			break
		}
		pages++
		if pages > batches+1 {
			t.Fatalf("paging did not terminate")
		}
		prev := cursor
		for _, event := range resp {
			eventPos := readEventPosition(event)
			if prev != nil && comparePositions(eventPos, prev) <= 0 {
				t.Fatalf("event %s position %v not after %v", event.EventId, eventPos, prev)
			}
			prev = eventPos
			if _, dup := seen[event.EventId]; dup {
				t.Fatalf("event %s returned twice", event.EventId)
			}
			seen[event.EventId] = struct{}{}
		}
		cursor = readEventPosition(resp[len(resp)-1])
	}
	if len(seen) != batches*perBatch {
		t.Fatalf("expected %d events across pages, got %d in %d pages", batches*perBatch, len(seen), pages)
	}

	// DESC from the newest cursor must page backwards without duplicates.
	desc, err := backend.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:     "test",
		Count:        perBatch,
		Direction:    eventstore.Direction_DESC,
		FromPosition: cursor,
	})
	if err != nil {
		t.Fatalf("Get DESC: %v", err)
	}
	if len(desc) != perBatch {
		t.Fatalf("expected %d DESC events, got %d", perBatch, len(desc))
	}
	if comparePositions(readEventPosition(desc[0]), cursor) != 0 {
		t.Fatalf("DESC page should start at the inclusive cursor")
	}
}

// TestFoundationDBCCCSuccessAndStaleExpected drives a single aggregate through
// the optimistic-lock cycle: first write against an empty context, follow-up
// write with the returned position, then a stale write that must conflict.
func TestFoundationDBCCCSuccessAndStaleExpected(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if err := backend.CreateBoundaryIndex(ctx, "test", "agg", []eventstore.BoundaryIndexField{
		{JsonKey: "agg_id", ValueType: "text"},
	}, nil, eventstore.IndexCombinatorAND); err != nil {
		t.Fatalf("CreateBoundaryIndex: %v", err)
	}
	criteria := &eventstore.Query{Criteria: []*eventstore.Criterion{
		{Tags: []*eventstore.Tag{{Key: "agg_id", Value: "agg-1"}}},
	}}

	notExists := eventstore.NotExistsPosition()
	txID, gid, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
		EventId:   uuid.NewString(),
		EventType: "Created",
		Data:      map[string]any{"agg_id": "agg-1"},
		Metadata:  map[string]any{},
	}}, "test", &notExists, criteria)
	if err != nil {
		t.Fatalf("first Save: %v", err)
	}
	commit, err := strconv.ParseInt(txID, 10, 64)
	if err != nil {
		t.Fatalf("parse commit position: %v", err)
	}
	current := eventstore.Position{CommitPosition: commit, PreparePosition: gid}

	if _, _, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
		EventId:   uuid.NewString(),
		EventType: "Updated",
		Data:      map[string]any{"agg_id": "agg-1"},
		Metadata:  map[string]any{},
	}}, "test", &current, criteria); err != nil {
		t.Fatalf("Save with correct expected position: %v", err)
	}

	if _, _, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
		EventId:   uuid.NewString(),
		EventType: "Updated",
		Data:      map[string]any{"agg_id": "agg-1"},
		Metadata:  map[string]any{},
	}}, "test", &current, criteria); statuscode.CodeOf(err) != statuscode.AlreadyExists {
		t.Fatalf("expected ALREADY_EXISTS for stale expected position, got %v", err)
	}
}

// TestFoundationDBUserRename: renaming a user must stop the old username from
// resolving — both in FoundationDB and in the auth cache.
func TestFoundationDBUserRename(t *testing.T) {
	backend := newTestBackend(t)

	user := eventstore.User{Id: "u-1", Name: "Ada", Username: "ada", HashedPassword: "h1", Roles: []eventstore.Role{eventstore.RoleAdmin}}
	if err := backend.UpsertUser(context.Background(), user); err != nil {
		t.Fatalf("UpsertUser: %v", err)
	}
	if _, err := backend.GetUserByUsername(context.Background(), "ada"); err != nil {
		t.Fatalf("GetUserByUsername(ada): %v", err)
	}

	user.Username = "ada2"
	if err := backend.UpsertUser(context.Background(), user); err != nil {
		t.Fatalf("UpsertUser rename: %v", err)
	}
	if _, err := backend.GetUserByUsername(context.Background(), "ada"); err == nil {
		t.Fatalf("old username must not resolve after rename")
	}
	got, err := backend.GetUserByUsername(context.Background(), "ada2")
	if err != nil {
		t.Fatalf("GetUserByUsername(ada2): %v", err)
	}
	if got.Id != "u-1" {
		t.Fatalf("expected user u-1, got %q", got.Id)
	}
}

func TestFoundationDBGetEventsCountPaged(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	const total = 25
	for i := 0; i < total; i++ {
		if _, _, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
			EventId:   uuid.NewString(),
			EventType: "Counted",
			Data:      map[string]any{"n": strconv.Itoa(i)},
			Metadata:  map[string]any{},
		}}, "test", nil, nil); err != nil {
			t.Fatalf("Save %d: %v", i, err)
		}
	}
	count, err := backend.GetEventsCount(context.Background(), "test")
	if err != nil {
		t.Fatalf("GetEventsCount: %v", err)
	}
	if count != total {
		t.Fatalf("expected %d events, got %d", total, count)
	}
}

func TestFoundationDBRecreatedIndexUsesFreshGeneration(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	if _, _, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
		EventId:   uuid.NewString(),
		EventType: "AccountOpened",
		Data:      map[string]any{"account_id": "acct-1"},
		Metadata:  map[string]any{},
	}}, "test", nil, nil); err != nil {
		t.Fatalf("Save: %v", err)
	}

	fields := []eventstore.BoundaryIndexField{{JsonKey: "account_id", ValueType: "text"}}
	if err := backend.CreateBoundaryIndex(ctx, "test", "account", fields, nil, eventstore.IndexCombinatorAND); err != nil {
		t.Fatalf("CreateBoundaryIndex: %v", err)
	}
	firstGeneration := readIndexGeneration(t, backend, "test", "account")
	if firstGeneration == "" {
		t.Fatalf("expected generated index metadata generation")
	}

	if err := backend.DropBoundaryIndex(ctx, "test", "account"); err != nil {
		t.Fatalf("DropBoundaryIndex: %v", err)
	}
	if err := backend.CreateBoundaryIndex(ctx, "test", "account", fields, nil, eventstore.IndexCombinatorAND); err != nil {
		t.Fatalf("recreate CreateBoundaryIndex: %v", err)
	}
	secondGeneration := readIndexGeneration(t, backend, "test", "account")
	if secondGeneration == "" || secondGeneration == firstGeneration {
		t.Fatalf("expected fresh generation, got first=%q second=%q", firstGeneration, secondGeneration)
	}

	resp, err := backend.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Count:     10,
		Direction: eventstore.Direction_ASC,
		Query: &eventstore.Query{Criteria: []*eventstore.Criterion{
			{Tags: []*eventstore.Tag{{Key: "account_id", Value: "acct-1"}}},
		}},
	})
	if err != nil {
		t.Fatalf("Get with recreated index: %v", err)
	}
	if len(resp) != 1 {
		t.Fatalf("expected recreated index to find 1 event, got %d", len(resp))
	}
}

// TestFoundationDBTotalOrderUnderConcurrency: boundary-wide total order is a
// core Orisun guarantee. Many goroutines append in parallel (no consistency
// condition, so nothing serialises them app-side); afterwards a paged read of
// the boundary must yield every event exactly once in strictly increasing
// position order, and batches must never interleave.
func TestFoundationDBTotalOrderUnderConcurrency(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	const writers = 8
	const savesPerWriter = 20
	const eventsPerSave = 3

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < savesPerWriter; i++ {
				events := make([]eventstore.EventWithMapTags, eventsPerSave)
				for j := range events {
					events[j] = eventstore.EventWithMapTags{
						EventId:   uuid.NewString(),
						EventType: "Ordered",
						Data:      map[string]any{"writer": strconv.Itoa(w), "save": strconv.Itoa(i), "j": strconv.Itoa(j)},
						Metadata:  map[string]any{},
					}
				}
				if _, _, err := backend.Save(ctx, events, "test", nil, nil); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Save: %v", err)
	}

	const total = writers * savesPerWriter * eventsPerSave
	seen := map[string]struct{}{}
	var prev *eventstore.Position
	var prevCommit int64 = -1
	var cursor *eventstore.Position
	for {
		req := &eventstore.GetEventsRequest{Boundary: "test", Count: 50, Direction: eventstore.Direction_ASC}
		if cursor != nil {
			req.FromPosition = &eventstore.Position{
				CommitPosition:  cursor.CommitPosition,
				PreparePosition: cursor.PreparePosition + 1,
			}
		}
		resp, err := backend.GetBatch(ctx, req)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(resp) == 0 {
			break
		}
		for _, event := range resp {
			eventPos := readEventPosition(event)
			if prev != nil && comparePositions(eventPos, prev) <= 0 {
				t.Fatalf("total order violated: %v after %v", eventPos, prev)
			}
			// A batch shares one commit position; once the commit position
			// advances it must never come back (batches cannot interleave).
			if event.CommitPosition < prevCommit {
				t.Fatalf("batch interleaving: commit %d after %d", event.CommitPosition, prevCommit)
			}
			prevCommit = event.CommitPosition
			prev = eventPos
			if _, dup := seen[event.EventId]; dup {
				t.Fatalf("event %s seen twice", event.EventId)
			}
			seen[event.EventId] = struct{}{}
		}
		cursor = readEventPosition(resp[len(resp)-1])
	}
	if len(seen) != total {
		t.Fatalf("expected %d events in total order, got %d", total, len(seen))
	}
}

func newTestBackend(tb testing.TB) *Backend {
	tb.Helper()
	clusterFile := startFDBCluster(tb)
	apiVersion := defaultAPIVersion
	if raw := os.Getenv("ORISUN_FDB_TEST_API_VERSION"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			tb.Fatalf("invalid ORISUN_FDB_TEST_API_VERSION: %v", err)
		}
		apiVersion = parsed
	}
	if err := ensureAPIVersion(apiVersion); err != nil {
		tb.Fatalf("FoundationDB API version: %v", err)
	}
	db, err := fdb.OpenDatabase(clusterFile)
	if err != nil {
		tb.Fatalf("open FoundationDB: %v", err)
	}
	root := "orisun_test_" + uuid.NewString()
	backend := &Backend{
		db:            db,
		root:          root,
		adminBoundary: "orisun_admin",
		boundaries: map[string]struct{}{
			"test":         {},
			"orisun_admin": {},
		},
		logger:    logging.InitializeDefaultLogger(configForTestLogger()),
		userCache: make(map[string]*eventstore.User),
	}
	tb.Cleanup(func() {
		_, _ = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.ClearRange(prefixRange(fdb.Key(tuple.Tuple{root}.Pack())))
			return nil, nil
		})
		db.Close()
	})
	return backend
}

func readIndexGeneration(tb testing.TB, backend *Backend, boundary, name string) string {
	tb.Helper()
	result, err := backend.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		raw := rt.Get(backend.indexMetaKey(boundary, name)).MustGet()
		if raw == nil {
			return "", nil
		}
		var def indexDefinition
		if err := json.Unmarshal(raw, &def); err != nil {
			return "", err
		}
		return def.Generation, nil
	})
	if err != nil {
		tb.Fatalf("read index generation: %v", err)
	}
	return result.(string)
}

func configForTestLogger() config.LoggingConfig {
	return config.LoggingConfig{Level: "ERROR"}
}
