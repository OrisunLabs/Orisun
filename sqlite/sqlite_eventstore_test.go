package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

func newTestPools(t *testing.T) (map[string]*BoundaryPools, func()) {
	t.Helper()
	dir := t.TempDir()
	const boundary = "test"
	logger, _ := logging.ZapLogger("error")
	_ = logger
	bp, err := OpenBoundaryPools(context.Background(), dir, boundary, boundary)
	if err != nil {
		t.Fatalf("open pools: %v", err)
	}
	pools := map[string]*BoundaryPools{boundary: bp}
	return pools, func() { _ = bp.Close() }
}

func newTestPoolsWithMetadata(t *testing.T) (map[string]*BoundaryPools, map[string]*BoundaryPools, func()) {
	t.Helper()
	dir := t.TempDir()
	const boundary = "test"
	bp, err := OpenBoundaryPools(context.Background(), dir, boundary, boundary)
	if err != nil {
		t.Fatalf("open pools: %v", err)
	}
	metadataPool, err := OpenMetadataPoolsWithConfig(context.Background(), config.SqliteConfig{Dir: dir}, boundary)
	if err != nil {
		_ = bp.Close()
		t.Fatalf("open metadata pool: %v", err)
	}
	pools := map[string]*BoundaryPools{boundary: bp}
	metadataPools := map[string]*BoundaryPools{boundary: metadataPool}
	return pools, metadataPools, func() {
		_ = metadataPool.Close()
		_ = bp.Close()
	}
}

func tableExists(conn *sqlite.Conn, tableName string) (bool, error) {
	found := false
	err := sqlitex.Execute(conn,
		"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
		&sqlitex.ExecOptions{
			Args: []any{tableName},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				found = true
				return nil
			},
		})
	return found, err
}

func mustEvent(t *testing.T, eventType string, data, meta map[string]any) eventstore.EventWithMapTags {
	t.Helper()
	return eventstore.EventWithMapTags{
		EventId:   uuid.NewString(),
		EventType: eventType,
		Data:      data,
		Metadata:  meta,
	}
}

func TestSave_RoundTrip(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")

	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)

	ctx := context.Background()
	events := []eventstore.EventWithMapTags{
		mustEvent(t, "Foo", map[string]any{"k": "v1"}, map[string]any{}),
		mustEvent(t, "Bar", map[string]any{"k": "v2"}, map[string]any{}),
	}

	tx, gid, err := saver.Save(ctx, events, "test", nil, nil)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	if tx == "" || gid <= 0 {
		t.Fatalf("expected non-zero tx/gid, got tx=%q gid=%d", tx, gid)
	}

	resp, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Direction: eventstore.Direction_ASC,
		Count:     100,
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp) != 2 {
		t.Fatalf("expected 2 events, got %d", len(resp))
	}
	// Both events in the batch should share transaction_id == max global_id
	if resp[0].CommitPosition != resp[1].CommitPosition {
		t.Errorf("expected shared tx_id within batch, got %d vs %d",
			resp[0].CommitPosition, resp[1].CommitPosition)
	}
	if resp[1].PreparePosition != gid {
		t.Errorf("last event global_id should equal returned gid: got %d, want %d",
			resp[1].PreparePosition, gid)
	}
}

func TestSave_ChunksBatchLargerThanSqliteParamLimit(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")

	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)
	ctx := context.Background()

	batchSize := sqliteMaxEventsPerInsert*2 + 3
	events := make([]eventstore.EventWithMapTags, batchSize)
	for i := range events {
		events[i] = mustEvent(t, "Chunked", map[string]any{"seq": i}, map[string]any{})
	}

	tx, gid, err := saver.Save(ctx, events, "test", nil, nil)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	resp, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Direction: eventstore.Direction_ASC,
		Count:     uint32(batchSize + 10),
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp) != batchSize {
		t.Fatalf("expected %d events, got %d", batchSize, len(resp))
	}
	// All chunks committed in one transaction: every event shares tx_id == max global_id.
	for i, e := range resp {
		if got := strconv.FormatInt(e.CommitPosition, 10); got != tx {
			t.Fatalf("event %d: expected tx_id %s, got %s", i, tx, got)
		}
	}
	if last := resp[batchSize-1].PreparePosition; last != gid {
		t.Fatalf("last event global_id should equal returned gid: got %d, want %d", last, gid)
	}
}

func TestSave_AddsEventTypeToData(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")

	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)
	ctx := context.Background()

	_, _, err := saver.Save(ctx, []eventstore.EventWithMapTags{
		mustEvent(t, "OrderPlaced", map[string]any{
			"order_id":  "order-1",
			"eventType": "stale",
		}, map[string]any{}),
	}, "test", nil, nil)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	resp, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Direction: eventstore.Direction_ASC,
		Count:     100,
		Query: &eventstore.Query{
			Criteria: []*eventstore.Criterion{
				{Tags: []*eventstore.Tag{{Key: "eventType", Value: "OrderPlaced"}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp) != 1 {
		t.Fatalf("expected 1 matching event, got %d", len(resp))
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(resp[0].Data), &data); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if data["eventType"] != "OrderPlaced" {
		t.Fatalf("expected canonical eventType in data, got %v", data["eventType"])
	}
	if resp[0].EventType != "OrderPlaced" {
		t.Fatalf("expected EventType from data.eventType, got %q", resp[0].EventType)
	}
	if data["order_id"] != "order-1" {
		t.Fatalf("expected original data to be preserved, got %v", data["order_id"])
	}
}

func TestSave_RejectsEmpty(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")

	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	_, _, err := saver.Save(context.Background(), nil, "test", nil, nil)
	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestSave_RejectsUnknownBoundary(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")

	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	_, _, err := saver.Save(context.Background(),
		[]eventstore.EventWithMapTags{mustEvent(t, "X", map[string]any{}, map[string]any{})},
		"missing", nil, nil)
	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

func TestSave_RejectsInvalidJSONStrings(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()

	_, _, err := saver.Save(context.Background(),
		[]eventstore.EventWithMapTags{{
			EventId:   uuid.NewString(),
			EventType: "BadData",
			Data:      `{"broken":`,
			Metadata:  map[string]any{},
		}},
		"test", nil, nil)
	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("expected InvalidArgument for invalid data JSON, got %v", err)
	}

	_, _, err = saver.Save(context.Background(),
		[]eventstore.EventWithMapTags{{
			EventId:   uuid.NewString(),
			EventType: "BadMetadata",
			Data:      map[string]any{},
			Metadata:  []byte(`{"broken":`),
		}},
		"test", nil, nil)
	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("expected InvalidArgument for invalid metadata JSON, got %v", err)
	}
}

func TestSave_CCCViolation(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	ctx := context.Background()

	// First write — establishes a position matching criteria {"agg":"a1"}
	criteria := &eventstore.Query{
		Criteria: []*eventstore.Criterion{
			{Tags: []*eventstore.Tag{{Key: "agg", Value: "a1"}}},
		},
	}
	_, gid, err := saver.Save(ctx,
		[]eventstore.EventWithMapTags{mustEvent(t, "Created", map[string]any{"agg": "a1"}, map[string]any{})},
		"test", nil, criteria)
	if err != nil {
		t.Fatalf("first save: %v", err)
	}

	// Second write with a stale expectedPosition (-1,-1 / nil) should fail.
	_, _, err = saver.Save(ctx,
		[]eventstore.EventWithMapTags{mustEvent(t, "Updated", map[string]any{"agg": "a1"}, map[string]any{})},
		"test", nil, criteria)
	if statuscode.CodeOf(err) != statuscode.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got %v", err)
	}
	if !strings.Contains(err.Error(), "OptimisticConcurrencyException") {
		t.Errorf("expected OptimisticConcurrencyException in error message, got %v", err)
	}

	// Third write with correct expectedPosition (the gid we got back) should succeed.
	expected := &eventstore.Position{CommitPosition: gid, PreparePosition: gid}
	_, _, err = saver.Save(ctx,
		[]eventstore.EventWithMapTags{mustEvent(t, "Updated", map[string]any{"agg": "a1"}, map[string]any{})},
		"test", expected, criteria)
	if err != nil {
		t.Fatalf("expected success with correct expected position, got: %v", err)
	}
}

func TestGet_FilterByCriteria(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)
	ctx := context.Background()

	_, _, err := saver.Save(ctx, []eventstore.EventWithMapTags{
		mustEvent(t, "A", map[string]any{"agg": "1"}, map[string]any{}),
		mustEvent(t, "A", map[string]any{"agg": "2"}, map[string]any{}),
		mustEvent(t, "A", map[string]any{"agg": "1"}, map[string]any{}),
	}, "test", nil, nil)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	resp, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Direction: eventstore.Direction_ASC,
		Count:     100,
		Query: &eventstore.Query{
			Criteria: []*eventstore.Criterion{
				{Tags: []*eventstore.Tag{{Key: "agg", Value: "1"}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp) != 2 {
		t.Fatalf("expected 2 matching events, got %d", len(resp))
	}
}

func TestGet_FilterByUntypedScalarCriteria(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)
	ctx := context.Background()

	_, _, err := saver.Save(ctx, []eventstore.EventWithMapTags{
		mustEvent(t, "A", map[string]any{"amount": 45, "active": true}, map[string]any{}),
		mustEvent(t, "A", map[string]any{"amount": 46, "active": false}, map[string]any{}),
		mustEvent(t, "A", map[string]any{"big": 1e10, "deleted": nil}, map[string]any{}),
	}, "test", nil, nil)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	for _, tt := range []struct {
		name string
		tag  *eventstore.Tag
		want int
	}{
		{name: "number", tag: &eventstore.Tag{Key: "amount", Value: "45"}, want: 1},
		{name: "boolean", tag: &eventstore.Tag{Key: "active", Value: "true"}, want: 1},
		{name: "number decimal form", tag: &eventstore.Tag{Key: "big", Value: "10000000000"}, want: 1},
		{name: "number exponent form does not match decimal rendering", tag: &eventstore.Tag{Key: "big", Value: "1e10"}, want: 0},
		// PG ->> parity: JSON null renders as SQL NULL, never matches anything.
		{name: "json null never matches", tag: &eventstore.Tag{Key: "deleted", Value: "null"}, want: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
				Boundary:  "test",
				Direction: eventstore.Direction_ASC,
				Count:     100,
				Query: &eventstore.Query{
					Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{tt.tag}}},
				},
			})
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if len(resp) != tt.want {
				t.Fatalf("expected %d matching events, got %d", tt.want, len(resp))
			}
		})
	}
}

func TestGet_OrderAndFromPosition(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, _, err := saver.Save(ctx, []eventstore.EventWithMapTags{
			mustEvent(t, "Ordered", map[string]any{"index": i}, map[string]any{}),
		}, "test", nil, nil); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	respAsc, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Direction: eventstore.Direction_ASC,
		Count:     10,
	})
	if err != nil {
		t.Fatalf("get asc: %v", err)
	}
	respDesc, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Direction: eventstore.Direction_DESC,
		Count:     10,
	})
	if err != nil {
		t.Fatalf("get desc: %v", err)
	}
	if len(respAsc) != 5 || len(respDesc) != 5 {
		t.Fatalf("expected 5 asc and desc events, got %d and %d", len(respAsc), len(respDesc))
	}
	for i := range respAsc {
		if respAsc[i].EventId != respDesc[4-i].EventId {
			t.Fatalf("desc order mismatch at %d", i)
		}
	}

	from := &eventstore.Position{
		CommitPosition:  respAsc[2].CommitPosition,
		PreparePosition: respAsc[2].PreparePosition,
	}
	respFromAsc, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:     "test",
		Direction:    eventstore.Direction_ASC,
		Count:        10,
		FromPosition: from,
	})
	if err != nil {
		t.Fatalf("get from asc: %v", err)
	}
	if len(respFromAsc) != 3 || respFromAsc[0].EventId != respAsc[2].EventId {
		t.Fatalf("expected inclusive ASC page from third event, got %d events", len(respFromAsc))
	}

	respFromDesc, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:     "test",
		Direction:    eventstore.Direction_DESC,
		Count:        10,
		FromPosition: from,
	})
	if err != nil {
		t.Fatalf("get from desc: %v", err)
	}
	if len(respFromDesc) != 3 || respFromDesc[0].EventId != respAsc[2].EventId {
		t.Fatalf("expected inclusive DESC page from third event, got %d events", len(respFromDesc))
	}
}

func TestEventPublishing_EmptyCheckpointReturnsNotExists(t *testing.T) {
	_, metadataPools, cleanup := newTestPoolsWithMetadata(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	tracker := NewSqliteEventPublishingWithMetadata(metadataPools, logger)

	pos, err := tracker.GetLastPublishedEventPosition(context.Background(), "test")
	if err != nil {
		t.Fatalf("get last published position: %v", err)
	}
	want := eventstore.NotExistsPosition()
	if pos.CommitPosition != want.CommitPosition || pos.PreparePosition != want.PreparePosition {
		t.Fatalf("expected not-exists position, got commit=%d prepare=%d", pos.CommitPosition, pos.PreparePosition)
	}
}

func TestSqliteMetadataTablesAreSeparateFromBoundaryEventDB(t *testing.T) {
	pools, metadataPools, cleanup := newTestPoolsWithMetadata(t)
	defer cleanup()
	ctx := context.Background()

	boundaryConn, err := pools["test"].Read.Take(ctx)
	if err != nil {
		t.Fatalf("take boundary conn: %v", err)
	}
	defer pools["test"].Read.Put(boundaryConn)
	metadataConn, err := metadataPools["test"].Read.Take(ctx)
	if err != nil {
		t.Fatalf("take metadata conn: %v", err)
	}
	defer metadataPools["test"].Read.Put(metadataConn)

	for _, table := range []string{"orisun_es_event", "orisun_es_seq", "orisun_boundary_index_metadata"} {
		exists, err := tableExists(boundaryConn, table)
		if err != nil {
			t.Fatalf("boundary table check %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("expected boundary DB to contain %s", table)
		}
	}
	for _, table := range []string{"orisun_last_published_event_position", "events_count", "projector_checkpoint", "users", "users_count"} {
		exists, err := tableExists(boundaryConn, table)
		if err != nil {
			t.Fatalf("boundary table check %s: %v", table, err)
		}
		if exists {
			t.Fatalf("boundary DB should not contain metadata table %s", table)
		}
		exists, err = tableExists(metadataConn, table)
		if err != nil {
			t.Fatalf("metadata table check %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("expected metadata DB to contain %s", table)
		}
	}
}

func TestSqliteMetadataDBIsPerBoundary(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	logger, _ := logging.ZapLogger("error")

	pools := make(map[string]*BoundaryPools)
	metadataPools := make(map[string]*BoundaryPools)
	for _, boundary := range []string{"test", "other"} {
		bp, err := OpenBoundaryPools(ctx, dir, boundary, "test")
		if err != nil {
			t.Fatalf("open boundary pool %s: %v", boundary, err)
		}
		defer bp.Close()
		mp, err := OpenMetadataPoolsWithConfig(ctx, config.SqliteConfig{Dir: dir}, boundary)
		if err != nil {
			t.Fatalf("open metadata pool %s: %v", boundary, err)
		}
		defer mp.Close()
		pools[boundary] = bp
		metadataPools[boundary] = mp

		if _, err := os.Stat(filepath.Join(dir, boundary+"_metadata.db")); err != nil {
			t.Fatalf("expected metadata file for %s: %v", boundary, err)
		}
	}

	tracker := NewSqliteEventPublishingWithMetadata(metadataPools, logger)
	if err := tracker.InsertLastPublishedEvent(ctx, "test", 10, 9); err != nil {
		t.Fatalf("insert test publish checkpoint: %v", err)
	}
	if err := tracker.InsertLastPublishedEvent(ctx, "other", 20, 19); err != nil {
		t.Fatalf("insert other publish checkpoint: %v", err)
	}
	testPos, err := tracker.GetLastPublishedEventPosition(ctx, "test")
	if err != nil {
		t.Fatalf("get test publish checkpoint: %v", err)
	}
	otherPos, err := tracker.GetLastPublishedEventPosition(ctx, "other")
	if err != nil {
		t.Fatalf("get other publish checkpoint: %v", err)
	}
	if testPos.CommitPosition != 10 || testPos.PreparePosition != 9 {
		t.Fatalf("unexpected test publish position: (%d, %d)", testPos.CommitPosition, testPos.PreparePosition)
	}
	if otherPos.CommitPosition != 20 || otherPos.PreparePosition != 19 {
		t.Fatalf("unexpected other publish position: (%d, %d)", otherPos.CommitPosition, otherPos.PreparePosition)
	}

	admin := NewSqliteAdminDBWithMetadata(pools, metadataPools, "test", logger)
	if err := admin.SaveEventCount(context.Background(), 11, "test"); err != nil {
		t.Fatalf("save test event count: %v", err)
	}
	if err := admin.SaveEventCount(context.Background(), 22, "other"); err != nil {
		t.Fatalf("save other event count: %v", err)
	}

	assertBoundaryCount := func(boundary, unexpectedBoundary string, want int64) {
		t.Helper()
		conn, err := metadataPools[boundary].Read.Take(ctx)
		if err != nil {
			t.Fatalf("take metadata conn %s: %v", boundary, err)
		}
		defer metadataPools[boundary].Read.Put(conn)

		var got int64
		err = sqlitex.Execute(conn,
			"SELECT COUNT(*) FROM events_count WHERE boundary = ?",
			&sqlitex.ExecOptions{
				Args: []any{unexpectedBoundary},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					got = stmt.ColumnInt64(0)
					return nil
				},
			})
		if err != nil {
			t.Fatalf("count boundary rows in %s metadata: %v", boundary, err)
		}
		if got != want {
			t.Fatalf("expected %d rows for %s in %s metadata, got %d", want, unexpectedBoundary, boundary, got)
		}
	}
	assertBoundaryCount("test", "test", 1)
	assertBoundaryCount("test", "other", 0)
	assertBoundaryCount("other", "other", 1)
	assertBoundaryCount("other", "test", 0)
}

func TestCreateDropBoundaryIndex_MetadataAndTypedCriteria(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	ctx := context.Background()

	admin := NewSqliteAdminDB(pools, "test", logger)
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)
	pool := pools["test"]

	indexExists := func(name string) bool {
		conn, err := pool.Read.Take(ctx)
		if err != nil {
			t.Fatalf("take conn: %v", err)
		}
		defer pool.Read.Put(conn)

		found := false
		err = sqlitex.Execute(conn,
			"SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?",
			&sqlitex.ExecOptions{
				Args: []any{name},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					found = true
					return nil
				},
			})
		if err != nil {
			t.Fatalf("check index: %v", err)
		}
		return found
	}

	metadataCount := func(name string) int {
		conn, err := pool.Read.Take(ctx)
		if err != nil {
			t.Fatalf("take conn: %v", err)
		}
		defer pool.Read.Put(conn)

		count := 0
		err = sqlitex.Execute(conn,
			"SELECT COUNT(*) FROM orisun_boundary_index_metadata WHERE name = ?",
			&sqlitex.ExecOptions{
				Args: []any{name},
				ResultFunc: func(stmt *sqlite.Stmt) error {
					count = int(stmt.ColumnInt64(0))
					return nil
				},
			})
		if err != nil {
			t.Fatalf("check metadata: %v", err)
		}
		return count
	}

	err := admin.CreateBoundaryIndex(ctx, "test", "amount",
		[]eventstore.BoundaryIndexField{{JsonKey: "amount", ValueType: "numeric"}},
		nil,
		"")
	if err != nil {
		t.Fatalf("create index: %v", err)
	}
	if !indexExists("amount_idx") {
		t.Fatal("expected index to exist")
	}
	if got := metadataCount("amount"); got != 1 {
		t.Fatalf("expected one metadata row, got %d", got)
	}

	sql, err := buildCriteriaSQLForBoundary([]map[string]any{{"amount": "45"}}, pool.indexes, "test")
	if err != nil {
		t.Fatalf("build criteria: %v", err)
	}
	if !strings.Contains(sql, `CAST(json_extract(data, '$."amount"') AS REAL) = 45`) {
		t.Fatalf("expected inlined numeric comparison in criteria SQL, got %q", sql)
	}

	_, _, err = saver.Save(ctx, []eventstore.EventWithMapTags{
		mustEvent(t, "Priced", map[string]any{"amount": 45}, map[string]any{}),
	}, "test", nil, nil)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	resp, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
		Boundary:  "test",
		Direction: eventstore.Direction_ASC,
		Count:     10,
		Query: &eventstore.Query{
			Criteria: []*eventstore.Criterion{
				{Tags: []*eventstore.Tag{{Key: "amount", Value: "45"}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp) != 1 {
		t.Fatalf("expected 1 event, got %d", len(resp))
	}

	if err := admin.DropBoundaryIndex(ctx, "test", "amount"); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if indexExists("amount_idx") {
		t.Fatal("expected index to be dropped")
	}
	if got := metadataCount("amount"); got != 0 {
		t.Fatalf("expected no metadata rows, got %d", got)
	}

	sql, err = buildCriteriaSQLForBoundary([]map[string]any{{"amount": "45"}}, pool.indexes, "test")
	if err != nil {
		t.Fatalf("build criteria: %v", err)
	}
	if strings.Contains(sql, "AS REAL") {
		t.Fatalf("expected untyped criteria SQL after dropping index, got %q", sql)
	}
	if !strings.Contains(sql, "CASE json_type") {
		t.Fatalf("expected semantic scalar comparison after dropping index, got %q", sql)
	}
}

func TestCreateDropBoundaryIndex_ValidationParity(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	admin := NewSqliteAdminDB(pools, "test", logger)
	ctx := context.Background()

	t.Run("composite index", func(t *testing.T) {
		if err := admin.CreateBoundaryIndex(ctx, "test", "cat_prio", []eventstore.BoundaryIndexField{
			{JsonKey: "category", ValueType: "text"},
			{JsonKey: "priority", ValueType: "text"},
		}, nil, ""); err != nil {
			t.Fatalf("create composite index: %v", err)
		}
		if err := admin.DropBoundaryIndex(ctx, "test", "cat_prio"); err != nil {
			t.Fatalf("drop composite index: %v", err)
		}
	})

	t.Run("partial index", func(t *testing.T) {
		if err := admin.CreateBoundaryIndex(ctx, "test", "placed_amount", []eventstore.BoundaryIndexField{
			{JsonKey: "amount", ValueType: "numeric"},
		}, []eventstore.BoundaryIndexCondition{
			{Key: "eventType", Operator: "=", Value: "OrderPlaced"},
		}, eventstore.IndexCombinatorAND); err != nil {
			t.Fatalf("create partial index: %v", err)
		}
		if err := admin.DropBoundaryIndex(ctx, "test", "placed_amount"); err != nil {
			t.Fatalf("drop partial index: %v", err)
		}
	})

	t.Run("unknown boundary", func(t *testing.T) {
		err := admin.CreateBoundaryIndex(ctx, "missing", "idx", []eventstore.BoundaryIndexField{
			{JsonKey: "id", ValueType: "text"},
		}, nil, "")
		if err == nil || !strings.Contains(err.Error(), "unknown boundary") {
			t.Fatalf("expected unknown boundary error, got %v", err)
		}
	})

	t.Run("no fields", func(t *testing.T) {
		err := admin.CreateBoundaryIndex(ctx, "test", "empty", nil, nil, "")
		if err == nil || !strings.Contains(err.Error(), "at least one field") {
			t.Fatalf("expected no fields error, got %v", err)
		}
	})

	t.Run("invalid operator", func(t *testing.T) {
		err := admin.CreateBoundaryIndex(ctx, "test", "bad_op", []eventstore.BoundaryIndexField{
			{JsonKey: "id", ValueType: "text"},
		}, []eventstore.BoundaryIndexCondition{
			{Key: "eventType", Operator: "LIKE", Value: "Order%"},
		}, "")
		if err == nil || !strings.Contains(err.Error(), "invalid operator") {
			t.Fatalf("expected invalid operator error, got %v", err)
		}
	})

	t.Run("invalid combinator", func(t *testing.T) {
		err := admin.CreateBoundaryIndex(ctx, "test", "bad_comb", []eventstore.BoundaryIndexField{
			{JsonKey: "id", ValueType: "text"},
		}, []eventstore.BoundaryIndexCondition{
			{Key: "eventType", Operator: "=", Value: "Placed"},
		}, "XOR")
		if err == nil || !strings.Contains(err.Error(), "invalid combinator") {
			t.Fatalf("expected invalid combinator error, got %v", err)
		}
	})

	t.Run("drop unknown boundary", func(t *testing.T) {
		err := admin.DropBoundaryIndex(ctx, "missing", "idx")
		if err == nil || !strings.Contains(err.Error(), "unknown boundary") {
			t.Fatalf("expected unknown boundary error, got %v", err)
		}
	})

	t.Run("numeric condition emits numeric comparison", func(t *testing.T) {
		if err := admin.CreateBoundaryIndex(ctx, "test", "big_amount", []eventstore.BoundaryIndexField{
			{JsonKey: "amount", ValueType: "numeric"},
		}, []eventstore.BoundaryIndexCondition{
			{Key: "amount", Operator: ">", Value: "100"},
		}, ""); err != nil {
			t.Fatalf("create numeric-condition index: %v", err)
		}
		defer func() {
			if err := admin.DropBoundaryIndex(ctx, "test", "big_amount"); err != nil {
				t.Fatalf("drop index: %v", err)
			}
		}()

		pool := pools["test"]
		conn, err := pool.Read.Take(ctx)
		if err != nil {
			t.Fatalf("take conn: %v", err)
		}
		defer pool.Read.Put(conn)

		var ddl string
		err = sqlitex.Execute(conn,
			"SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'big_amount_idx'",
			&sqlitex.ExecOptions{
				ResultFunc: func(stmt *sqlite.Stmt) error {
					ddl = stmt.ColumnText(0)
					return nil
				},
			})
		if err != nil {
			t.Fatalf("read index ddl: %v", err)
		}
		// A text literal here would make the predicate always-false: in SQLite,
		// numbers sort before text, so json_extract(...) > '100' never matches.
		if !strings.Contains(ddl, `CAST(json_extract(data, '$."amount"') AS REAL) > 100`) {
			t.Fatalf("expected numeric predicate in index DDL, got %q", ddl)
		}
	})

	t.Run("numeric condition rejects non-numeric value", func(t *testing.T) {
		err := admin.CreateBoundaryIndex(ctx, "test", "bad_num", []eventstore.BoundaryIndexField{
			{JsonKey: "amount", ValueType: "numeric"},
		}, []eventstore.BoundaryIndexCondition{
			{Key: "amount", Operator: ">", Value: "lots"},
		}, "")
		if err == nil || !strings.Contains(err.Error(), "requires a finite numeric value") {
			t.Fatalf("expected numeric value error, got %v", err)
		}
	})

	t.Run("planner uses conditioned partial index for inlined criteria", func(t *testing.T) {
		if err := admin.CreateBoundaryIndex(ctx, "test", "placed_amount2", []eventstore.BoundaryIndexField{
			{JsonKey: "amount", ValueType: "numeric"},
		}, []eventstore.BoundaryIndexCondition{
			{Key: "eventType", Operator: "=", Value: "OrderPlaced"},
		}, ""); err != nil {
			t.Fatalf("create partial index: %v", err)
		}
		defer func() {
			if err := admin.DropBoundaryIndex(ctx, "test", "placed_amount2"); err != nil {
				t.Fatalf("drop index: %v", err)
			}
		}()

		pool := pools["test"]
		where, err := buildCriteriaSQLForBoundary([]map[string]any{
			{"eventType": "OrderPlaced", "amount": "150"},
		}, pool.indexes, "test")
		if err != nil {
			t.Fatalf("build criteria: %v", err)
		}

		conn, err := pool.Read.Take(ctx)
		if err != nil {
			t.Fatalf("take conn: %v", err)
		}
		defer pool.Read.Put(conn)

		var plan strings.Builder
		err = sqlitex.ExecuteTransient(conn,
			"EXPLAIN QUERY PLAN SELECT COUNT(*) FROM orisun_es_event WHERE "+where,
			&sqlitex.ExecOptions{
				ResultFunc: func(stmt *sqlite.Stmt) error {
					plan.WriteString(stmt.ColumnText(3))
					plan.WriteString("\n")
					return nil
				},
			})
		if err != nil {
			t.Fatalf("explain: %v", err)
		}
		// Bound parameters would fail SQLite's partial-index implication proof;
		// inlined literals must make the planner pick the conditioned index.
		if !strings.Contains(plan.String(), "placed_amount2_idx") {
			t.Fatalf("expected query plan to use placed_amount2_idx, got:\n%s", plan.String())
		}
	})

	t.Run("condition on undeclared key matches numeric JSON values", func(t *testing.T) {
		// status is not declared as a typed field anywhere; the condition predicate
		// must use the CASE scalar-text shape so stored JSON numbers still match —
		// a raw text comparison would make the partial index permanently empty.
		if err := admin.CreateBoundaryIndex(ctx, "test", "status404", []eventstore.BoundaryIndexField{
			{JsonKey: "amount", ValueType: "numeric"},
		}, []eventstore.BoundaryIndexCondition{
			{Key: "status", Operator: "=", Value: "404"},
		}, ""); err != nil {
			t.Fatalf("create index: %v", err)
		}
		defer func() {
			if err := admin.DropBoundaryIndex(ctx, "test", "status404"); err != nil {
				t.Fatalf("drop index: %v", err)
			}
		}()

		logger, _ := logging.ZapLogger("error")
		saver := NewSqliteSaveEvents(pools, logger)
		defer saver.close()
		getter := NewSqliteGetEvents(pools, logger)
		if _, _, err := saver.Save(ctx, []eventstore.EventWithMapTags{
			mustEvent(t, "S", map[string]any{"status": 404, "amount": 5}, map[string]any{}),
			mustEvent(t, "S", map[string]any{"status": "404", "amount": 6}, map[string]any{}),
			mustEvent(t, "S", map[string]any{"status": 500, "amount": 7}, map[string]any{}),
		}, "test", nil, nil); err != nil {
			t.Fatalf("save: %v", err)
		}

		resp, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
			Boundary:  "test",
			Direction: eventstore.Direction_ASC,
			Count:     100,
			Query: &eventstore.Query{
				Criteria: []*eventstore.Criterion{
					{Tags: []*eventstore.Tag{{Key: "status", Value: "404"}}},
				},
			},
		})
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		// PG ->> renders number 404 and string "404" identically as '404'.
		if len(resp) != 2 {
			t.Fatalf("expected 2 matching events (numeric and string status), got %d", len(resp))
		}

		// Condition key is registry-known after creation, so the query term exactly
		// matches the predicate and the planner can pick the partial index.
		where, err := buildCriteriaSQLForBoundary([]map[string]any{
			{"status": "404", "amount": "5"},
		}, pools["test"].indexes, "test")
		if err != nil {
			t.Fatalf("build criteria: %v", err)
		}
		conn, err := pools["test"].Read.Take(ctx)
		if err != nil {
			t.Fatalf("take conn: %v", err)
		}
		defer pools["test"].Read.Put(conn)
		var plan strings.Builder
		err = sqlitex.ExecuteTransient(conn,
			"EXPLAIN QUERY PLAN SELECT COUNT(*) FROM orisun_es_event WHERE "+where,
			&sqlitex.ExecOptions{
				ResultFunc: func(stmt *sqlite.Stmt) error {
					plan.WriteString(stmt.ColumnText(3))
					plan.WriteString("\n")
					return nil
				},
			})
		if err != nil {
			t.Fatalf("explain: %v", err)
		}
		if !strings.Contains(plan.String(), "status404_idx") {
			t.Fatalf("expected query plan to use status404_idx, got:\n%s", plan.String())
		}
	})

	t.Run("boolean condition emits integer comparison", func(t *testing.T) {
		if err := admin.CreateBoundaryIndex(ctx, "test", "active_only", []eventstore.BoundaryIndexField{
			{JsonKey: "active", ValueType: "boolean"},
		}, []eventstore.BoundaryIndexCondition{
			{Key: "active", Operator: "=", Value: "true"},
		}, ""); err != nil {
			t.Fatalf("create boolean-condition index: %v", err)
		}
		if err := admin.DropBoundaryIndex(ctx, "test", "active_only"); err != nil {
			t.Fatalf("drop index: %v", err)
		}
	})
}

func TestAdminUserCacheInvalidatedOnDelete(t *testing.T) {
	pools, metadataPools, cleanup := newTestPoolsWithMetadata(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	admin := NewSqliteAdminDBWithMetadata(pools, metadataPools, "test", logger)

	user := eventstore.User{
		Id:             uuid.NewString(),
		Name:           "Test User",
		Username:       "cache-delete-test",
		HashedPassword: "hash",
		Roles:          []eventstore.Role{eventstore.RoleAdmin},
	}
	if err := admin.UpsertUser(context.Background(), user); err != nil {
		t.Fatalf("upsert user: %v", err)
	}
	if _, err := admin.GetUserByUsername(context.Background(), user.Username); err != nil {
		t.Fatalf("expected cached user: %v", err)
	}
	if err := admin.DeleteUser(context.Background(), user.Id); err != nil {
		t.Fatalf("delete user: %v", err)
	}
	if _, err := admin.GetUserByUsername(context.Background(), user.Username); err == nil {
		t.Fatal("expected deleted user lookup to fail")
	}
}

func TestAdminUserCacheEvictedOnUsernameChange(t *testing.T) {
	pools, metadataPools, cleanup := newTestPoolsWithMetadata(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	admin := NewSqliteAdminDBWithMetadata(pools, metadataPools, "test", logger)

	user := eventstore.User{
		Id:             uuid.NewString(),
		Name:           "Test User",
		Username:       "old-name",
		HashedPassword: "hash",
		Roles:          []eventstore.Role{eventstore.RoleAdmin},
	}
	if err := admin.UpsertUser(context.Background(), user); err != nil {
		t.Fatalf("upsert user: %v", err)
	}
	if _, err := admin.GetUserByUsername(context.Background(), "old-name"); err != nil {
		t.Fatalf("expected cached user: %v", err)
	}

	user.Username = "new-name"
	if err := admin.UpsertUser(context.Background(), user); err != nil {
		t.Fatalf("rename user: %v", err)
	}
	if _, err := admin.GetUserByUsername(context.Background(), "old-name"); err == nil {
		t.Fatal("old username must stop resolving after rename")
	}
	if _, err := admin.GetUserByUsername(context.Background(), "new-name"); err != nil {
		t.Fatalf("new username should resolve: %v", err)
	}
}

func TestSaveNotifiesSqliteEventSignalAfterCommit(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")

	notifier := NewSqliteEventNotifier(time.Hour)
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	saver.notifier = notifier
	signal := notifier.Signal("test")
	defer signal.Stop()

	waitCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- signal.Wait(waitCtx)
	}()

	_, _, err := saver.Save(context.Background(), []eventstore.EventWithMapTags{
		mustEvent(t, "Notified", map[string]any{"k": "v"}, map[string]any{}),
	}, "test", nil, nil)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	if err := <-waitCh; err != nil {
		t.Fatalf("expected save notification before polling interval: %v", err)
	}
}

func TestBuildCriteriaSQL(t *testing.T) {
	caseExpr := func(k string) string {
		return `CASE json_type(data, '$."` + k + `"') WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' ELSE CAST(json_extract(data, '$."` + k + `"') AS TEXT) END`
	}
	cases := []struct {
		name  string
		input []map[string]any
		want  string
	}{
		{"empty", nil, "1"},
		{"single criterion", []map[string]any{{"k": "v"}},
			"(" + caseExpr("k") + " = 'v')"},
		{"multi-key AND", []map[string]any{{"a": "1", "b": "2"}},
			"(" + caseExpr("a") + " = '1' AND " + caseExpr("b") + " = '2')"},
		{"multi-criterion OR", []map[string]any{{"a": "1"}, {"b": "2"}},
			"(" + caseExpr("a") + " = '1') OR (" + caseExpr("b") + " = '2')"},
		{"quote escaped", []map[string]any{{"k": "it's"}},
			"(" + caseExpr("k") + " = 'it''s')"},
		{"inf not treated as numeric", []map[string]any{{"k": "inf"}},
			"(" + caseExpr("k") + " = 'inf')"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sql, err := buildCriteriaSQL(c.input)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if sql != c.want {
				t.Errorf("got %q, want %q", sql, c.want)
			}
		})
	}

	t.Run("rejects NUL byte", func(t *testing.T) {
		if _, err := buildCriteriaSQL([]map[string]any{{"k": "a\x00b"}}); err == nil {
			t.Fatal("expected NUL byte rejection")
		}
	})

	t.Run("declared text field keeps direct index expression", func(t *testing.T) {
		registry := newSqliteIndexRegistry()
		registry.replaceBoundaryFields("test", map[string]sqliteFieldInfo{
			"k": {valueType: "text", declaredField: true},
		})
		sql, err := buildCriteriaSQLForBoundary([]map[string]any{{"k": "v"}}, registry, "test")
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		want := `(json_extract(data, '$."k"') = 'v')`
		if sql != want {
			t.Fatalf("got %q, want %q", sql, want)
		}
	})

	t.Run("condition-only key keeps exact CASE shape", func(t *testing.T) {
		// No numeric OR branch: the query term must exactly match the partial-index
		// condition predicate or the implication proof fails and the index goes unused.
		registry := newSqliteIndexRegistry()
		registry.replaceBoundaryFields("test", map[string]sqliteFieldInfo{
			"k": {valueType: "text", declaredField: false},
		})
		sql, err := buildCriteriaSQLForBoundary([]map[string]any{{"k": "1"}}, registry, "test")
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		want := "(" + caseExpr("k") + " = '1')"
		if sql != want {
			t.Fatalf("got %q, want %q", sql, want)
		}
	})
}

func TestJSONPathLiteralEscapes(t *testing.T) {
	cases := map[string]string{
		`simple`:       `'$."simple"'`,
		`with"quote`:   `'$."with""quote"'`,
		`with'apos`:    `'$."with''apos"'`,
		`both"and'mix`: `'$."both""and''mix"'`,
	}
	for in, want := range cases {
		got := jsonPathLiteral(in)
		if got != want {
			t.Errorf("jsonPathLiteral(%q) = %q, want %q", in, got, want)
		}
	}
}
