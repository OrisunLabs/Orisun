package sqlite

import (
	"strings"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestLatestCriteriaPositionMatchesOrderedReads(t *testing.T) {
	saver, pool, cleanup := newGCTestSaver(t)
	defer cleanup()
	events := []orisun.EventWithMapTags{
		mustEvent(t, "Fact", map[string]any{"context": "a", "value": 42, "nested": map[string]any{"a": 1}}, nil),
		mustEvent(t, "Fact", map[string]any{"reference": "b", "value": true, "quoted'key": "it's"}, nil),
		mustEvent(t, "Other", map[string]any{"value": nil}, nil),
	}
	_, _, err := saver.Save(t.Context(), events, gcBoundary, nil)
	require.NoError(t, err)
	criterion := func(key, value string) orisun.ReadCriterion {
		return gcReadCriterion(orisun.ReadTag{Key: key, Value: value})
	}
	cases := [][]orisun.ReadCriterion{
		{criterion("context", "a"), criterion("reference", "b")},
		{criterion("reference", "b"), criterion("context", "a")},
		{criterion("missing", "x"), criterion("context", "a")},
		{criterion("missing", "x"), criterion("value", "null")},
		{criterion("value", "42"), criterion("value", "true")},
		{criterion("nested", `{"a":1}`), criterion("quoted'key", "it's")},
		{criterion("__eventId", events[0].EventId), criterion("__preparePosition", "2")},
		{criterion("__commitPosition", "3"), criterion("__writeId", "3:3")},
		{criterion("__eventType", "Other"), criterion("context", "a")},
		{criterion("context", "a"), criterion("context", "a")},
		{criterion("reference", "b"), gcReadCriterion(orisun.ReadTag{Key: "reference", Value: "b"}, orisun.ReadTag{Key: "unindexed", Value: "missing"})},
		{gcReadCriterion(orisun.ReadTag{Key: "reference", Value: "b"}, orisun.ReadTag{Key: "unindexed", Value: "missing"}), criterion("reference", "b")},
		{gcReadCriterion(orisun.ReadTag{Key: "reference", Value: "other"}, orisun.ReadTag{Key: "value", Value: "true"}), criterion("reference", "b")},
	}
	getter := NewSqliteGetEvents(map[string]*BoundaryPools{gcBoundary: pool}, saver.logger)
	admin := NewSqliteAdminDB(map[string]*BoundaryPools{gcBoundary: pool}, gcBoundary, saver.logger)
	for _, indexed := range []bool{false, true} {
		if indexed {
			for _, key := range []string{"context", "reference"} {
				require.NoError(t, admin.CreateBoundaryIndex(t.Context(), gcBoundary, key, []orisun.BoundaryIndexField{{JsonKey: key, ValueType: "text"}}, []orisun.BoundaryIndexCondition{{Key: "__eventType", Operator: "=", Value: "Fact"}}, "AND"))
			}
		}
		for _, criteria := range cases {
			query := &orisun.Query{}
			for _, c := range criteria {
				cquery := &orisun.Criterion{}
				for _, tag := range c.Tags {
					cquery.Tags = append(cquery.Tags, &orisun.Tag{Key: tag.Key, Value: tag.Value})
				}
				query.Criteria = append(query.Criteria, cquery)
			}
			rows, err := getter.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: gcBoundary, Query: query, Direction: orisun.Direction_DESC, Count: 1})
			require.NoError(t, err)
			expected := orisun.NotExistsPosition()
			if len(rows) > 0 {
				expected = orisun.Position{CommitPosition: rows[0].CommitPosition, PreparePosition: rows[0].PreparePosition}
			}
			conn, err := pool.Write.Take(t.Context())
			require.NoError(t, err)
			tx, gid, err := latestCriteriaPosition(conn, criteria)
			pool.Write.Put(conn)
			require.NoError(t, err)
			require.Equal(t, expected, orisun.Position{CommitPosition: tx, PreparePosition: gid}, "indexed=%v criteria=%v", indexed, criteria)
		}
	}
	// Both OR branches must retain their partial-index access and avoid sorting
	// the event history. Their literals are part of the index predicate.
	criteria := []orisun.ReadCriterion{
		gcReadCriterion(orisun.ReadTag{Key: "__eventType", Value: "Fact"}, orisun.ReadTag{Key: "context", Value: "a"}),
		gcReadCriterion(orisun.ReadTag{Key: "__eventType", Value: "Fact"}, orisun.ReadTag{Key: "reference", Value: "absent"}),
	}
	queries, err := latestCriteriaQueries(criteria)
	require.NoError(t, err)
	conn, err := pool.Write.Take(t.Context())
	require.NoError(t, err)
	defer pool.Write.Put(conn)
	for i, query := range queries {
		var plan []string
		require.NoError(t, sqlitex.ExecuteTransient(conn, "EXPLAIN QUERY PLAN "+query, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { plan = append(plan, stmt.ColumnText(3)); return nil }}))
		require.Contains(t, strings.Join(plan, "\n"), []string{"context", "reference"}[i]+"_idx")
		require.NotContains(t, strings.Join(plan, "\n"), "TEMP B-TREE")
	}
}

func TestRedundantCriteriaDoNotSuppressValidation(t *testing.T) {
	saver, pool, cleanup := newGCTestSaver(t)
	defer cleanup()
	broad := gcReadCriterion(orisun.ReadTag{Key: "tenant", Value: "a"})
	invalid := gcReadCriterion(orisun.ReadTag{Key: "tenant", Value: "a"}, orisun.ReadTag{Key: "bad", Value: "invalid\x00value"})
	for _, criteria := range [][]orisun.ReadCriterion{{broad, invalid}, {invalid, broad}} {
		req := sequenceRequest(t, "Rejected")
		req.consistency = gcConsistency(orisun.NotExistsPosition(), criteria...)
		_, _, err := saver.SavePrepared(t.Context(), req.inserts, gcBoundary, req.consistency)
		require.Equal(t, statuscode.InvalidArgument, statuscode.CodeOf(err))
	}
	require.EqualValues(t, 1, readSeqNextID(t, pool))
}

func TestPreparedConsistencyChecksPreserveOrderAndSnapshot(t *testing.T) {
	saver, pool, cleanup := newGCTestSaver(t)
	defer cleanup()
	seed := sequenceRequest(t, "Seed")
	saver.runTestFlush(gcBoundary, pool, []*sqliteTestRequest{seed})
	require.NoError(t, (<-seed.result).err)

	conflict := gcConsistency(orisun.NotExistsPosition(), gcReadCriterion(orisun.ReadTag{Key: "__eventType", Value: "Seed"}))[0]
	invalid := orisun.ConsistencyCheck{Position: orisun.NotExistsPosition(), Criteria: []orisun.ReadCriterion{gcReadCriterion(orisun.ReadTag{Key: "bad", Value: "invalid\x00value"})}}
	for _, tc := range []struct {
		checks []orisun.ConsistencyCheck
		code   statuscode.Code
	}{
		{[]orisun.ConsistencyCheck{conflict, invalid}, statuscode.AlreadyExists},
		{[]orisun.ConsistencyCheck{invalid, conflict}, statuscode.InvalidArgument},
	} {
		req := sequenceRequest(t, "Rejected")
		req.preparedChecks = prepareConsistencyChecks(tc.checks)
		saver.runTestFlush(gcBoundary, pool, []*sqliteTestRequest{req})
		require.Equal(t, tc.code, statuscode.CodeOf((<-req.result).err))
	}
	require.EqualValues(t, 2, readSeqNextID(t, pool))

	checks := gcConsistency(orisun.NotExistsPosition(), gcReadCriterion(orisun.ReadTag{Key: "tenant", Value: "original"}))
	prepared := prepareConsistencyChecks(checks)
	before := append([]string(nil), prepared[0].queries...)
	checks[0].Criteria[0].Tags[0].Value = "changed"
	checks[0].Position = orisun.Position{CommitPosition: 99, PreparePosition: 99}
	require.Equal(t, before, prepared[0].queries)
	require.Equal(t, orisun.NotExistsPosition(), prepared[0].position)
}

// Envelope fields moved to columns, but criteria keep JSON-field semantics:
// positions compare as text for equality and as exact numbers for ranges.
func TestEnvelopeCriteriaKeepFieldSemantics(t *testing.T) {
	saver, getter, _, cleanup := setupBenchmarkPools(t)
	defer cleanup()
	ctx := t.Context()
	for i, event := range []orisun.EventWithMapTags{
		{EventId: "a", EventType: "Alpha", Data: `{"k":"v"}`, Metadata: `{"tier":"gold"}`},
		{EventId: "b", EventType: "Beta", Data: `{"k":"v"}`, Metadata: `"plain"`},
		{EventId: "c", EventType: "Gamma", Data: `{"k":"v"}`, Metadata: `7`},
	} {
		_, gid, err := saver.Save(ctx, []orisun.EventWithMapTags{event}, benchBoundary, nil)
		require.NoError(t, err)
		require.EqualValues(t, i+1, gid)
	}
	cases := []struct {
		key, op, value string
		want           []string
	}{
		{"__eventId", "", "b", []string{"b"}},
		{"__eventId", "ne", "b", []string{"a", "c"}},
		{"__eventId", "gt", "a", []string{"b", "c"}},
		{"__eventType", "", "Gamma", []string{"c"}},
		{"__eventType", "lte", "Beta", []string{"a", "b"}},
		{"__preparePosition", "", "2", []string{"b"}},
		{"__preparePosition", "", "2.0", nil},
		{"__preparePosition", "gt", "1", []string{"b", "c"}},
		{"__preparePosition", "lt", "2.5", []string{"a", "b"}},
		{"__preparePosition", "gt", "not-a-number", nil},
		{"__commitPosition", "gte", "3", []string{"c"}},
		{"__writeId", "", "2:2", []string{"b"}},
		{"__writeId", "gt", "2:2", []string{"c"}},
		{"__metadata", "", `{"tier":"gold"}`, []string{"a"}},
		{"__metadata", "", "plain", []string{"b"}},
		{"__metadata", "", "7", []string{"c"}},
		{"__metadata", "gte", "7", []string{"b", "c"}},
		{"__metadata", "gt", "a", []string{"b"}},
		{"__unknown", "", "x", nil},
		{"__eventId", "", "x' OR '1'='1", nil},
	}
	for _, tc := range cases {
		t.Run(tc.key+" "+tc.op+" "+tc.value, func(t *testing.T) {
			batch, err := getter.GetBatch(ctx, &orisun.GetEventsRequest{Boundary: benchBoundary, Direction: orisun.Direction_ASC, Count: 10, Query: &orisun.Query{Criteria: []*orisun.Criterion{{Tags: []*orisun.Tag{{Key: tc.key, Value: tc.value, Operator: tc.op}}}}}})
			require.NoError(t, err)
			var got []string
			for _, event := range batch {
				got = append(got, event.EventId)
			}
			require.Equal(t, tc.want, got)
		})
	}
	first, err := getter.GetBatch(ctx, &orisun.GetEventsRequest{Boundary: benchBoundary, Direction: orisun.Direction_ASC, Count: 1})
	require.NoError(t, err)
	require.Equal(t, `{"k":"v"}`, first[0].Data)
	require.Equal(t, `{"tier":"gold"}`, first[0].Metadata)
	require.Equal(t, "1:1", first[0].WriteId)
}

// Position reads rely on the rowid trailing the index in both directions.
func TestPositionOrderReadsUseIndex(t *testing.T) {
	_, pool, cleanup := newGCTestSaver(t)
	defer cleanup()
	conn, err := pool.Read.Take(t.Context())
	require.NoError(t, err)
	defer pool.Read.Put(conn)
	for _, query := range []string{
		"SELECT global_id FROM orisun_es_event ORDER BY transaction_id ASC, global_id ASC LIMIT 1",
		"SELECT global_id FROM orisun_es_event ORDER BY transaction_id DESC, global_id DESC LIMIT 1",
		"SELECT global_id FROM orisun_es_event WHERE (transaction_id, global_id) >= (5, 2) ORDER BY transaction_id ASC, global_id ASC LIMIT 1",
		"SELECT global_id FROM orisun_es_event WHERE (transaction_id, global_id) <= (5, 2) ORDER BY transaction_id DESC, global_id DESC LIMIT 1",
	} {
		var plan strings.Builder
		require.NoError(t, sqlitex.ExecuteTransient(conn, "EXPLAIN QUERY PLAN "+query, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
			plan.WriteString(stmt.ColumnText(3))
			return nil
		}}))
		require.Contains(t, plan.String(), "idx_event_position", query)
		require.NotContains(t, plan.String(), "TEMP B-TREE", query)
	}
}
