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
