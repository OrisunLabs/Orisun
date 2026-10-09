package sqlite

import (
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
)

func TestIndexesPreserveCCCEqualityAcrossLifecycle(t *testing.T) {
	for _, valueType := range []string{"text", "numeric", "boolean", "timestamptz"} {
		t.Run(valueType, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			logger, err := logging.ZapLogger("error")
			require.NoError(t, err)
			var pool *BoundaryPools
			var saver *SqliteSaveEvents
			var getter *SqliteGetEvents
			var admin *SqliteAdminDB
			open := func() {
				var err error
				pool, err = OpenBoundaryPools(ctx, dir, "test")
				require.NoError(t, err)
				pools := map[string]*BoundaryPools{"test": pool}
				saver = NewSqliteSaveEvents(pools, logger)
				getter = NewSqliteGetEvents(pools, logger)
				admin = NewSqliteAdminDB(pools, "test", logger)
			}
			close := func() { saver.close(); require.NoError(t, pool.Close()) }
			open()
			defer func() { close() }()
			values := []any{42, "42", "042", true, "true", false, "false", 0, "oops", nil, "2026-09-25T00:00:00Z"}
			events := make([]eventstore.EventWithMapTags, 0, len(values)+1)
			for _, value := range values {
				events = append(events, mustEvent(t, "Observed", map[string]any{"value": value}, nil))
			}
			events = append(events, mustEvent(t, "Missing", map[string]any{}, nil))
			_, _, err = saver.Save(ctx, events, "test", nil)
			require.NoError(t, err)
			cases := []struct {
				value   string
				indexes []int
			}{
				{"42", []int{0, 1}}, {"042", []int{2}}, {"true", []int{3, 4}},
				{"false", []int{5, 6}}, {"0", []int{7}}, {"oops", []int{8}},
				{"null", nil}, {"2026-09-25T00:00:00Z", []int{10}},
			}
			verify := func(stage string) {
				t.Helper()
				for _, tc := range cases {
					t.Run(stage+"/"+tc.value, func(t *testing.T) {
						query := &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{{Key: "value", Value: tc.value}}}}}
						criteria := []eventstore.ReadCriterion{{Tags: []eventstore.ReadTag{{Key: "value", Value: tc.value}}}}
						rows, err := getter.GetBatch(ctx, &eventstore.GetEventsRequest{
							Boundary: "test", Direction: eventstore.Direction_ASC, Count: 100,
							Query: &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{{Key: "value", Value: tc.value}}}}},
						})
						require.NoError(t, err)
						require.Len(t, rows, len(tc.indexes))
						for i, index := range tc.indexes {
							require.Equal(t, events[index].EventId, rows[i].EventId)
						}
						latest, err := getter.GetLatestByCriteria(ctx, eventstore.LatestByCriteriaQuery{Boundary: "test", Criteria: criteria})
						require.NoError(t, err)
						require.Len(t, latest.Matches, 1)
						position := eventstore.NotExistsPosition()
						if len(rows) > 0 {
							last := rows[len(rows)-1]
							require.True(t, latest.Matches[0].Found)
							require.Equal(t, last.EventId, latest.Matches[0].Event.EventId)
							position = eventstore.Position{CommitPosition: last.CommitPosition, PreparePosition: last.PreparePosition}
							// A stale observation must not become acceptable when an index hides a match.
							_, _, err = saver.Save(ctx, []eventstore.EventWithMapTags{mustEvent(t, "Rejected", map[string]any{}, nil)}, "test", []*eventstore.ConsistencyObservation{{Query: query, Position: &eventstore.Position{CommitPosition: -1, PreparePosition: -1}}})
							require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
						} else {
							require.False(t, latest.Matches[0].Found)
						}
						require.Equal(t, position.CommitPosition, latest.ContextCommitPosition)
						require.Equal(t, position.PreparePosition, latest.ContextPreparePosition)
						// The correct observation must not be rejected by numeric/boolean aliasing.
						_, _, err = saver.Save(ctx, []eventstore.EventWithMapTags{mustEvent(t, "Accepted", map[string]any{}, nil)}, "test", []*eventstore.ConsistencyObservation{{Query: query, Position: &position}})
						require.NoError(t, err)
					})
				}
			}
			verify("without_index")
			require.NoError(t, admin.CreateBoundaryIndex(ctx, "test", "a_value", []eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: valueType}}, nil, ""))
			verify("created")
			require.NoError(t, admin.CreateBoundaryIndex(ctx, "test", "z_value", []eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: "numeric"}}, nil, ""))
			verify("overlapping_types")
			close()
			open()
			verify("reopened")
			require.NoError(t, admin.DropBoundaryIndex(ctx, "test", "a_value"))
			require.NoError(t, admin.DropBoundaryIndex(ctx, "test", "z_value"))
			verify("dropped")
		})
	}
}

func TestTypedIndexPreservesDistinctContextsInGroupCommit(t *testing.T) {
	saver, pool, cleanup := newGCTestSaver(t)
	defer cleanup()
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	admin := NewSqliteAdminDB(map[string]*BoundaryPools{"test": pool}, "test", logger)
	require.NoError(t, admin.CreateBoundaryIndex(t.Context(), "test", "value", []eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: "numeric"}}, nil, ""))
	var requests []*sqliteSaveRequest
	for _, value := range []string{"42", "042"} {
		requests = append(requests, &sqliteSaveRequest{
			ctx:         t.Context(),
			inserts:     preparedGCEvents(t, mustEvent(t, "Created", map[string]any{"value": value}, nil)),
			consistency: gcConsistency(eventstore.NotExistsPosition(), gcReadCriterion(eventstore.ReadTag{Key: "value", Value: value})),
			result:      make(chan sqliteSaveResult, 1),
		})
	}
	prepareGCWriteContexts(t, requests)
	saver.runFlush("test", pool, requests)
	for i, req := range requests {
		result := <-req.result
		require.NoError(t, result.err)
		require.EqualValues(t, i+1, result.globalID)
	}
	require.Equal(t, 1, countEventsMatching(t, pool, map[string]any{"value": "42"}))
	require.Equal(t, 1, countEventsMatching(t, pool, map[string]any{"value": "042"}))
}
