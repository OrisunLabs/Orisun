package storagecontract

import (
	"strconv"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TagOperators exercises predicates through reads, latest observations,
// optimistic writes and durable write contexts. Fixture order intentionally
// differs from value order, catching index scans that apply LIMIT too soon.
func TagOperators(t *testing.T, saver orisun.EventsSaver, reader orisun.EventsRetriever, contexts orisun.WriteContextRetriever, boundary string) {
	t.Helper()
	scope := uuid.NewString()
	values := []any{10, 2, 20, "10", "2", "z", true, nil, json.Number("9007199254740993"), json.Number("0.10000000000000000001"), -2}
	events := make([]orisun.EventWithMapTags, 0, len(values)+1)
	for _, value := range values {
		events = append(events, orisun.EventWithMapTags{EventId: uuid.NewString(), EventType: "OperatorFixture", Data: map[string]any{"operator_scope": scope, "value": value}})
	}
	events = append(events, orisun.EventWithMapTags{EventId: uuid.NewString(), EventType: "OperatorFixture", Data: map[string]any{"operator_scope": scope}})
	prepared, err := orisun.PrepareEventsForSave(events)
	require.NoError(t, err)
	_, _, err = saver.SavePrepared(t.Context(), prepared, boundary, nil)
	require.NoError(t, err)
	cases := []struct {
		name string
		tags []orisun.ReadTag
		want []int
	}{
		{"default", []orisun.ReadTag{{Key: "value", Value: "10"}}, []int{0, 3}},
		{"eq", []orisun.ReadTag{{Key: "value", Value: "10", Operator: "eq"}}, []int{0, 3}},
		{"ne", []orisun.ReadTag{{Key: "value", Value: "10", Operator: "ne"}}, []int{1, 2, 4, 5, 6, 8, 9, 10}},
		{"gt", []orisun.ReadTag{{Key: "value", Value: "2", Operator: "gt"}}, []int{0, 2, 5, 8}},
		{"gte", []orisun.ReadTag{{Key: "value", Value: "2", Operator: "gte"}}, []int{0, 1, 2, 4, 5, 8}},
		{"lt", []orisun.ReadTag{{Key: "value", Value: "2", Operator: "lt"}}, []int{3, 9, 10}},
		{"lte", []orisun.ReadTag{{Key: "value", Value: "2", Operator: "lte"}}, []int{1, 3, 4, 9, 10}},
		{"range", []orisun.ReadTag{{Key: "value", Value: "2", Operator: "gte"}, {Key: "value", Value: "20", Operator: "lt"}}, []int{0, 1, 4}},
		{"large_integer", []orisun.ReadTag{{Key: "value", Value: "9007199254740992", Operator: "gt"}}, []int{5, 8}},
		{"exact_decimal", []orisun.ReadTag{{Key: "value", Value: "0.1", Operator: "gt"}, {Key: "value", Value: "0.10000000000000000002", Operator: "lt"}}, []int{9}},
		{"non_numeric_target", []orisun.ReadTag{{Key: "value", Value: "a", Operator: "gt"}}, []int{5}},
		{"quoted_target", []orisun.ReadTag{{Key: "value", Value: "' OR TRUE --", Operator: "gt"}}, []int{3, 4, 5}},
		{"empty_range", []orisun.ReadTag{{Key: "value", Value: "20", Operator: "gt"}, {Key: "value", Value: "2", Operator: "lt"}}, nil},
		{"no_matches", []orisun.ReadTag{{Key: "value", Value: "z", Operator: "gt"}}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tags := append([]orisun.ReadTag{{Key: "operator_scope", Value: scope}}, tc.tags...)
			criterion := &orisun.Criterion{}
			for _, tag := range tags {
				criterion.Tags = append(criterion.Tags, &orisun.Tag{Key: tag.Key, Value: tag.Value, Operator: tag.Operator})
			}
			query := &orisun.Query{Criteria: []*orisun.Criterion{criterion}}
			rows, err := reader.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Query: query, Count: 100, Direction: orisun.Direction_ASC})
			require.NoError(t, err)
			require.Len(t, rows, len(tc.want))
			for i, index := range tc.want {
				require.Equal(t, events[index].EventId, rows[i].EventId)
			}
			latest, err := reader.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: boundary, Criteria: []orisun.ReadCriterion{{Tags: tags}}})
			require.NoError(t, err)
			if len(tc.want) == 0 {
				require.False(t, latest.Matches[0].Found)
				checks := []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: tags}}, Position: orisun.NotExistsPosition()}}
				unrelated, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{EventId: uuid.NewString(), EventType: "EmptyRangeDecision", Data: map[string]any{}}})
				require.NoError(t, err)
				_, _, err = saver.SavePrepared(t.Context(), unrelated, boundary, checks)
				require.NoError(t, err)
				return
			}
			require.True(t, latest.Matches[0].Found)
			last := rows[len(rows)-1]
			require.Equal(t, last.EventId, latest.Matches[0].Event.EventId)
			// DESC + LIMIT must choose by position after filtering, not field value.
			descending, err := reader.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Query: query, Count: 1, Direction: orisun.Direction_DESC})
			require.NoError(t, err)
			require.Len(t, descending, 1)
			require.Equal(t, last.EventId, descending[0].EventId)
			checks := []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: tags}}, Position: orisun.NotExistsPosition()}}
			unrelated, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{EventId: uuid.NewString(), EventType: "Decision", Data: map[string]any{}}})
			require.NoError(t, err)
			_, _, err = saver.SavePrepared(t.Context(), unrelated, boundary, checks)
			require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
			checks[0].Position = orisun.Position{CommitPosition: last.CommitPosition, PreparePosition: last.PreparePosition}
			tx, gid, err := saver.SavePrepared(t.Context(), unrelated, boundary, checks)
			require.NoError(t, err)
			commit, err := strconv.ParseInt(tx, 10, 64)
			require.NoError(t, err)
			recorded, err := contexts.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: boundary, WriteId: orisun.WriteID(commit, gid)})
			require.NoError(t, err)
			encoded, err := orisun.MarshalConsistency(checks)
			require.NoError(t, err)
			expected, err := orisun.DecodeWriteContext(recorded.WriteId, encoded)
			require.NoError(t, err)
			require.Equal(t, expected, recorded)
		})
	}
	t.Run("overlapping_or_criteria", func(t *testing.T) {
		query := &orisun.Query{Criteria: []*orisun.Criterion{
			{Tags: []*orisun.Tag{{Key: "operator_scope", Value: scope}, {Key: "value", Value: "2", Operator: "gt"}}},
			{Tags: []*orisun.Tag{{Key: "operator_scope", Value: scope}, {Key: "value", Value: "10", Operator: "eq"}}},
		}}
		rows, err := reader.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Query: query, Count: 100, Direction: orisun.Direction_ASC})
		require.NoError(t, err)
		want := []int{0, 2, 3, 5, 8}
		require.Len(t, rows, len(want))
		for i, index := range want {
			require.Equal(t, events[index].EventId, rows[i].EventId)
		}
	})
	// Invalid operators must fail before a read or consistency write can ignore them.
	for i, operator := range []string{"LIKE", "EQ", "GT", "=", ">=", " eq", "eq ", "gt; SELECT 1", "\x00"} {
		t.Run("invalid_operator_"+strconv.Itoa(i), func(t *testing.T) {
			bad := orisun.ReadTag{Key: "value", Value: "10", Operator: operator}
			_, err := reader.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Count: 1, Query: &orisun.Query{Criteria: []*orisun.Criterion{{Tags: []*orisun.Tag{{Key: bad.Key, Value: bad.Value, Operator: bad.Operator}}}}}})
			require.Equal(t, statuscode.InvalidArgument, statuscode.CodeOf(err))
			_, err = reader.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: boundary, Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{bad}}}})
			require.Equal(t, statuscode.InvalidArgument, statuscode.CodeOf(err))
			_, _, err = saver.SavePrepared(t.Context(), prepared, boundary, []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{bad}}}, Position: orisun.NotExistsPosition()}})
			require.Equal(t, statuscode.InvalidArgument, statuscode.CodeOf(err))
		})
	}
}
