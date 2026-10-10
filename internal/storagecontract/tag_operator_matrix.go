package storagecontract

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TagOperatorMatrix runs the same combinations against each durable backend.
// Expected matches use a test-only rational-number oracle, never the production
// predicate renderer or numeric comparator.
func TagOperatorMatrix(t *testing.T, saver orisun.EventsSaver, reader orisun.EventsRetriever, contexts orisun.WriteContextRetriever, boundary string) {
	t.Helper()
	operators := []string{"", "eq", "ne", "gt", "gte", "lt", "lte"}
	values := []any{20, -2, "10", 2, false, "z", 10, "2", true, nil, "", "é", "Z", "🙂", "' OR TRUE --"}
	scope := uuid.NewString()
	fixtures := make([]map[string]any, len(values)+1)
	events := make([]orisun.EventWithMapTags, len(fixtures))
	for i := range fixtures {
		fixtures[i] = map[string]any{"operator_scope": scope}
		if i < len(values) {
			fixtures[i]["x"] = values[i]
			fixtures[i]["y"] = values[len(values)-1-i]
		}
		events[i] = orisun.EventWithMapTags{EventId: uuid.NewString(), EventType: "OperatorMatrix", Data: fixtures[i]}
	}
	prepared, err := orisun.PrepareEventsForSave(events)
	require.NoError(t, err)
	_, _, err = saver.SavePrepared(t.Context(), prepared, boundary, nil)
	require.NoError(t, err)
	decision, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{EventId: uuid.NewString(), EventType: "MatrixDecision", Data: map[string]any{}}})
	require.NoError(t, err)

	run := func(t *testing.T, alternatives [][]orisun.ReadTag) {
		t.Helper()
		criteria := make([]orisun.ReadCriterion, len(alternatives))
		query := &orisun.Query{Criteria: make([]*orisun.Criterion, len(alternatives))}
		for i, tags := range alternatives {
			criteria[i].Tags = append([]orisun.ReadTag{{Key: "operator_scope", Value: scope}}, tags...)
			query.Criteria[i] = &orisun.Criterion{}
			for _, tag := range criteria[i].Tags {
				query.Criteria[i].Tags = append(query.Criteria[i].Tags, &orisun.Tag{Key: tag.Key, Value: tag.Value, Operator: tag.Operator})
			}
		}
		want := make([]string, 0)
		lastByCriterion := make([]string, len(criteria))
		for i, fixture := range fixtures {
			matches := false
			for j, criterion := range criteria {
				if matrixCriterionMatches(fixture, criterion.Tags) {
					matches = true
					lastByCriterion[j] = events[i].EventId
				}
			}
			if matches {
				want = append(want, events[i].EventId)
			}
		}
		rows, err := reader.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Query: query, Count: 100, Direction: orisun.Direction_ASC})
		require.NoError(t, err)
		ids := make([]string, len(rows))
		for i, row := range rows {
			ids[i] = row.EventId
		}
		require.Equal(t, want, ids)
		for _, direction := range []orisun.Direction{orisun.Direction_ASC, orisun.Direction_DESC} {
			limited, err := reader.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Query: query, Count: 2, Direction: direction})
			require.NoError(t, err)
			require.Len(t, limited, min(2, len(want)))
			for i, row := range limited {
				index := i
				if direction == orisun.Direction_DESC {
					index = len(want) - 1 - i
				}
				require.Equal(t, want[index], row.EventId)
			}
		}
		if len(rows) > 0 {
			middle := len(rows) / 2
			cursor := &orisun.Position{CommitPosition: rows[middle].CommitPosition, PreparePosition: rows[middle].PreparePosition}
			for _, direction := range []orisun.Direction{orisun.Direction_ASC, orisun.Direction_DESC} {
				page, err := reader.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Query: query, Count: 2, Direction: direction, FromPosition: cursor})
				require.NoError(t, err)
				available := len(rows) - middle
				if direction == orisun.Direction_DESC {
					available = middle + 1
				}
				require.Len(t, page, min(2, available))
				for i, row := range page {
					index := middle + i
					if direction == orisun.Direction_DESC {
						index = middle - i
					}
					require.Equal(t, want[index], row.EventId, "cursor reads are inclusive in both directions")
				}
			}
		}
		latest, err := reader.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: boundary, Criteria: criteria})
		require.NoError(t, err)
		require.Len(t, latest.Matches, len(criteria))
		for i, id := range lastByCriterion {
			require.Equal(t, id != "", latest.Matches[i].Found)
			if id != "" {
				require.Equal(t, id, latest.Matches[i].Event.EventId)
			}
		}
		checks := []orisun.ConsistencyCheck{{Criteria: criteria, Position: orisun.NotExistsPosition()}}
		if len(rows) > 0 {
			_, _, err = saver.SavePrepared(t.Context(), decision, boundary, checks)
			require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
			last := rows[len(rows)-1]
			checks[0].Position = orisun.Position{CommitPosition: last.CommitPosition, PreparePosition: last.PreparePosition}
		}
		tx, gid, err := saver.SavePrepared(t.Context(), decision, boundary, checks)
		require.NoError(t, err)
		commit, err := strconv.ParseInt(tx, 10, 64)
		require.NoError(t, err)
		recorded, err := contexts.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: boundary, WriteId: orisun.WriteID(commit, gid)})
		require.NoError(t, err)
		require.Equal(t, orisun.WriteID(commit, gid), recorded.WriteId)
		require.Len(t, recorded.Consistency, 1)
		require.Equal(t, checks[0].Position, *recorded.Consistency[0].Position)
		require.Len(t, recorded.Consistency[0].Query.Criteria, len(criteria))
		for i, criterion := range criteria {
			expectedTags := make([]*orisun.Tag, len(criterion.Tags))
			for j, tag := range criterion.Tags {
				op := tag.Operator
				if op == "eq" {
					op = ""
				}
				expectedTags[j] = &orisun.Tag{Key: tag.Key, Value: tag.Value, Operator: op}
			}
			require.ElementsMatch(t, expectedTags, recorded.Consistency[0].Query.Criteria[i].Tags)
		}
	}
	for _, op := range operators {
		for i, target := range []string{"2", "10", "20", "-2", "true", "false", "", "é", "Z", "🙂", "' OR TRUE --", "not-a-number"} {
			t.Run(fmt.Sprintf("single/%s/target_%02d", matrixOperatorName(op), i), func(t *testing.T) {
				run(t, [][]orisun.ReadTag{{{Key: "x", Value: target, Operator: op}}})
			})
		}
		for _, other := range operators {
			name := matrixOperatorName(op) + "_" + matrixOperatorName(other)
			a := orisun.ReadTag{Key: "x", Value: "2", Operator: op}
			b := orisun.ReadTag{Key: "y", Value: "10", Operator: other}
			t.Run("and/"+name, func(t *testing.T) { run(t, [][]orisun.ReadTag{{a, b}}) })
			t.Run("or/"+name, func(t *testing.T) { run(t, [][]orisun.ReadTag{{a}, {b}}) })
			b.Key, b.Value = "x", "20"
			t.Run("same_field/"+name, func(t *testing.T) { run(t, [][]orisun.ReadTag{{a, b}}) })
		}
	}
	// Ordered comparisons reject container values and compare decimals exactly.
	// Equality retains each backend's existing scalar text representation.
	edgeValues := []any{json.Number("9007199254740993"), json.Number("0.10000000000000000001"), json.Number("-0.10000000000000000001"), json.Number("1e30"), 0, []any{2}, map[string]any{"n": 2}}
	for _, value := range edgeValues {
		fixture := map[string]any{"operator_scope": scope, "x": value}
		event := orisun.EventWithMapTags{EventId: uuid.NewString(), EventType: "OperatorMatrixEdge", Data: fixture}
		batch, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{event})
		require.NoError(t, err)
		_, _, err = saver.SavePrepared(t.Context(), batch, boundary, nil)
		require.NoError(t, err)
		fixtures = append(fixtures, fixture)
		events = append(events, event)
	}
	for _, op := range []string{"gt", "gte", "lt", "lte"} {
		for i, target := range []string{"9007199254740992", "9007199254740993", "0.1", "-0.1", "1e30", "-0", "1e-30", "01", "+1", "NaN"} {
			t.Run(fmt.Sprintf("ordered_edges/%s/target_%02d", op, i), func(t *testing.T) {
				run(t, [][]orisun.ReadTag{{{Key: "x", Value: target, Operator: op}}})
			})
		}
	}
	for _, op := range operators {
		t.Run("stale_observation/"+matrixOperatorName(op), func(t *testing.T) {
			matching, nonmatching := 10, 20
			switch op {
			case "ne", "gt":
				matching, nonmatching = 20, 10
			case "gte":
				nonmatching = 5
			case "lt":
				matching = 5
			}
			localScope := uuid.NewString()
			criteria := []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "operator_scope", Value: localScope}, {Key: "x", Value: "10", Operator: op}}}}
			appendValue := func(value int) {
				batch, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{EventId: uuid.NewString(), EventType: "StaleMatrix", Data: map[string]any{"operator_scope": localScope, "x": value}}})
				require.NoError(t, err)
				_, _, err = saver.SavePrepared(t.Context(), batch, boundary, nil)
				require.NoError(t, err)
			}
			checks := []orisun.ConsistencyCheck{{Criteria: criteria, Position: orisun.NotExistsPosition()}}
			appendValue(nonmatching)
			_, _, err := saver.SavePrepared(t.Context(), decision, boundary, checks)
			require.NoError(t, err, "nonmatching writes must not invalidate the observation")
			appendValue(matching)
			_, _, err = saver.SavePrepared(t.Context(), decision, boundary, checks)
			require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err), "first matching write must invalidate NotExists")
			latest, err := reader.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: boundary, Criteria: criteria})
			require.NoError(t, err)
			require.True(t, latest.Matches[0].Found)
			last := latest.Matches[0].Event
			checks[0].Position = orisun.Position{CommitPosition: last.CommitPosition, PreparePosition: last.PreparePosition}
			_, _, err = saver.SavePrepared(t.Context(), decision, boundary, checks)
			require.NoError(t, err)
			appendValue(matching)
			_, _, err = saver.SavePrepared(t.Context(), decision, boundary, checks)
			require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err), "later matching write must invalidate an observed position")
		})
	}
}

func matrixOperatorName(op string) string {
	if op == "" {
		return "default_eq"
	}
	return op
}

func matrixCriterionMatches(data map[string]any, tags []orisun.ReadTag) bool {
	for _, tag := range tags {
		value, present := data[tag.Key]
		if !present || value == nil {
			return false
		}
		if tag.Operator == "" || tag.Operator == "eq" || tag.Operator == "ne" {
			equal := fmt.Sprint(value) == tag.Value
			if (tag.Operator == "ne" && equal) || (tag.Operator != "ne" && !equal) {
				return false
			}
			continue
		}
		var comparison int
		switch value := value.(type) {
		case string:
			comparison = strings.Compare(value, tag.Value)
		case int, json.Number:
			// json.Valid rejects forms accepted by big.Rat but excluded by JSON,
			// such as +1 and 01. big.Rat compares the remaining values exactly.
			if !json.Valid([]byte(tag.Value)) {
				return false
			}
			left, ok := new(big.Rat).SetString(fmt.Sprint(value))
			if !ok {
				panic("invalid numeric matrix fixture")
			}
			right, ok := new(big.Rat).SetString(tag.Value)
			if !ok {
				return false
			}
			comparison = left.Cmp(right)
		default:
			return false
		}
		matches := map[string]bool{"gt": comparison > 0, "gte": comparison >= 0, "lt": comparison < 0, "lte": comparison <= 0}
		if !matches[tag.Operator] {
			return false
		}
	}
	return true
}
