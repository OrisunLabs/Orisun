package postgres

import (
	"context"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// The flush plan renders criteria in Go; reads still render them in SQL.
// Both must produce the same predicate for every operator and literal.
func TestCriterionSQLMatchesDatabaseRenderer(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.container.Terminate(context.Background()) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	criteria := [][]eventstore.ReadTag{
		{{Key: "customerId", Value: "cust-1"}},
		{{Key: "b", Value: "x"}, {Key: "a", Value: "y"}},
		{{Key: "amount", Value: "10", Operator: "gte"}, {Key: "amount", Value: "20", Operator: "lt"}, {Key: "scope", Value: "s"}},
		{{Key: "n", Value: "1", Operator: "ne"}},
		{{Key: "q", Value: "it's"}, {Key: "w", Value: `back\slash`, Operator: "gt"}},
		{{Key: "quote'key", Value: `both ' and \`}},
		{{Key: "__eventType", Value: "Created"}, {Key: "z", Value: "9", Operator: "eq"}},
		{{Key: "t", Value: "a", Operator: "eq"}, {Key: "t", Value: "b", Operator: "lte"}},
	}
	for _, tags := range criteria {
		fields := eventstore.EncodeCriterion(tags)
		encoded, err := json.Marshal(fields)
		require.NoError(t, err)
		for _, expression := range []string{"stored.data", "candidate.document"} {
			var expected string
			require.NoError(t, db.QueryRow("SELECT public.orisun_criterion_sql($1::jsonb, $2)", encoded, expression).Scan(&expected))
			actual, err := postgresCriterionSQL(fields, expression)
			require.NoError(t, err)
			require.Equal(t, expected, actual, "criterion %s", encoded)
		}
	}
}

func TestFlushPlanDeduplicatesCriteriaAcrossRequests(t *testing.T) {
	first := []eventstore.ConsistencyCheck{{
		Criteria: []eventstore.ReadCriterion{{Tags: []eventstore.ReadTag{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}}},
		Position: eventstore.Position{CommitPosition: 5, PreparePosition: 4},
	}}
	// Same criterion with tags in another order, plus an operator criterion.
	second := []eventstore.ConsistencyCheck{
		{Criteria: []eventstore.ReadCriterion{
			{Tags: []eventstore.ReadTag{{Key: "b", Value: "2"}, {Key: "a", Value: "1"}}},
			{Tags: []eventstore.ReadTag{{Key: "n", Value: "3", Operator: "gt"}}},
		}, Position: eventstore.NotExistsPosition()},
		{Criteria: []eventstore.ReadCriterion{{Tags: []eventstore.ReadTag{{Key: "c", Value: "x"}}}}, Position: eventstore.Position{CommitPosition: 7, PreparePosition: 6}},
	}
	firstChecks, err := compileConsistencyChecks(first)
	require.NoError(t, err)
	secondChecks, err := compileConsistencyChecks(second)
	require.NoError(t, err)

	plan := planPostgresFlush(`"public"."b_orisun_es_event"`, [][]compiledCheck{firstChecks, nil, secondChecks})
	require.Equal(t, 3, plan.criterionCount)
	require.Equal(t, []int32{1, 2, 2, 4}, plan.requestCheckOffsets)
	require.Equal(t, []int32{1, 2, 4, 5}, plan.checkCriterionOffsets)
	require.Equal(t, []int32{1, 1, 2, 3}, plan.checkCriterionIDs)
	require.Equal(t, []int64{5, -1, 7}, plan.expectedTransactions)
	require.Equal(t, []int64{4, -1, 6}, plan.expectedGlobals)
	require.JSONEq(t, `[[1,{"a":"1","b":"2"}],[3,{"c":"x"}]]`, string(plan.equalityCriteria))
	require.JSONEq(t, `[["a","b"],["c"]]`, string(plan.criterionShapes))
	require.NotNil(t, plan.latestQuery)
	require.NotNil(t, plan.predicateQuery)
	require.Contains(t, *plan.predicateQuery, "THEN 2 END")
}

func TestFlushPlanWithoutCriteria(t *testing.T) {
	plan := planPostgresFlush(`"public"."b_orisun_es_event"`, [][]compiledCheck{nil, nil})
	require.Zero(t, plan.criterionCount)
	require.Equal(t, []int32{1, 1, 1}, plan.requestCheckOffsets)
	require.Equal(t, []int32{1}, plan.checkCriterionOffsets)
	require.Nil(t, plan.latestQuery)
	require.Nil(t, plan.predicateQuery)
	require.Equal(t, "[]", string(plan.equalityCriteria))
}
