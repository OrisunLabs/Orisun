package postgres

import (
	"context"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// The flush plan renders stored-row criteria in Go; reads render them in SQL.
// Both must produce the same predicate for every key, operator, and literal.
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
	for _, key := range envelopeTestKeys {
		for _, operator := range []string{"eq", "ne", "gt", "lte"} {
			criteria = append(criteria, []eventstore.ReadTag{{Key: key, Value: "v'1", Operator: operator}})
		}
	}
	for _, tags := range criteria {
		fields := eventstore.EncodeCriterion(tags)
		encoded, err := json.Marshal(fields)
		require.NoError(t, err)
		for _, prefix := range []string{"", "e.", "stored."} {
			var expected string
			require.NoError(t, db.QueryRow("SELECT public.orisun_stored_criterion_sql($1::jsonb, $2)", encoded, prefix).Scan(&expected))
			actual, err := storedRow(prefix).criterionSQL(fields)
			require.NoError(t, err)
			require.Equal(t, expected, actual, "criterion %s", encoded)
		}
	}
}

var envelopeTestKeys = []string{"__eventId", "__eventType", "__dateCreated", "__writeId", "__commitPosition", "__preparePosition", "__metadata"}

// Stored rows resolve envelope keys to typed columns; in-batch candidates and
// earlier releases evaluate the same keys inside one JSON document. Every
// key, operator, and value must select the same events either way.
func TestEnvelopeColumnsMatchDocumentSemantics(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.container.Terminate(context.Background()) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.Exec(`CREATE TEMP TABLE envelope_rows AS
SELECT row.*, public.orisun_event_document(row.data || jsonb_build_object('__eventId', row.event_id, '__eventType', row.event_type),
       row.metadata, row.transaction_id, row.global_id, row.write_id, row.date_created) AS document
FROM (VALUES
 (3::BIGINT, 5::BIGINT, 4::BIGINT, '00000000-0000-0000-0000-00000000000a'::UUID, '2026-10-09T00:00:00Z', 'A', '{}'::JSONB, '{"n":10,"s":"x","b":true,"z":null}'::JSONB),
 (11, 12, NULL, '00000000-0000-0000-0000-00000000000b', '2026-10-09T00:00:00.5Z', 'B', '{"trace":"x"}', '{"n":2.5,"s":"10"}'),
 (12, 12, 11, 'ffffffff-0000-0000-0000-00000000000c', '2026-10-10T01:02:03.123456Z', 'Z', NULL, '{}'),
 (40, 41, 40, '10000000-0000-0000-0000-00000000000d', '2025-01-01T00:00:00Z', 'b', '"m"', '{"n":-1}'),
 (100, 101, 100, '20000000-0000-0000-0000-00000000000e', '2027-01-01T00:00:00Z', 'Ä', '7', '{"s":"Ä"}')
) AS row(global_id, transaction_id, write_id, event_id, date_created, event_type, metadata, data)`)
	require.NoError(t, err)

	values := []string{"", "0", "5", "05", "12", "11", "41", "1e1", "-3", "2.5", "abc", "A", "B", "b", "Z", "Ä",
		"5:4", "12:11", "41:40", "2026-10-09T00:00:00Z", "2026-10-09T00:00:00.5Z", "2026-10-10", "2027",
		"00000000-0000-0000-0000-00000000000a", "10000000", "f", "{}", `{"trace": "x"}`, "m", "7", "true", "x", "10"}
	keys := append(append([]string{}, envelopeTestKeys...), "n", "s", "b", "z", "missing")
	checked := 0
	for _, key := range keys {
		for _, operator := range []string{"eq", "ne", "gt", "gte", "lt", "lte"} {
			for _, value := range values {
				fields := eventstore.EncodeCriterion([]eventstore.ReadTag{{Key: key, Value: value, Operator: operator}})
				document, err := candidateDocument("document").criterionSQL(fields)
				require.NoError(t, err)
				columns, err := storedRow("").criterionSQL(fields)
				require.NoError(t, err)
				var fromDocument, fromColumns string
				require.NoError(t, db.QueryRow(`SELECT
 COALESCE(string_agg(global_id::TEXT, ',' ORDER BY global_id) FILTER (WHERE `+document+`), ''),
 COALESCE(string_agg(global_id::TEXT, ',' ORDER BY global_id) FILTER (WHERE `+columns+`), '')
FROM envelope_rows`).Scan(&fromDocument, &fromColumns))
				require.Equal(t, fromDocument, fromColumns, "%s %s %q", key, operator, value)
				checked++
			}
		}
	}
	t.Logf("checked %d key/operator/value combinations", checked)
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
