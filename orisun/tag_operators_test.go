package orisun

import (
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTagOperatorLiveMatching(t *testing.T) {
	store := &EventStore{}
	for _, tc := range []struct {
		name, data, op, value string
		want                  bool
	}{
		{"numeric_gt", `{"n":10}`, "gt", "2", true},
		{"text_gt", `{"n":"10"}`, "gt", "2", false},
		{"large_integer", `{"n":9007199254740993}`, "gt", "9007199254740992", true},
		{"large_integer_equal", `{"n":9007199254740993}`, "eq", "9007199254740993", true},
		{"precise_decimal", `{"n":0.10000000000000000001}`, "gt", "0.1", true},
		{"negative", `{"n":-10}`, "lt", "-2", true},
		{"exponent", `{"n":1e100}`, "gte", "1e99", true},
		{"ne", `{"n":true}`, "ne", "false", true},
		{"default_eq", `{"n":10}`, "", "10", true},
		{"boolean_unordered", `{"n":true}`, "gt", "false", false},
		{"null_eq", `{"n":null}`, "eq", "null", false},
		{"null_ne", `{"n":null}`, "ne", "null", false},
		{"missing_ne", `{}`, "ne", "10", false},
		{"invalid_numeric_target", `{"n":10}`, "gt", "invalid", false},
		{"unknown_operator", `{"n":10}`, "GE", "2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := &Query{Criteria: []*Criterion{{Tags: []*Tag{{Key: "n", Value: tc.value, Operator: tc.op}}}}}
			require.Equal(t, tc.want, store.eventMatchesQueryCriteria(&Event{Data: tc.data, Metadata: `{}`}, query))
		})
	}
	query := &Query{Criteria: []*Criterion{
		{Tags: []*Tag{{Key: "n", Value: "2", Operator: "gte"}, {Key: "n", Value: "20", Operator: "lt"}}},
		{Tags: []*Tag{{Key: "kind", Value: "override"}}},
	}}
	for _, data := range []string{`{"n":10}`, `{"n":20,"kind":"override"}`} {
		require.True(t, store.eventMatchesQueryCriteria(&Event{Data: data, Metadata: `{}`}, query))
	}
	require.False(t, store.eventMatchesQueryCriteria(&Event{Data: `{"n":20}`, Metadata: `{}`}, query))
}

func TestOperatorQueryNormalizationAndWriteContext(t *testing.T) {
	query := &Query{Criteria: []*Criterion{{Tags: []*Tag{
		{Key: "value", Value: "10", Operator: "gte"},
		{Key: "value", Value: "20", Operator: "lt"},
		{Key: "type", Value: "Created", Operator: "eq"},
	}}}}
	criteria, key, err := normalizeConsistencyQuery(query)
	require.NoError(t, err)
	require.Len(t, criteria[0].Tags, 3)
	reversed := &Query{Criteria: []*Criterion{{Tags: []*Tag{query.Criteria[0].Tags[2], query.Criteria[0].Tags[1], query.Criteria[0].Tags[0], query.Criteria[0].Tags[0]}}}}
	_, sameKey, err := normalizeConsistencyQuery(reversed)
	require.NoError(t, err)
	require.Equal(t, key, sameKey)
	query.Criteria[0].Tags[0].Operator = "gt"
	_, differentKey, err := normalizeConsistencyQuery(query)
	require.NoError(t, err)
	require.NotEqual(t, key, differentKey)
	checks := []ConsistencyCheck{{Criteria: criteria, Position: NotExistsPosition()}}
	data, err := MarshalConsistency(checks)
	require.NoError(t, err)
	require.True(t, json.Valid(data))
	decoded, err := DecodeWriteContext("1:0", data)
	require.NoError(t, err)
	restored, err := consistencyChecksFromObservations(decoded.Consistency)
	require.NoError(t, err)
	require.Equal(t, checks, restored)
	query.Criteria[0].Tags[0].Operator = "GT"
	_, _, err = normalizeConsistencyQuery(query)
	require.Error(t, err)
	for _, op := range []string{"", "eq"} {
		_, canonical, err := normalizeConsistencyQuery(&Query{Criteria: []*Criterion{{Tags: []*Tag{{Key: "a", Value: "b", Operator: op}}}}})
		require.NoError(t, err)
		if op == "" {
			sameKey = canonical
		} else {
			require.Equal(t, sameKey, canonical)
		}
	}
}
