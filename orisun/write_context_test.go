package orisun

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteContextPreservesCompleteQueriesAndPositions(t *testing.T) {
	checks := []ConsistencyCheck{
		{Criteria: []ReadCriterion{
			{Tags: []ReadTag{{Key: "customer", Value: "c1"}, {Key: "__eventType", Value: "Opened"}}},
			{Tags: []ReadTag{{Key: "account", Value: "a1"}}},
		}, Position: Position{CommitPosition: 123, PreparePosition: 45}},
		{Criteria: []ReadCriterion{{Tags: []ReadTag{{Key: "transfer", Value: "t1"}}}}, Position: NotExistsPosition()},
	}
	data, err := MarshalConsistency(checks)
	require.NoError(t, err)
	require.JSONEq(t, `[
  {"query":{"criteria":[{"customer":"c1","__eventType":"Opened"},{"account":"a1"}]},"position":{"transaction_id":123,"global_id":45}},
  {"query":{"criteria":[{"transfer":"t1"}]},"position":{"transaction_id":-1,"global_id":-1}}
 ]`, string(data))
	decoded, err := DecodeWriteContext("200:50", data)
	require.NoError(t, err)
	require.Equal(t, "200:50", decoded.WriteId)
	restored, err := ConsistencyChecksFromObservations(decoded.Consistency)
	require.NoError(t, err)
	// Normalization may reorder criteria; compare their semantic query identity.
	for i, observation := range decoded.Consistency {
		require.Equal(t, checks[i].Position, *observation.Position)
		require.Equal(t, checks[i].Position, restored[i].Position)
	}
	require.Len(t, decoded.Consistency[0].Query.Criteria, 2)
	require.Len(t, decoded.Consistency[0].Query.Criteria[0].Tags, 2)
	require.Equal(t, "c1", decoded.Consistency[0].Query.Criteria[0].Tags[1].Value)
	require.Equal(t, "a1", decoded.Consistency[0].Query.Criteria[1].Tags[0].Value)
	data, err = MarshalConsistency(nil)
	require.NoError(t, err)
	require.Equal(t, "[]", string(data))
}

func TestWriteContextIDValidation(t *testing.T) {
	for _, id := range []string{"", "1", "-1:0", "0:-1", "+1:0", "01:0", "1:00", "1:2:3", "9223372036854775808:0"} {
		_, err := ValidateWriteContextRequest(&GetWriteContextRequest{Boundary: "test", WriteId: id})
		require.Error(t, err, id)
	}
	_, err := ValidateWriteContextRequest(nil)
	require.Error(t, err)
	_, err = ValidateWriteContextRequest(&GetWriteContextRequest{WriteId: "1:0"})
	require.Error(t, err)
	for _, position := range []Position{{}, {CommitPosition: 9223372036854775807, PreparePosition: 9223372036854775807}} {
		decoded, err := ValidateWriteContextRequest(&GetWriteContextRequest{Boundary: "test", WriteId: WriteID(position.CommitPosition, position.PreparePosition)})
		require.NoError(t, err)
		require.Equal(t, position, decoded)
	}
}
