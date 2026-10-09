//go:build foundationdb

package foundationdb

import (
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/internal/storagecontract"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestFoundationDBTagOperatorsContract(t *testing.T) {
	backend := newTestBackend(t)
	require.NoError(t, backend.CreateBoundaryIndex(t.Context(), "test", "operator_values", []orisun.BoundaryIndexField{{JsonKey: "operator_scope", ValueType: "text"}, {JsonKey: "value", ValueType: "text"}}, nil, "AND"))
	storagecontract.TagOperators(t, backend, backend, backend, "test")
}

func TestFoundationDBTagOperatorsNativePositions(t *testing.T) {
	backend := newTestBackend(t)
	events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
		{EventId: uuid.NewString(), EventType: "First", Data: map[string]any{}},
		{EventId: uuid.NewString(), EventType: "Second", Data: map[string]any{}},
	})
	require.NoError(t, err)
	tx, gid, err := backend.SavePrepared(t.Context(), events, "test", nil)
	require.NoError(t, err)
	commit, err := strconv.ParseInt(tx, 10, 64)
	require.NoError(t, err)
	for _, operator := range []string{"gte", "lte", "ne"} {
		value := tx
		if operator == "ne" {
			value = "0"
		}
		criterion := orisun.ReadCriterion{Tags: []orisun.ReadTag{{Key: "__commitPosition", Value: value, Operator: operator}, {Key: "__preparePosition", Value: strconv.FormatInt(gid, 10), Operator: "gte"}}}
		latest, err := backend.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: "test", Criteria: []orisun.ReadCriterion{criterion}})
		require.NoError(t, err)
		require.True(t, latest.Matches[0].Found)
		require.Equal(t, events[1].EventId, latest.Matches[0].Event.EventId)
		_, _, err = backend.SavePrepared(t.Context(), events, "test", []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{criterion}, Position: orisun.NotExistsPosition()}})
		require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
		require.Equal(t, commit, latest.ContextCommitPosition)
	}
}
