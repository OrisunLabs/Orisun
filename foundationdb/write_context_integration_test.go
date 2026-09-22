//go:build foundationdb

package foundationdb

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/storagecontract"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestFoundationDBWriteContext(t *testing.T) {
	backend := newTestBackend(t)
	for _, key := range []string{"context_test", "absent_context"} {
		require.NoError(t, backend.CreateBoundaryIndex(t.Context(), "test", key, []orisun.BoundaryIndexField{{JsonKey: key, ValueType: "text"}}, nil, orisun.IndexCombinatorAND))
	}
	require.NoError(t, backend.CreateBoundaryIndex(t.Context(), "test", "context_kind", []orisun.BoundaryIndexField{{JsonKey: "context_test", ValueType: "text"}, {JsonKey: "kind", ValueType: "text"}}, nil, orisun.IndexCombinatorAND))
	storagecontract.WriteContext(t, backend, backend, backend, "test")
}

func TestFoundationDBWriteContextSpansValues(t *testing.T) {
	backend := newTestBackend(t)
	require.NoError(t, backend.CreateBoundaryIndex(t.Context(), "test", "absent", []orisun.BoundaryIndexField{{JsonKey: "absent", ValueType: "text"}}, nil, orisun.IndexCombinatorAND))
	checks := make([]orisun.ConsistencyCheck, 400)
	for i := range checks {
		checks[i] = orisun.ConsistencyCheck{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "absent", Value: fmt.Sprint(i) + strings.Repeat("v", 256)}}}}, Position: orisun.NotExistsPosition()}
	}
	encoded, err := orisun.MarshalConsistency(checks)
	require.NoError(t, err)
	require.Greater(t, len(encoded), 100000)
	events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{EventId: uuid.NewString(), EventType: "LargeContext", Data: map[string]any{}}})
	require.NoError(t, err)
	tx, gid, err := backend.SavePrepared(t.Context(), events, "test", checks)
	require.NoError(t, err)
	commit, err := strconv.ParseInt(tx, 10, 64)
	require.NoError(t, err)
	id := orisun.WriteID(commit, gid)
	actual, err := backend.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: "test", WriteId: id})
	require.NoError(t, err)
	expected, err := orisun.DecodeWriteContext(id, encoded)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
