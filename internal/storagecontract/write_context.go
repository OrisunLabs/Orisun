// Package storagecontract contains backend-independent persistence assertions.
package storagecontract

import (
	"strconv"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// WriteContext verifies batch ownership, complete query observations, stale-save
// rejection, unconditional saves, and associations across ordinary/latest reads.
func WriteContext(t *testing.T, saver orisun.EventsSaver, reader orisun.EventsRetriever, contexts orisun.WriteContextRetriever, boundary string) {
	t.Helper()
	key := uuid.NewString()
	checks := []orisun.ConsistencyCheck{
		{
			Criteria: []orisun.ReadCriterion{
				{Tags: []orisun.ReadTag{{Key: "context_test", Value: key}, {Key: "kind", Value: "a"}}},
				{Tags: []orisun.ReadTag{{Key: "context_test", Value: key}, {Key: "kind", Value: "b"}}},
			},
			Position: orisun.NotExistsPosition(),
		},
		{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "absent_context", Value: key}}}}, Position: orisun.NotExistsPosition()},
	}
	prepare := func(kind string) orisun.PreparedEventBatch {
		events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
			{EventId: uuid.NewString(), EventType: "ContextRecorded", Data: map[string]any{"context_test": key, "kind": kind}},
			{EventId: uuid.NewString(), EventType: "ContextRecorded", Data: map[string]any{"context_test": key, "kind": kind}},
		})
		require.NoError(t, err)
		return events
	}
	tx, gid, err := saver.SavePrepared(t.Context(), prepare("a"), boundary, checks)
	require.NoError(t, err)
	commit, err := strconv.ParseInt(tx, 10, 64)
	require.NoError(t, err)
	writeID := orisun.WriteID(commit, gid)
	ctx, err := contexts.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: boundary, WriteId: writeID})
	require.NoError(t, err)
	encoded, err := orisun.MarshalConsistency(checks)
	require.NoError(t, err)
	expected, err := orisun.DecodeWriteContext(writeID, encoded)
	require.NoError(t, err)
	require.Equal(t, expected, ctx)
	_, _, err = saver.SavePrepared(t.Context(), prepare("a"), boundary, checks)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))

	// A successful non-empty observed position must be retained, not replaced by
	// the new commit position.
	checks[0].Position = orisun.Position{CommitPosition: commit, PreparePosition: gid}
	tx2, gid2, err := saver.SavePrepared(t.Context(), prepare("b"), boundary, checks)
	require.NoError(t, err)
	commit2, err := strconv.ParseInt(tx2, 10, 64)
	require.NoError(t, err)
	id2 := orisun.WriteID(commit2, gid2)
	ctx2, err := contexts.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: boundary, WriteId: id2})
	require.NoError(t, err)
	require.Equal(t, checks[0].Position, *ctx2.Consistency[0].Position)
	require.NotEqual(t, writeID, id2)

	tx3, gid3, err := saver.SavePrepared(t.Context(), prepare("c"), boundary, nil)
	require.NoError(t, err)
	commit3, err := strconv.ParseInt(tx3, 10, 64)
	require.NoError(t, err)
	id3 := orisun.WriteID(commit3, gid3)
	unconditional, err := contexts.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: boundary, WriteId: id3})
	require.NoError(t, err)
	require.Empty(t, unconditional.Consistency)
	events, err := reader.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Count: 100, Query: &orisun.Query{Criteria: []*orisun.Criterion{{Tags: []*orisun.Tag{{Key: "context_test", Value: key}}}}}})
	require.NoError(t, err)
	require.Len(t, events, 6)
	for i, event := range events {
		require.Equal(t, []string{writeID, id2, id3}[i/2], event.WriteId)
	}
	latest, err := reader.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: boundary, Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "context_test", Value: key}}}}})
	require.NoError(t, err)
	require.Equal(t, id3, latest.Matches[0].Event.WriteId)
	_, err = contexts.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: boundary, WriteId: "9223372036854775807:0"})
	require.Equal(t, statuscode.NotFound, statuscode.CodeOf(err))
	_, err = contexts.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: boundary, WriteId: ""})
	require.Equal(t, statuscode.InvalidArgument, statuscode.CodeOf(err))
	_, err = contexts.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: "unknown_context_boundary", WriteId: writeID})
	require.Equal(t, statuscode.InvalidArgument, statuscode.CodeOf(err))
}
