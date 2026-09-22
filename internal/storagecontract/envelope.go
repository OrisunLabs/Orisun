package storagecontract

import (
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Envelope verifies the backend contract directly, without API preparation or
// response adapters. Persistence owns reserved fields; reads return domain data.
func Envelope(t *testing.T, saver orisun.EventsSaver, reader orisun.EventsRetriever, indexes orisun.BoundaryIndexManager, boundary string, addStoredFields func(string)) {
	t.Helper()
	require.NoError(t, indexes.CreateBoundaryIndex(t.Context(), boundary, "envelope_lookup", []orisun.BoundaryIndexField{
		{JsonKey: "__eventId", ValueType: "text"}, {JsonKey: "__eventType", ValueType: "text"},
	}, nil, "AND"))
	id := uuid.NewString()
	const domain = `{"eventId":"application-id","eventType":"application-type","nested":{"__eventId":"nested","items":[{"__future":true}]},"_single":"kept","ordinary__key":"kept","number":9223372036854775807}`
	input := orisun.PreparedEventBatch{{EventId: id, EventType: "EnvelopeTest", DataJSON: domain, MetadataJSON: `{"__trace":"preserved"}`}}
	criteria := []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "__eventId", Value: id}, {Key: "__eventType", Value: "EnvelopeTest"}}}}
	checks := []orisun.ConsistencyCheck{{Criteria: criteria, Position: orisun.NotExistsPosition()}}
	_, _, err := saver.SavePrepared(t.Context(), input, boundary, checks)
	require.NoError(t, err)
	require.Equal(t, domain, input[0].DataJSON, "backend must not mutate caller input")
	// Seed future system fields directly in storage, bypassing application validation.
	addStoredFields(`{"__future":null,"__":{"secret":true},"___internal":[1,2]}`)
	_, _, err = saver.SavePrepared(t.Context(), input, boundary, checks)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
	read, err := reader.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Count: 10, Query: &orisun.Query{Criteria: []*orisun.Criterion{{Tags: []*orisun.Tag{{Key: "__eventId", Value: id}, {Key: "__eventType", Value: "EnvelopeTest"}}}}}})
	require.NoError(t, err)
	require.Len(t, read, 1)
	latest, err := reader.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: boundary, Criteria: criteria})
	require.NoError(t, err)
	require.Len(t, latest.Matches, 1)
	require.True(t, latest.Matches[0].Found)
	for _, event := range []orisun.ReadEvent{read[0], latest.Matches[0].Event} {
		require.Equal(t, id, event.EventId)
		require.Equal(t, "EnvelopeTest", event.EventType)
		var data map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(event.Data), &data))
		require.NotContains(t, data, "__eventId")
		require.NotContains(t, data, "__eventType")
		require.NotContains(t, data, "__future")
		require.NotContains(t, data, "__")
		require.NotContains(t, data, "___internal")
		require.JSONEq(t, domain, event.Data)
		require.JSONEq(t, `{"__trace":"preserved"}`, event.Metadata)
		require.Equal(t, `"application-id"`, string(data["eventId"]))
		require.Equal(t, `"application-type"`, string(data["eventType"]))
		require.Equal(t, `9223372036854775807`, string(data["number"]))
		require.JSONEq(t, `{"__eventId":"nested","items":[{"__future":true}]}`, string(data["nested"]))
	}
}
