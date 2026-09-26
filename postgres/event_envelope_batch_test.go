package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestCanonicalBatchMatchesCompleteEnvelope(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.container.Terminate(context.Background()) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for i, tag := range []orisun.ReadTag{{Key: "__eventType", Value: "Created"}, {Key: "__commitPosition", Value: "1"}, {Key: "__preparePosition", Value: "0"}, {Key: "__writeId", Value: "1:0"}} {
		t.Run(tag.Key, func(t *testing.T) {
			boundary := fmt.Sprintf("envelope_batch_%d", i)
			require.NoError(t, RunDbScripts(db, boundary, "public", false, t.Context()))
			events, err := json.Marshal(orisun.PreparedEventBatch{{EventId: uuid.NewString(), EventType: "Created", DataJSON: `{}`, MetadataJSON: `{}`}})
			require.NoError(t, err)
			checks, err := orisun.MarshalConsistency([]orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{tag}}}, Position: orisun.NotExistsPosition()}})
			require.NoError(t, err)
			payload, err := json.Marshal([]postgresBatchPayload{{Events: events, Consistency: json.RawMessage(`[]`)}, {Events: events, Consistency: checks}})
			require.NoError(t, err)
			rows, err := db.Query(fmt.Sprintf(insertEventRequestsWithConsistency, "public"), boundary, "public", payload)
			require.NoError(t, err)
			defer rows.Close()
			count := 0
			for rows.Next() {
				var index int
				var gid, tx, last sql.NullInt64
				var code, message sql.NullString
				require.NoError(t, rows.Scan(&index, &gid, &tx, &last, &code, &message))
				if index == 0 {
					require.False(t, message.Valid, message.String)
				} else {
					require.Contains(t, message.String, "StreamVersionConflict")
				}
				count++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, 2, count)
		})
	}
}

func TestEnvelopeWritesResolveTheirOwnSchema(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.container.Terminate(context.Background()) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	// A caller's default public path must not accidentally supply the helper.
	_, err = db.Exec("DROP FUNCTION public.orisun_event_document(jsonb,jsonb,bigint,bigint,bigint,timestamptz)")
	require.NoError(t, err)
	for i := range 2 {
		boundary := fmt.Sprintf("scoped_%d", i)
		require.NoError(t, RunDbScripts(db, boundary, "envelope_tenant", false, t.Context()))
		events, err := json.Marshal(orisun.PreparedEventBatch{{EventId: uuid.NewString(), EventType: "Created", DataJSON: `{"context":"one"}`, MetadataJSON: `{}`}})
		require.NoError(t, err)
		consistency := json.RawMessage(`[]`)
		if i != 1 {
			consistency, err = orisun.MarshalConsistency([]orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "context", Value: "one"}}}}, Position: orisun.NotExistsPosition()}})
			require.NoError(t, err)
		}
		payload, err := json.Marshal([]postgresBatchPayload{{Events: events, Consistency: consistency}})
		require.NoError(t, err)
		rows, err := db.Query(fmt.Sprintf(insertEventRequestsWithConsistency, "envelope_tenant"), boundary, "envelope_tenant", payload)
		require.NoError(t, err)
		require.True(t, rows.Next())
		var index int
		var gid, tx, last sql.NullInt64
		var code, message sql.NullString
		require.NoError(t, rows.Scan(&index, &gid, &tx, &last, &code, &message))
		require.False(t, message.Valid, message.String)
		require.NoError(t, rows.Close())
	}
}
