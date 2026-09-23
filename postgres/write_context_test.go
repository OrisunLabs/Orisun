package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/storagecontract"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWriteContextContract(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	logger, _ := logging.ZapLogger("error")
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{"test_boundary": {Boundary: "test_boundary", Schema: "public"}}
	saver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	defer saver.close()
	getter := NewPostgresGetEvents(db, logger, mapping)
	storagecontract.WriteContext(t, saver, getter, getter, "test_boundary")
	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM public.test_boundary_orisun_es_write").Scan(&count))
	require.Equal(t, 3, count)
	// Re-running initialization preserves both stored evidence and legacy NULLs.
	_, err = db.Exec(`INSERT INTO public.test_boundary_orisun_es_event(data) VALUES(orisun_event_document('{"__eventId":"00000000-0000-0000-0000-000000000001"}', '{}', 0, -1, NULL, now()))`)
	require.NoError(t, err)
	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()))
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM public.test_boundary_orisun_es_event WHERE write_id IS NULL").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM public.test_boundary_orisun_es_write").Scan(&count))
	require.Equal(t, 3, count)
}

func TestWriteContextSQLGroupCommitPaths(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	for _, path := range []string{"unconditional", "independent", "canonical", "isolated"} {
		t.Run(path, func(t *testing.T) {
			boundary := "write_" + path
			require.NoError(t, RunDbScripts(db, boundary, "public", false, t.Context()))
			payloads := make([]postgresBatchPayload, 3)
			checks := make([][]orisun.ConsistencyCheck, 3)
			for i := range payloads {
				value := fmt.Sprint(i)
				if path != "unconditional" {
					checks[i] = []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "context", Value: value}}}}, Position: orisun.NotExistsPosition()}}
					if i == 1 {
						checks[i][0].Position = orisun.Position{CommitPosition: 123, PreparePosition: 123}
					}
				}
				events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{
					{EventId: uuid.NewString(), EventType: "Created", Data: map[string]any{"context": value}},
					{EventId: uuid.NewString(), EventType: "Created", Data: map[string]any{"context": value}},
				})
				require.NoError(t, err)
				payloads[i].Consistency, err = orisun.MarshalConsistency(checks[i])
				require.NoError(t, err)
				payloads[i].Events, err = json.Marshal(events)
				require.NoError(t, err)
			}
			query := insertEventRequestsWithConsistency
			args := []any{boundary, "public"}
			switch path {
			case "unconditional":
				query = insertUnconditionalEventRequests
			case "independent":
				query = insertIndependentEventRequestsWithConsistency
				args = append(args, "context")
			case "canonical":
				query = insertCanonicalEventRequestsWithConsistency
			}
			payload, err := json.Marshal(payloads)
			require.NoError(t, err)
			args = append(args, payload)
			rows, err := db.QueryContext(t.Context(), fmt.Sprintf(query, "public"), args...)
			require.NoError(t, err)
			ids := map[int]string{}
			for rows.Next() {
				var index int
				var gid, tx, last sql.NullInt64
				var code, message sql.NullString
				require.NoError(t, rows.Scan(&index, &gid, &tx, &last, &code, &message))
				if index == 1 && path != "unconditional" {
					require.True(t, message.Valid)
					continue
				}
				require.False(t, message.Valid, message.String)
				ids[index] = orisun.WriteID(tx.Int64, last.Int64)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			want := 2
			if path == "unconditional" {
				want = 3
			}
			require.Len(t, ids, want)
			logger, _ := logging.ZapLogger("error")
			getter := NewPostgresGetEvents(db, logger, map[string]config.BoundaryToPostgresSchemaMapping{boundary: {Boundary: boundary, Schema: "public"}})
			seen := map[string]bool{}
			for index, id := range ids {
				require.False(t, seen[id])
				seen[id] = true
				actual, err := getter.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: boundary, WriteId: id})
				require.NoError(t, err)
				expected, err := orisun.DecodeWriteContext(id, payloads[index].Consistency)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
			events, err := getter.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Count: 100})
			require.NoError(t, err)
			require.Len(t, events, want*2)
			for _, event := range events {
				require.True(t, seen[event.WriteId])
			}
			var count int
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM public."+boundary+"_orisun_es_write").Scan(&count))
			require.Equal(t, want, count)
			// A row insertion failure after context allocation must roll the context
			// back as well. The isolated SQL entry point catches this per request.
			broken := []postgresBatchPayload{{Consistency: json.RawMessage(`[]`), Events: json.RawMessage(`[{"event_id":"invalid-uuid","event_type":"Invalid","data":{},"metadata":{}}]`)}}
			encoded, err := json.Marshal(broken)
			require.NoError(t, err)
			failed, err := db.Query(fmt.Sprintf(insertEventRequestsWithConsistency, "public"), boundary, "public", encoded)
			require.NoError(t, err)
			require.True(t, failed.Next())
			var index int
			var gid, tx, last sql.NullInt64
			var code, message sql.NullString
			require.NoError(t, failed.Scan(&index, &gid, &tx, &last, &code, &message))
			require.True(t, message.Valid)
			require.NoError(t, failed.Close())
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM public."+boundary+"_orisun_es_write").Scan(&count))
			require.Equal(t, want, count)
		})
	}
}

func TestBackendOwnsEventEnvelope(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.container.Terminate(context.Background()) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	logger, _ := logging.ZapLogger("error")
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{"test_boundary": {Boundary: "test_boundary", Schema: "public"}}
	saver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	defer saver.close()
	getter := NewPostgresGetEvents(db, logger, mapping)
	storagecontract.Envelope(t, saver, getter, NewPostgresAdminDB(db, logger, "public", "test_boundary", mapping), "test_boundary", func(fields string) {
		_, err := db.Exec("UPDATE test_boundary_orisun_es_event SET data = data || $1::jsonb", fields)
		require.NoError(t, err)
	})
	var id, kind string
	require.NoError(t, db.QueryRow("SELECT data->>'__eventId', data->>'__eventType' FROM test_boundary_orisun_es_event").Scan(&id, &kind))
	require.NotEmpty(t, id)
	require.Equal(t, "EnvelopeTest", kind)
}
