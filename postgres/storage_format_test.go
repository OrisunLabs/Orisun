package postgres

import (
	"context"
	"fmt"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBoundaryStorageRejectsOlderFormatsWithoutConversion(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() { require.NoError(t, container.container.Terminate(context.Background())) }()
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	for _, version := range []int{0, 1, 3, 6} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			boundary := fmt.Sprintf("old_format_%d", version)
			table := "public." + boundary + "_orisun_es_event"
			_, err := db.Exec("CREATE TABLE " + table + "(data JSONB); INSERT INTO " + table + " VALUES ('{\"original\":true}')")
			require.NoError(t, err)
			if version != 0 {
				_, err = db.Exec("CREATE TABLE public." + boundary + "_orisun_schema_version(id INTEGER PRIMARY KEY,version INTEGER); INSERT INTO public." + boundary + "_orisun_schema_version VALUES (1," + fmt.Sprint(version) + ")")
				require.NoError(t, err)
			}
			require.ErrorContains(t, RunDbScripts(db, boundary, "public", false, t.Context()), "unsupported")
			var data string
			require.NoError(t, db.QueryRow("SELECT data::TEXT FROM "+table).Scan(&data))
			require.JSONEq(t, `{"original":true}`, data)
			if version != 0 {
				var got int
				require.NoError(t, db.QueryRow("SELECT version FROM public."+boundary+"_orisun_schema_version WHERE id=1").Scan(&got))
				require.Equal(t, version, got)
			}
		})
	}
}

func TestBoundaryStorageUpgradesV013(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	const boundary = "previous_release"
	require.NoError(t, RunDbScripts(db, boundary, "public", false, t.Context()))
	// Version 4 has the same generated document schema, nullable historical
	// write IDs, durable contexts, and user-managed indexes.
	_, err = db.Exec(`
 UPDATE public.previous_release_orisun_schema_version SET version = 4;
 CREATE TABLE public.previous_release_orisun_last_published_event_position(position BIGINT);
 INSERT INTO public.previous_release_orisun_last_published_event_position VALUES (99);
 INSERT INTO public.previous_release_orisun_es_write VALUES (10, '[{"query":{"criteria":[{"tags":[{"key":"account","value":"a"}]}]},"position":{"commit_position":-1,"prepare_position":-1}}]');
 INSERT INTO public.previous_release_orisun_es_event(data) VALUES
 ('{"__eventId":"00000000-0000-0000-0000-000000000001","__eventType":"Created","__commitPosition":11,"__preparePosition":10,"__writeId":"11:10","__dateCreated":"2026-10-09T00:00:00Z","__metadata":{},"account":"a"}'),
 ('{"__eventId":"00000000-0000-0000-0000-000000000002","__eventType":"Historical","__commitPosition":1,"__preparePosition":0,"__writeId":null,"__dateCreated":"2026-10-08T00:00:00Z","__metadata":{},"account":"b"}');
 CREATE INDEX previous_release_custom ON public.previous_release_orisun_es_event ((data->>'account'));
 SELECT setval('public.previous_release_orisun_es_event_global_id_seq', 100, false);
 `)
	require.NoError(t, err)
	var before, contextBefore string
	require.NoError(t, db.QueryRow("SELECT jsonb_agg(data ORDER BY global_id)::TEXT FROM public.previous_release_orisun_es_event").Scan(&before))
	require.NoError(t, db.QueryRow("SELECT consistency::TEXT FROM public.previous_release_orisun_es_write WHERE write_id=10").Scan(&contextBefore))
	// Even a completed upgrade is rolled back if initialization cannot commit.
	tx, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, requireBoundaryStorage(t.Context(), tx, "public", boundary))
	require.NoError(t, tx.Rollback())
	var version int
	require.NoError(t, db.QueryRow("SELECT version FROM public.previous_release_orisun_schema_version").Scan(&version))
	require.Equal(t, 4, version)
	var checkpoint int
	require.NoError(t, db.QueryRow("SELECT position FROM public.previous_release_orisun_last_published_event_position").Scan(&checkpoint))
	require.Equal(t, 99, checkpoint)
	for range 2 {
		require.NoError(t, RunDbScripts(db, boundary, "public", false, t.Context()))
	}
	var after, contextAfter string
	require.NoError(t, db.QueryRow("SELECT jsonb_agg(data ORDER BY global_id)::TEXT FROM public.previous_release_orisun_es_event").Scan(&after))
	require.Equal(t, before, after)
	require.NoError(t, db.QueryRow("SELECT consistency::TEXT FROM public.previous_release_orisun_es_write WHERE write_id=10").Scan(&contextAfter))
	require.Equal(t, contextBefore, contextAfter)
	require.NoError(t, db.QueryRow("SELECT version FROM public.previous_release_orisun_schema_version").Scan(&version))
	require.Equal(t, boundarySchemaVersion, version)
	var exists bool
	require.NoError(t, db.QueryRow("SELECT to_regclass('public.previous_release_orisun_last_published_event_position') IS NOT NULL").Scan(&exists))
	require.False(t, exists)
	require.NoError(t, db.QueryRow("SELECT to_regclass('public.previous_release_custom') IS NOT NULL").Scan(&exists))
	require.True(t, exists)
	var next int
	require.NoError(t, db.QueryRow("SELECT nextval('public.previous_release_orisun_es_event_global_id_seq')").Scan(&next))
	require.Equal(t, 100, next)
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{boundary: {Boundary: boundary, Schema: "public"}}
	getter := NewPostgresGetEvents(db, logger, mapping)
	batch, err := getter.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Count: 100})
	require.NoError(t, err)
	require.Len(t, batch, 2)
	require.Empty(t, batch[0].WriteId)
	require.Equal(t, "11:10", batch[1].WriteId)
	latest, err := getter.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: boundary, Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "account", Value: "b"}}}}})
	require.NoError(t, err)
	require.True(t, latest.Matches[0].Found)
	require.Empty(t, latest.Matches[0].Event.WriteId)
	saver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	defer saver.close()
	_, lastID, err := saver.Save(t.Context(), []orisun.EventWithMapTags{{EventId: "00000000-0000-0000-0000-000000000003", EventType: "AfterUpgrade", Data: map[string]any{"account": "a"}}}, boundary, nil)
	require.NoError(t, err)
	require.Greater(t, lastID, int64(100))
}
