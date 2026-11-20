package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestBoundaryStorageRejectsOlderFormatsWithoutConversion(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() { require.NoError(t, container.container.Terminate(context.Background())) }()
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	for _, version := range []int{0, 1, 3, 7} {
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

// legacyBoundaryFixture creates storage exactly as releases 0.13 and 0.14 left
// it: the envelope inside data, generated envelope columns, and the write
// foreign key and duplicate position index later releases removed.
func legacyBoundaryFixture(t *testing.T, db *sql.DB, boundary string, version int, events string) {
	t.Helper()
	_, err := db.Exec(strings.ReplaceAll(`
 CREATE TABLE public.B_orisun_schema_version (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL);
 INSERT INTO public.B_orisun_schema_version VALUES (1, `+fmt.Sprint(version)+`);
 CREATE TABLE public.B_orisun_es_write (write_id BIGINT PRIMARY KEY, consistency JSONB NOT NULL CHECK (jsonb_typeof(consistency) = 'array'));
 CREATE TABLE public.B_orisun_es_event (
  data JSONB NOT NULL CHECK (jsonb_typeof(data) = 'object'),
  pg_xact_id BIGINT,
  transaction_id BIGINT GENERATED ALWAYS AS ((data->>'__commitPosition')::BIGINT) STORED NOT NULL,
  global_id BIGINT GENERATED ALWAYS AS ((data->>'__preparePosition')::BIGINT) STORED NOT NULL PRIMARY KEY,
  write_id BIGINT GENERATED ALWAYS AS (NULLIF(split_part(data->>'__writeId', ':', 2), '')::BIGINT) STORED REFERENCES public.B_orisun_es_write(write_id),
  metadata JSONB GENERATED ALWAYS AS (data->'__metadata') STORED,
  date_created TEXT GENERATED ALWAYS AS (data->>'__dateCreated') STORED NOT NULL,
  CHECK ((jsonb_typeof(data->'__dateCreated') = 'string' AND (data->>'__dateCreated')::timestamptz IS NOT NULL) IS TRUE));
 CREATE SEQUENCE public.B_orisun_es_event_global_id_seq START WITH 0 MINVALUE 0 OWNED BY public.B_orisun_es_event.global_id;
 CREATE INDEX B_idx_global_order_covering ON public.B_orisun_es_event (transaction_id DESC, global_id DESC);
 CREATE INDEX B_idx_event_type_order ON public.B_orisun_es_event ((data->>'__eventType'), transaction_id DESC, global_id DESC);
 CREATE INDEX B_idx_event_order_visibility_covering ON public.B_orisun_es_event (transaction_id DESC, global_id DESC) INCLUDE (pg_xact_id);
 CREATE TABLE public.B_orisun_boundary_index_metadata (name TEXT PRIMARY KEY, fields JSONB NOT NULL, conditions JSONB NOT NULL DEFAULT '[]', combinator TEXT NOT NULL DEFAULT 'AND', state TEXT NOT NULL DEFAULT 'ready', date_created TIMESTAMPTZ DEFAULT NOW() NOT NULL, date_updated TIMESTAMPTZ DEFAULT NOW() NOT NULL);
 INSERT INTO public.B_orisun_boundary_index_metadata (name, fields, conditions) VALUES
  ('by_type', '[{"JsonKey":"__eventType","ValueType":"text"},{"JsonKey":"account","ValueType":"text"}]', '[]'),
  ('created_accounts', '[{"JsonKey":"account","ValueType":"text"}]', '[{"Key":"__eventType","Operator":"=","Value":"Created"}]'),
  ('never_built', '[{"JsonKey":"account","ValueType":"text"}]', '[]');
 CREATE INDEX B_by_type_idx ON public.B_orisun_es_event ((data->>'__eventType'), (data->>'account'), transaction_id DESC, global_id DESC);
 CREATE INDEX B_created_accounts_idx ON public.B_orisun_es_event ((data->>'account'), transaction_id DESC, global_id DESC) WHERE (data->>'__eventType') = 'Created';
 CREATE INDEX B_custom ON public.B_orisun_es_event ((data->>'account'));
 INSERT INTO public.B_orisun_es_write VALUES (0, '[]'), (10, '[{"query":{"criteria":[{"tags":[{"key":"account","value":"a"}]}]},"position":{"commit_position":-1,"prepare_position":-1}}]');
 INSERT INTO public.B_orisun_es_event(data, pg_xact_id) VALUES `+events+`;
 SELECT setval('public.B_orisun_es_event_global_id_seq', 100, false);
 `, "B_", boundary+"_"))
	require.NoError(t, err)
}

const legacyEvents = `
 ('{"__eventId":"00000000-0000-0000-0000-000000000001","__eventType":"Created","__commitPosition":11,"__preparePosition":10,"__writeId":"11:10","__dateCreated":"2026-10-09T00:00:00.25Z","__metadata":{"trace":"t"},"account":"a","n":7}', 5),
 ('{"__eventId":"00000000-0000-0000-0000-000000000002","__eventType":"Historical","__commitPosition":1,"__preparePosition":0,"__writeId":null,"__dateCreated":"2026-10-08T00:00:00Z","__metadata":{},"account":"b"}', NULL)`

func TestBoundaryStorageUpgradesEnvelopeColumns(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	for _, version := range []int{4, 5} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			boundary := fmt.Sprintf("previous_v%d", version)
			table := func(suffix string) string { return "public." + boundary + suffix }
			legacyBoundaryFixture(t, db, boundary, version, legacyEvents)
			if version == 4 {
				_, err := db.Exec("CREATE TABLE " + table("_orisun_last_published_event_position") + "(position BIGINT)")
				require.NoError(t, err)
			}
			var before string
			require.NoError(t, db.QueryRow("SELECT jsonb_agg(data ORDER BY global_id)::TEXT FROM "+table("_orisun_es_event")).Scan(&before))

			// Even a completed upgrade is rolled back if initialization cannot commit.
			tx, err := db.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			require.NoError(t, requireBoundaryStorage(t.Context(), tx, "public", boundary))
			require.NoError(t, tx.Rollback())
			var stored int
			require.NoError(t, db.QueryRow("SELECT version FROM "+table("_orisun_schema_version")).Scan(&stored))
			require.Equal(t, version, stored)
			var unchanged string
			require.NoError(t, db.QueryRow("SELECT jsonb_agg(data ORDER BY global_id)::TEXT FROM "+table("_orisun_es_event")).Scan(&unchanged))
			require.Equal(t, before, unchanged)

			for range 2 {
				require.NoError(t, RunDbScripts(db, boundary, "public", false, t.Context()))
			}
			require.NoError(t, db.QueryRow("SELECT version FROM "+table("_orisun_schema_version")).Scan(&stored))
			require.Equal(t, boundarySchemaVersion, stored)
			var exists bool
			require.NoError(t, db.QueryRow("SELECT to_regclass($1) IS NOT NULL", table("_orisun_last_published_event_position")).Scan(&exists))
			require.False(t, exists)

			// Envelope fields moved to columns; data keeps application fields.
			var rows string
			require.NoError(t, db.QueryRow(`SELECT jsonb_agg(jsonb_build_array(global_id, transaction_id, write_id, pg_xact_id, event_id, date_created, event_type, metadata, data) ORDER BY global_id)::TEXT FROM `+table("_orisun_es_event")).Scan(&rows))
			require.JSONEq(t, `[
 [0, 1, null, null, "00000000-0000-0000-0000-000000000002", "2026-10-08T00:00:00Z", "Historical", {}, {"account":"b"}],
 [10, 11, 10, 5, "00000000-0000-0000-0000-000000000001", "2026-10-09T00:00:00.25Z", "Created", {"trace":"t"}, {"account":"a","n":7}]]`, rows)
			var contextAfter string
			require.NoError(t, db.QueryRow("SELECT consistency::TEXT FROM "+table("_orisun_es_write")+" WHERE write_id=10").Scan(&contextAfter))
			require.JSONEq(t, `[{"query":{"criteria":[{"tags":[{"key":"account","value":"a"}]}]},"position":{"commit_position":-1,"prepare_position":-1}}]`, contextAfter)

			var owner sql.NullString
			require.NoError(t, db.QueryRow("SELECT pg_get_serial_sequence($1, 'global_id')", table("_orisun_es_event")).Scan(&owner))
			require.Equal(t, table("_orisun_es_event_global_id_seq"), owner.String)
			var next int
			require.NoError(t, db.QueryRow("SELECT nextval($1)", table("_orisun_es_event_global_id_seq")).Scan(&next))
			require.Equal(t, 100, next)

			indexes := map[string]string{}
			rowsCursor, err := db.Query("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = $1", boundary+"_orisun_es_event")
			require.NoError(t, err)
			for rowsCursor.Next() {
				var name, definition string
				require.NoError(t, rowsCursor.Scan(&name, &definition))
				indexes[strings.TrimPrefix(name, boundary+"_")] = definition
			}
			require.NoError(t, rowsCursor.Err())
			require.ElementsMatch(t, []string{"orisun_es_event_pkey", "idx_event_type_order", "idx_event_order_visibility_covering", "by_type_idx", "created_accounts_idx", "custom"}, keys(indexes))
			require.Contains(t, indexes["idx_event_type_order"], "(event_type, transaction_id DESC, global_id DESC)")
			require.Contains(t, indexes["by_type_idx"], "(event_type, ((data ->> 'account'::text)), transaction_id DESC, global_id DESC)")
			require.Contains(t, indexes["created_accounts_idx"], "WHERE (event_type = 'Created'::text)")
			require.Contains(t, indexes["custom"], "((data ->> 'account'::text))")
			var foreignKeys int
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM pg_constraint WHERE contype = 'f' AND conrelid = $1::regclass", table("_orisun_es_event")).Scan(&foreignKeys))
			require.Zero(t, foreignKeys)

			logger, err := logging.ZapLogger("error")
			require.NoError(t, err)
			mapping := map[string]config.BoundaryToPostgresSchemaMapping{boundary: {Boundary: boundary, Schema: "public"}}
			getter := NewPostgresGetEvents(db, logger, mapping)
			batch, err := getter.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: boundary, Count: 100})
			require.NoError(t, err)
			require.Len(t, batch, 2)
			require.Empty(t, batch[0].WriteId)
			require.Equal(t, "11:10", batch[1].WriteId)
			require.JSONEq(t, `{"account":"a","n":7}`, batch[1].Data)
			require.JSONEq(t, `{"trace":"t"}`, batch[1].Metadata)
			for _, tags := range [][]orisun.ReadTag{
				{{Key: "__eventType", Value: "Created"}},
				{{Key: "__writeId", Value: "11:10"}},
				{{Key: "__dateCreated", Value: "2026-10-09T00:00:00.25Z"}},
				{{Key: "__commitPosition", Value: "5", Operator: "gt"}},
				{{Key: "__metadata", Value: `{"trace": "t"}`}},
			} {
				latest, err := getter.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: boundary, Criteria: []orisun.ReadCriterion{{Tags: tags}}})
				require.NoError(t, err)
				require.True(t, latest.Matches[0].Found, "%v", tags)
				require.Equal(t, "00000000-0000-0000-0000-000000000001", latest.Matches[0].Event.EventId)
			}
			saver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
			defer saver.close()
			_, lastID, err := saver.Save(t.Context(), []orisun.EventWithMapTags{{EventId: "00000000-0000-0000-0000-000000000003", EventType: "AfterUpgrade", Data: map[string]any{"account": "a"}}}, boundary, nil)
			require.NoError(t, err)
			require.Equal(t, int64(101), lastID) // 100 was drawn above
		})
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	return out
}

// The upgrade validates every document before changing anything.
func TestBoundaryStorageUpgradeRefusesUnrepresentableDocuments(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()

	for name, tc := range map[string]struct{ events, setup, message string }{
		"unknown reserved field": {
			events:  `('{"__eventId":"00000000-0000-0000-0000-000000000001","__eventType":"A","__commitPosition":1,"__preparePosition":0,"__writeId":"1:0","__dateCreated":"2026-10-09T00:00:00Z","__metadata":{},"__future":1}', NULL)`,
			message: `unknown reserved field "__future"`,
		},
		"write id disagrees with position": {
			events:  `('{"__eventId":"00000000-0000-0000-0000-000000000001","__eventType":"A","__commitPosition":1,"__preparePosition":0,"__writeId":"2:0","__dateCreated":"2026-10-09T00:00:00Z","__metadata":{}}', NULL)`,
			message: "write ID or position",
		},
		"noncanonical event id": {
			events:  `('{"__eventId":"00000000-0000-0000-0000-00000000000A","__eventType":"A","__commitPosition":1,"__preparePosition":0,"__writeId":"1:0","__dateCreated":"2026-10-09T00:00:00Z","__metadata":{}}', NULL)`,
			message: "canonical event ID",
		},
		"unmanaged reserved index": {
			events:  `('{"__eventId":"00000000-0000-0000-0000-000000000001","__eventType":"A","__commitPosition":1,"__preparePosition":0,"__writeId":"1:0","__dateCreated":"2026-10-09T00:00:00Z","__metadata":{}}', NULL)`,
			setup:   "CREATE INDEX B_by_kind ON public.B_orisun_es_event ((data->>'__eventType'))",
			message: "references reserved event fields",
		},
	} {
		t.Run(name, func(t *testing.T) {
			boundary := "refuse_" + strings.ReplaceAll(name, " ", "_")
			legacyBoundaryFixture(t, db, boundary, 5, tc.events)
			if tc.setup != "" {
				_, err := db.Exec(strings.ReplaceAll(tc.setup, "B_", boundary+"_"))
				require.NoError(t, err)
			}
			var before string
			require.NoError(t, db.QueryRow("SELECT jsonb_agg(data ORDER BY global_id)::TEXT FROM public."+boundary+"_orisun_es_event").Scan(&before))
			require.ErrorContains(t, RunDbScripts(db, boundary, "public", false, t.Context()), tc.message)
			var after string
			require.NoError(t, db.QueryRow("SELECT jsonb_agg(data ORDER BY global_id)::TEXT FROM public."+boundary+"_orisun_es_event").Scan(&after))
			require.Equal(t, before, after)
			var version int
			require.NoError(t, db.QueryRow("SELECT version FROM public."+boundary+"_orisun_schema_version").Scan(&version))
			require.Equal(t, 5, version)
		})
	}
}
