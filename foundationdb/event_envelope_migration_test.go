//go:build foundationdb

package foundationdb

import (
	"fmt"
	"testing"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestFoundationDBEnvelopeMigrationResumesAndPreservesNativePositions(t *testing.T) {
	b := newTestBackend(t)
	require.NoError(t, b.CreateBoundaryIndex(t.Context(), "test", "created", []eventstore.BoundaryIndexField{{JsonKey: "__dateCreated", ValueType: "text"}}, nil, "AND"))
	key := func(i int64) fdb.Key {
		return b.eventKeyForPosition("test", &eventstore.Position{CommitPosition: 42, PreparePosition: i})
	}
	seed := func(i int64, collision bool) []byte {
		data := fmt.Sprintf(`{"__eventId":"id-%d","__eventType":"Created","number":9223372036854775807}`, i)
		if collision {
			data = `{"__metadata":null}`
		}
		raw, err := json.Marshal(map[string]any{"data": data, "metadata": `{"__trace":9223372036854775807}`, "date_created": "2026-01-02T03:04:05.123456789Z", "write_last_offset": uint16(39)})
		require.NoError(t, err)
		return raw
	}
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for i := int64(0); i < 40; i++ {
			tr.Set(key(i), seed(i, i == 39))
		}
		return nil, nil
	})
	require.NoError(t, err)
	require.ErrorContains(t, b.migrateEventEnvelope(t.Context(), "test"), "conflicts")
	_, err = b.db.Transact(func(tr fdb.Transaction) (interface{}, error) { tr.Set(key(39), seed(39, false)); return nil, nil })
	require.NoError(t, err)
	require.NoError(t, b.migrateEventEnvelope(t.Context(), "test"))
	require.NoError(t, b.migrateEventEnvelope(t.Context(), "test"))
	for _, tag := range []*eventstore.Tag{{Key: "__dateCreated", Value: "2026-01-02T03:04:05.123456789Z"}, {Key: "__writeId", Value: "42:39"}} {
		events, err := b.GetBatch(t.Context(), &eventstore.GetEventsRequest{Boundary: "test", Count: 100, Query: &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{tag}}}}})
		require.NoError(t, err)
		require.Len(t, events, 40)
		for i, event := range events {
			require.EqualValues(t, 42, event.CommitPosition)
			require.EqualValues(t, i, event.PreparePosition)
			require.Equal(t, "42:39", event.WriteId)
			require.Contains(t, event.Data, "9223372036854775807")
			require.NotContains(t, event.Data, "__")
			require.Equal(t, `{"__trace":9223372036854775807}`, event.Metadata)
		}
	}
	_, err = b.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		var raw map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(tr.Get(key(0)).MustGet(), &raw))
		require.Len(t, raw, 1)
		require.Contains(t, raw, "data")
		return nil, nil
	})
	require.NoError(t, err)
}

func TestFoundationDBNativeEnvelopePagingAndIndexValidation(t *testing.T) {
	b := newTestBackend(t)
	events := eventstore.PreparedEventBatch{}
	for i := 0; i < 4; i++ {
		events = append(events, eventstore.PreparedEvent{EventId: fmt.Sprint(i), EventType: "Created", DataJSON: fmt.Sprintf(`{"which":"%d"}`, i), MetadataJSON: `{}`})
	}
	_, _, err := b.SavePrepared(t.Context(), events, "test", nil)
	require.NoError(t, err)
	all, err := b.GetBatch(t.Context(), &eventstore.GetEventsRequest{Boundary: "test", Count: 10})
	require.NoError(t, err)
	require.Len(t, all, 4)
	query := &eventstore.Query{Criteria: []*eventstore.Criterion{{Tags: []*eventstore.Tag{{Key: "__writeId", Value: all[0].WriteId}, {Key: "which", Value: "3"}}}}}
	filtered, err := b.GetBatch(t.Context(), &eventstore.GetEventsRequest{Boundary: "test", Count: 1, Query: query})
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	require.Equal(t, "3", filtered[0].EventId)
	query.Criteria[0].Tags = query.Criteria[0].Tags[:1]
	for _, direction := range []eventstore.Direction{eventstore.Direction_ASC, eventstore.Direction_DESC} {
		page, err := b.GetBatch(t.Context(), &eventstore.GetEventsRequest{Boundary: "test", Count: 2, Query: query, Direction: direction, FromPosition: &eventstore.Position{CommitPosition: all[2].CommitPosition, PreparePosition: all[2].PreparePosition}})
		require.NoError(t, err)
		require.Len(t, page, 2)
		require.Equal(t, "2", page[0].EventId)
		if direction == eventstore.Direction_ASC {
			require.Equal(t, "3", page[1].EventId)
		} else {
			require.Equal(t, "1", page[1].EventId)
		}
	}
	for _, key := range []string{"__commitPosition", "__preparePosition", "__writeId"} {
		require.ErrorContains(t, b.CreateBoundaryIndex(t.Context(), "test", "bad", []eventstore.BoundaryIndexField{{JsonKey: key, ValueType: "text"}}, nil, "AND"), "native position")
	}
}
