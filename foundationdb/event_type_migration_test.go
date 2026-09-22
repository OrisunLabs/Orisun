//go:build foundationdb

package foundationdb

import (
	"fmt"
	"testing"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestFoundationDBReservedEventTypeMigration(t *testing.T) {
	b := newTestBackend(t)
	position := func(i int64) *orisun.Position { return &orisun.Position{CommitPosition: 42, PreparePosition: i} }
	seed := func(i int64, collision bool) []byte {
		data := `{"eventType":"Created","number":9223372036854775807,"nested":{"eventType":"domain"}}`
		if collision {
			data = `{"eventType":"Created","__eventType":"collision"}`
		}
		record := map[string]any{"event_id": fmt.Sprint(i), "event_type": "Created", "data": data, "metadata": "{}", "date_created": "2026-01-01T00:00:00Z"}
		value, err := json.Marshal(record)
		require.NoError(t, err)
		return value
	}
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for i := int64(0); i < 40; i++ {
			tr.Set(b.eventKeyForPosition("test", position(i)), seed(i, i == 39))
		}
		return nil, nil
	})
	require.NoError(t, err)
	require.NoError(t, b.CreateBoundaryIndex(t.Context(), "test", "legacy", []orisun.BoundaryIndexField{{JsonKey: "eventType", ValueType: "text"}}, nil, "AND"))
	generation := readIndexGeneration(t, b, "test", "legacy")
	// Use multiple chunks to exercise context reconstruction and rechunking.
	context := []byte(`[{"query":{"criteria":[{"eventType":"Created"}]},"position":{"transaction_id":-1,"global_id":-1}}]`)
	_, err = b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for offset, chunk := 0, int64(0); offset < len(context); offset, chunk = offset+20, chunk+1 {
			tr.Set(b.tupleKey("test", "write", versionstampFromPosition(position(39)), chunk), context[offset:min(offset+20, len(context))])
		}
		return nil, nil
	})
	require.NoError(t, err)
	// One batch commits before this conflict; retry must resume safely.
	require.ErrorContains(t, b.migrateBoundaryStorage(t.Context(), "test"), "conflicts")
	_, err = b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(b.eventKeyForPosition("test", position(39)), seed(39, false))
		return nil, nil
	})
	require.NoError(t, err)
	require.NoError(t, b.migrateBoundaryStorage(t.Context(), "test"))
	require.NoError(t, b.migrateBoundaryStorage(t.Context(), "test"))
	require.Equal(t, generation, readIndexGeneration(t, b, "test", "legacy"))
	events, err := b.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: "test", Count: 100, Query: &orisun.Query{Criteria: []*orisun.Criterion{{Tags: []*orisun.Tag{{Key: "__eventType", Value: "Created"}}}}}})
	require.NoError(t, err)
	require.Len(t, events, 40)
	for _, event := range events {
		require.Equal(t, "Created", event.EventType)
		require.NotEmpty(t, event.EventId)
		require.NotContains(t, event.Data, "__eventId")
		require.NotContains(t, event.Data, "__eventType")
		require.Contains(t, event.Data, `"number":9223372036854775807`)
		require.Contains(t, event.Data, `"nested":{"eventType":"domain"}`)
	}
	write, err := b.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: "test", WriteId: "42:39"})
	require.NoError(t, err)
	require.Equal(t, "__eventType", write.Consistency[0].Query.Criteria[0].Tags[0].Key)
	_, err = b.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		raw := tr.Get(b.eventKeyForPosition("test", position(0))).MustGet()
		var record map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &record))
		require.NotContains(t, record, "event_type")
		require.NotContains(t, record, "event_id")
		return nil, nil
	})
	require.NoError(t, err)
}
