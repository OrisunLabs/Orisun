//go:build foundationdb

package foundationdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStorageRejectsUnversionedRangesWithoutConversion(t *testing.T) {
	b := newTestBackend(t)
	key := b.tupleKey("unsupported", "event", "original")
	original := []byte(`{"event_id":"old","data":"{}"}`)
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) { tr.Set(key, original); return nil, nil })
	require.NoError(t, err)
	require.ErrorContains(t, b.ensureBoundaryMarker(t.Context(), "unsupported"), "unsupported")
	_, err = b.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		require.Equal(t, original, tr.Get(key).MustGet())
		require.Nil(t, tr.Get(b.tupleKey("unsupported", "schema", "version")).MustGet())
		require.Nil(t, tr.Get(b.boundaryMarkerKey("unsupported")).MustGet())
		return nil, nil
	})
	require.NoError(t, err)
}

func TestStorageAdoptsCompletedV013(t *testing.T) {
	b := newTestBackend(t)
	const boundary = "previous_release"
	original := []byte(`{"data":"{\"__eventId\":\"a\",\"__eventType\":\"Created\",\"__dateCreated\":\"2026-10-09T00:00:00Z\",\"__writeLastOffset\":null}"}`)
	key := b.tupleKey(boundary, "event", "original")
	contextKey := b.tupleKey(boundary, "write", "original")
	indexKey := b.tupleKey(boundary, "index_meta", "original")
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(key, original)
		tr.Set(contextKey, []byte("[]"))
		tr.Set(indexKey, []byte("unchanged"))
		tr.Set(b.tupleKey(boundary, "schema", "reserved_event_type"), []byte(`{"Stage":3}`))
		tr.Set(b.tupleKey(boundary, "schema", "event_id_document"), []byte(`{"Done":true}`))
		tr.Set(b.tupleKey(boundary, "schema", "envelope_document"), []byte(`{"Done":true}`))
		return nil, nil
	})
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, b.ensureBoundaryMarker(t.Context(), boundary))
	}
	_, err = b.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		require.Equal(t, original, tr.Get(key).MustGet())
		require.Equal(t, []byte("[]"), tr.Get(contextKey).MustGet())
		require.Equal(t, []byte("unchanged"), tr.Get(indexKey).MustGet())
		require.Equal(t, []byte("1"), tr.Get(b.tupleKey(boundary, "schema", "version")).MustGet())
		for _, name := range []string{"reserved_event_type", "event_id_document", "envelope_document"} {
			require.Nil(t, tr.Get(b.tupleKey(boundary, "schema", name)).MustGet())
		}
		return nil, nil
	})
	require.NoError(t, err)
	event, err := readEventFromRecord(original, 11, 10)
	require.NoError(t, err)
	require.Empty(t, event.WriteId)
	require.Equal(t, int64(11), event.CommitPosition)
	require.Equal(t, int64(10), event.PreparePosition)
}

func TestStorageRejectsIncompleteV013(t *testing.T) {
	for _, missing := range []string{"reserved_event_type", "event_id_document", "envelope_document"} {
		t.Run(missing, func(t *testing.T) {
			b := newTestBackend(t)
			const boundary = "incomplete"
			_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				for name, value := range map[string]string{"reserved_event_type": `{"Stage":3}`, "event_id_document": `{"Done":true}`, "envelope_document": `{"Done":true}`} {
					if name == missing {
						value = `{}`
					}
					tr.Set(b.tupleKey(boundary, "schema", name), []byte(value))
				}
				return nil, nil
			})
			require.NoError(t, err)
			require.ErrorContains(t, b.ensureBoundaryMarker(t.Context(), boundary), "incomplete")
			_, err = b.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
				require.Nil(t, tr.Get(b.tupleKey(boundary, "schema", "version")).MustGet())
				for _, name := range []string{"reserved_event_type", "event_id_document", "envelope_document"} {
					require.NotNil(t, tr.Get(b.tupleKey(boundary, "schema", name)).MustGet())
				}
				return nil, nil
			})
			require.NoError(t, err)
		})
	}
}
