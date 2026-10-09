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
