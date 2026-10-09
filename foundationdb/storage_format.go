//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func (b *Backend) requireBoundaryStorage(ctx context.Context, boundary string) error {
	_, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if err := contextStatusErr(ctx); err != nil {
			return nil, err
		}
		key := b.tupleKey(boundary, "schema", "version")
		version := tr.Get(key).MustGet()
		if version != nil {
			if string(version) != "1" {
				return nil, fmt.Errorf("unsupported boundary storage version %q", version)
			}
			return nil, nil
		}
		rows, err := tr.GetRange(prefixRange(b.tupleKey(boundary)), fdb.RangeOptions{Limit: 1}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(rows) != 0 {
			return nil, fmt.Errorf("unsupported unversioned boundary storage; refusing to convert existing events")
		}
		tr.Set(key, []byte("1"))
		return nil, nil
	})
	return err
}
