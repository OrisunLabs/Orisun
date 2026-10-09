//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/goccy/go-json"
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
			// The previous release recorded completion instead of a version number.
			// Accept only fully completed stages, without resuming old conversions.
			for _, name := range []string{"reserved_event_type", "event_id_document", "envelope_document"} {
				var state struct {
					Stage int
					Done  bool
				}
				raw := tr.Get(b.tupleKey(boundary, "schema", name)).MustGet()
				if len(raw) == 0 || json.Unmarshal(raw, &state) != nil || (name == "reserved_event_type" && state.Stage != 3) || (name != "reserved_event_type" && !state.Done) {
					return nil, fmt.Errorf("unsupported unversioned boundary storage: previous release upgrade %s is incomplete", name)
				}
			}
			for _, name := range []string{"reserved_event_type", "event_id_document", "envelope_document"} {
				tr.Clear(b.tupleKey(boundary, "schema", name))
			}
		}
		tr.Set(key, []byte("1"))
		return nil, nil
	})
	return err
}
