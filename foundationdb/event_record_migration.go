//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/goccy/go-json"
)

// migrateEventRecords commits each transformed batch and its resume cursor
// together. Boundaries stay unavailable until every storage stage has finished.
func (b *Backend) migrateEventRecords(ctx context.Context, boundary, migration string, transform func(fdb.Transaction, []fdb.KeyValue) error) error {
	stateKey := b.tupleKey(boundary, "schema", migration)
	type progress struct {
		After []byte
		Done  bool
	}
	for {
		result, err := b.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			if err := contextStatusErr(ctx); err != nil {
				return false, err
			}
			var state progress
			if raw := tr.Get(stateKey).MustGet(); raw != nil {
				if err := json.Unmarshal(raw, &state); err != nil {
					return false, err
				}
			}
			if state.Done {
				return true, nil
			}
			r := prefixRange(b.eventPrefix(boundary))
			begin := r.Begin.FDBKey()
			if state.After != nil {
				begin = fdb.Key(state.After)
			}
			rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: r.End}, fdb.RangeOptions{Limit: 32, Mode: fdb.StreamingModeWantAll}).GetSliceWithError()
			if err != nil {
				return false, err
			}
			if err := transform(tr, rows); err != nil {
				return false, err
			}
			if len(rows) > 0 {
				state.After = keyAfter(rows[len(rows)-1].Key)
			}
			state.Done = len(rows) < 32
			value, err := json.Marshal(state)
			if err != nil {
				return false, err
			}
			tr.Set(stateKey, value)
			return state.Done, nil
		})
		if err != nil {
			return fmt.Errorf("migrate boundary %s (%s): %w", boundary, migration, err)
		}
		if result.(bool) {
			return nil
		}
	}
}
