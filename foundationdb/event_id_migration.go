//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/goccy/go-json"
)

func (b *Backend) migrateBoundaryStorage(ctx context.Context, boundary string) error {
	if err := b.migrateReservedEventType(ctx, boundary); err != nil {
		return err
	}
	return b.migrateEventID(ctx, boundary)
}

func (b *Backend) migrateEventID(ctx context.Context, boundary string) error {
	stateKey := b.tupleKey(boundary, "schema", "event_id_document")
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
			for _, row := range rows {
				value, err := migrateRecordEventID(row.Value)
				if err != nil {
					return false, err
				}
				tr.Set(row.Key, value)
				state.After = keyAfter(row.Key)
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
			return fmt.Errorf("migrate boundary %s event IDs: %w", boundary, err)
		}
		if result.(bool) {
			return nil
		}
	}
}
