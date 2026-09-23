//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/goccy/go-json"
)

// Migration progress commits with each bounded batch. Startup resumes after a
// crash and does not expose a boundary until documents, contexts, and index
// metadata all use the new key. Run with older servers stopped.
func (b *Backend) migrateReservedEventType(ctx context.Context, boundary string) error {
	stateKey := b.tupleKey(boundary, "schema", "reserved_event_type")
	type progress struct {
		Stage int
		After []byte
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
			if state.Stage == 3 {
				return true, nil
			}
			if state.Stage < 0 || state.Stage > 3 {
				return false, fmt.Errorf("unsupported event-type migration stage %d", state.Stage)
			}
			prefix := b.eventPrefix(boundary)
			if state.Stage == 1 {
				prefix = b.tupleKey(boundary, "write")
			}
			if state.Stage == 2 {
				prefix = b.indexMetaPrefix(boundary)
			}
			rangeToRead := prefixRange(prefix)
			begin := rangeToRead.Begin.FDBKey()
			if state.After != nil {
				begin = fdb.Key(state.After)
			}
			limit := 32
			if state.Stage == 1 {
				limit = 1
			}
			rows, err := tr.GetRange(fdb.KeyRange{Begin: begin, End: rangeToRead.End}, fdb.RangeOptions{Limit: limit, Mode: fdb.StreamingModeWantAll}).GetSliceWithError()
			if err != nil {
				return false, err
			}
			if len(rows) == 0 {
				state.Stage++
				state.After = nil
			} else if state.Stage == 1 {
				// Contexts are split across values; migrate and rechunk one full
				// observation array atomically, preserving its write identifier.
				parts, err := tuple.Unpack(rows[0].Key)
				if err != nil {
					return false, err
				}
				if len(parts) != 5 {
					return false, fmt.Errorf("invalid write context chunk key")
				}
				parts = parts[:4]
				writeRange := prefixRange(fdb.Key(parts.Pack()))
				chunks, err := tr.GetRange(writeRange, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).GetSliceWithError()
				if err != nil {
					return false, err
				}
				var raw []byte
				for _, chunk := range chunks {
					raw = append(raw, chunk.Value...)
				}
				converted, err := eventdata.MigrateLegacyConsistency(raw)
				if err != nil {
					return false, err
				}
				tr.ClearRange(writeRange)
				for offset, chunk := 0, int64(0); offset < len(converted); offset, chunk = offset+90000, chunk+1 {
					key := append(append(tuple.Tuple{}, parts...), chunk)
					tr.Set(fdb.Key(key.Pack()), converted[offset:min(offset+90000, len(converted))])
				}
				state.After = writeRange.End.FDBKey()
			} else {
				for _, row := range rows {
					if state.Stage == 0 {
						encoded, err := migrateRecordEventType(row.Value)
						if err != nil {
							return false, err
						}
						tr.Set(row.Key, encoded)
					} else {
						var def indexDefinition
						if err := json.Unmarshal(row.Value, &def); err != nil {
							return false, err
						}
						for i := range def.Fields {
							if def.Fields[i].JsonKey == "eventType" {
								def.Fields[i].JsonKey = "__eventType"
							}
						}
						for i := range def.Conditions {
							if def.Conditions[i].Key == "eventType" {
								def.Conditions[i].Key = "__eventType"
							}
						}
						// Index tuples contain values, not field names; their
						// generation and entries remain valid after this rename.
						encoded, err := json.Marshal(def)
						if err != nil {
							return false, err
						}
						tr.Set(row.Key, encoded)
					}
					state.After = keyAfter(row.Key)
				}
			}
			encoded, err := json.Marshal(state)
			if err != nil {
				return false, err
			}
			tr.Set(stateKey, encoded)
			return state.Stage == 3, nil
		})
		if err != nil {
			return fmt.Errorf("migrate boundary %s event type: %w", boundary, err)
		}
		if result.(bool) {
			return nil
		}
	}
}
