//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func (b *Backend) migrateEventEnvelope(ctx context.Context, boundary string) error {
	return b.migrateEventRecords(ctx, boundary, "envelope_document", func(tr fdb.Transaction, rows []fdb.KeyValue) error {
		indexes, err := b.loadIndexes(tr, boundary)
		if err != nil {
			return err
		}
		for _, idx := range indexes {
			for _, field := range idx.Fields {
				if eventdata.IsPositionKey(field.JsonKey) {
					return fmt.Errorf("index %s uses commit-derived field %s; remove it before migration", idx.Name, field.JsonKey)
				}
			}
			for _, condition := range idx.Conditions {
				if eventdata.IsPositionKey(condition.Key) {
					return fmt.Errorf("index %s uses commit-derived condition %s; remove it before migration", idx.Name, condition.Key)
				}
			}
		}
		for _, row := range rows {
			value, err := migrateRecordEnvelope(row.Value)
			if err != nil {
				return err
			}
			tr.Set(row.Key, value)
			tx, gid, err := eventPositionFromKey(row.Key)
			if err != nil {
				return err
			}
			_, data, err := decodeEventRecord(value)
			if err != nil {
				return err
			}
			for _, idx := range indexes {
				if eventMatchesIndexConditions(data, idx) {
					if key, ok := b.indexKeyAtPosition(boundary, idx, data, &eventstore.Position{CommitPosition: tx, PreparePosition: gid}); ok {
						tr.Set(key, []byte{})
					}
				}
			}
		}
		return nil
	})
}
