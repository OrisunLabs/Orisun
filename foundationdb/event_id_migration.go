//go:build foundationdb

package foundationdb

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func (b *Backend) migrateBoundaryStorage(ctx context.Context, boundary string) error {
	if err := b.migrateReservedEventType(ctx, boundary); err != nil {
		return err
	}
	if err := b.migrateEventID(ctx, boundary); err != nil {
		return err
	}
	return b.migrateEventEnvelope(ctx, boundary)
}

func (b *Backend) migrateEventID(ctx context.Context, boundary string) error {
	return b.migrateEventRecords(ctx, boundary, "event_id_document", func(tr fdb.Transaction, rows []fdb.KeyValue) error {
		for _, row := range rows {
			value, err := migrateRecordEventID(row.Value)
			if err != nil {
				return err
			}
			tr.Set(row.Key, value)
		}
		return nil
	})
}
