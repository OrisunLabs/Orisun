package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

func migrateBoundaryStorage(ctx context.Context, tx *sql.Tx, schema, boundary string) error {
	table := func(suffix string) string {
		return pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+suffix)
	}
	versions := table("_orisun_schema_version")
	if _, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+versions+" (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO "+versions+" VALUES (1, 0) ON CONFLICT DO NOTHING"); err != nil {
		return err
	}
	var version int
	if err := tx.QueryRowContext(ctx, "SELECT version FROM "+versions+" WHERE id = 1 FOR UPDATE").Scan(&version); err != nil {
		return err
	}
	if version > 4 {
		return fmt.Errorf("boundary schema version %d is newer than supported version 4", version)
	}
	if version == 4 {
		return nil
	}

	if version < 1 {
		if err := migrateReservedEventType(ctx, tx, schema, boundary); err != nil {
			return err
		}
	}
	if version < 2 {
		if err := migrateEventID(ctx, tx, schema, boundary); err != nil {
			return err
		}
	}
	if version < 3 {
		if err := migrateEventEnvelope(ctx, tx, schema, boundary); err != nil {
			return err
		}
	}
	if err := migrateOrderedContextIndexes(ctx, tx, schema, boundary); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "UPDATE "+versions+" SET version = 4 WHERE id = 1")
	return err
}
