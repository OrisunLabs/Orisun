package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

func migrateEventID(ctx context.Context, tx *sql.Tx, schema, boundary string) error {
	table := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_orisun_es_event")
	if _, err := tx.ExecContext(ctx, "LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		return err
	}
	var collision bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM "+table+" WHERE data ? '__eventId')").Scan(&collision); err != nil {
		return err
	}
	if collision {
		return fmt.Errorf("legacy event data conflicts with reserved __eventId")
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+table+" SET data = jsonb_set(data, '{__eventId}', to_jsonb(event_id::text), true)"); err != nil {
		return err
	}
	// Preserve the former UUID NOT NULL column invariant in the document.
	if _, err := tx.ExecContext(ctx, "ALTER TABLE "+table+" ADD CONSTRAINT "+pq.QuoteIdentifier(boundary+"_event_id_valid")+" CHECK ((jsonb_typeof(data->'__eventId') = 'string' AND (data->>'__eventId')::uuid IS NOT NULL) IS TRUE), DROP COLUMN event_id"); err != nil {
		return err
	}
	return nil
}
