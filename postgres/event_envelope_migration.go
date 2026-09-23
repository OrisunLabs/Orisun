package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

func migrateEventEnvelope(ctx context.Context, tx *sql.Tx, schema, boundary string) error {
	name := boundary + "_orisun_es_event"
	table := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(name)
	if _, err := tx.ExecContext(ctx, "LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		return err
	}
	var collision bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM "+table+" WHERE data ?| ARRAY['__commitPosition','__preparePosition','__writeId','__dateCreated','__metadata'])").Scan(&collision); err != nil {
		return err
	}
	if collision {
		return fmt.Errorf("legacy event data conflicts with reserved envelope fields")
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+table+" SET data = orisun_event_document(data, metadata, transaction_id, global_id, write_id, date_created)"); err != nil {
		return err
	}
	// Preserve every non-constraint index, including user-managed expressions and
	// partial predicates. Dropping/recreating is inside the migration transaction.
	rows, err := tx.QueryContext(ctx, `SELECT c.relname, pg_get_indexdef(i.indexrelid) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = $1::regclass AND NOT EXISTS (SELECT 1 FROM pg_constraint co WHERE co.conindid = i.indexrelid)`, table)
	if err != nil {
		return err
	}
	type index struct{ name, ddl string }
	var indexes []index
	for rows.Next() {
		var i index
		if err := rows.Scan(&i.name, &i.ddl); err != nil {
			rows.Close()
			return err
		}
		indexes = append(indexes, i)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, i := range indexes {
		if _, err := tx.ExecContext(ctx, "DROP INDEX "+pq.QuoteIdentifier(schema)+"."+pq.QuoteIdentifier(i.name)); err != nil {
			return err
		}
	}
	sequence := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_orisun_es_event_global_id_seq")
	if _, err := tx.ExecContext(ctx, "ALTER SEQUENCE "+sequence+" OWNED BY NONE"); err != nil {
		return err
	}
	ddl := `ALTER TABLE ` + table + `
 DROP COLUMN transaction_id, DROP COLUMN global_id, DROP COLUMN write_id, DROP COLUMN metadata, DROP COLUMN date_created,
 ADD COLUMN transaction_id BIGINT GENERATED ALWAYS AS ((data->>'__commitPosition')::BIGINT) STORED NOT NULL,
 ADD COLUMN global_id BIGINT GENERATED ALWAYS AS ((data->>'__preparePosition')::BIGINT) STORED NOT NULL PRIMARY KEY,
 ADD COLUMN write_id BIGINT GENERATED ALWAYS AS (NULLIF(split_part(data->>'__writeId', ':', 2), '')::BIGINT) STORED REFERENCES ` + pq.QuoteIdentifier(schema) + `.` + pq.QuoteIdentifier(boundary+"_orisun_es_write") + `(write_id),
 ADD COLUMN metadata JSONB GENERATED ALWAYS AS (data->'__metadata') STORED,
 ADD COLUMN date_created TEXT GENERATED ALWAYS AS (data->>'__dateCreated') STORED NOT NULL,
 ADD CONSTRAINT ` + pq.QuoteIdentifier(boundary+"_event_date_valid") + ` CHECK ((jsonb_typeof(data->'__dateCreated') = 'string' AND (data->>'__dateCreated')::timestamptz IS NOT NULL) IS TRUE)`
	if _, err := tx.ExecContext(ctx, ddl); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "ALTER SEQUENCE "+sequence+" OWNED BY "+table+".global_id"); err != nil {
		return err
	}
	for _, i := range indexes {
		if _, err := tx.ExecContext(ctx, i.ddl); err != nil {
			return err
		}
	}
	return nil
}
