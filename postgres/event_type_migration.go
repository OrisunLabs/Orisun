package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/lib/pq"
)

// The migration runs before the boundary is made available. All nodes must be
// upgraded together: older writers do not understand the new document schema.
func migrateReservedEventType(ctx context.Context, tx *sql.Tx, schema, boundary string) error {
	table := func(suffix string) string {
		return pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+suffix)
	}
	if _, err := tx.ExecContext(ctx, "LOCK TABLE "+table("_orisun_es_event")+", "+table("_orisun_es_write")+", "+table("_orisun_boundary_index_metadata")+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		return err
	}
	for _, spec := range []struct {
		suffix, id, column string
		convert            func([]byte) ([]byte, error)
	}{
		{"_orisun_es_event", "global_id", "data", eventdata.MigrateLegacyEventType},
		{"_orisun_es_write", "write_id", "consistency", eventdata.MigrateLegacyConsistency},
	} {
		var after int64 = -1
		for {
			rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s > $1 ORDER BY %s LIMIT 256", spec.id, spec.column, table(spec.suffix), spec.id, spec.id), after)
			if err != nil {
				return err
			}
			type row struct {
				id    int64
				value []byte
			}
			var batch []row
			for rows.Next() {
				var row row
				if err := rows.Scan(&row.id, &row.value); err != nil {
					rows.Close()
					return err
				}
				batch = append(batch, row)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				break
			}
			for _, row := range batch {
				value, err := spec.convert(row.value)
				if err != nil {
					return fmt.Errorf("migrate %s row %d: %w", spec.suffix, row.id, err)
				}
				if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET %s = $1::jsonb WHERE %s = $2", table(spec.suffix), spec.column, spec.id), string(value), row.id); err != nil {
					return err
				}
				after = row.id
			}
		}
	}
	type index struct {
		name, combinator   string
		fields, conditions []byte
	}
	var indexes []index
	rows, err := tx.QueryContext(ctx, "SELECT name, fields, conditions, combinator FROM "+table("_orisun_boundary_index_metadata"))
	if err != nil {
		return err
	}
	for rows.Next() {
		var idx index
		if err := rows.Scan(&idx.name, &idx.fields, &idx.conditions, &idx.combinator); err != nil {
			rows.Close()
			return err
		}
		indexes = append(indexes, idx)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		fields, fc, err := eventdata.MigrateLegacyIndexKeys(idx.fields, "JsonKey")
		if err != nil {
			return err
		}
		conditions, cc, err := eventdata.MigrateLegacyIndexKeys(idx.conditions, "Key")
		if err != nil {
			return err
		}
		if !fc && !cc {
			continue
		}
		var fs []orisun.BoundaryIndexField
		var cs []orisun.BoundaryIndexCondition
		if err := json.Unmarshal(fields, &fs); err != nil {
			return err
		}
		if err := json.Unmarshal(conditions, &cs); err != nil {
			return err
		}
		expressions, where, err := boundaryIndexExpressions(fs, cs, idx.combinator)
		if err != nil {
			return err
		}
		name := pq.QuoteIdentifier(boundary + "_" + idx.name + "_idx")
		if _, err := tx.ExecContext(ctx, "DROP INDEX IF EXISTS "+pq.QuoteIdentifier(schema)+"."+name); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "CREATE INDEX "+name+" ON "+table("_orisun_es_event")+" USING btree ("+expressions+")"+where); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE "+table("_orisun_boundary_index_metadata")+" SET fields = $1::jsonb, conditions = $2::jsonb, state = 'ready', date_updated = NOW() WHERE name = $3", string(fields), string(conditions), idx.name); err != nil {
			return err
		}
	}
	name := pq.QuoteIdentifier(boundary + "_idx_event_type_order")
	if _, err := tx.ExecContext(ctx, "DROP INDEX IF EXISTS "+pq.QuoteIdentifier(schema)+"."+name); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "CREATE INDEX "+name+" ON "+table("_orisun_es_event")+" ((data->>'__eventType'), transaction_id DESC, global_id DESC)"); err != nil {
		return err
	}
	return nil
}
