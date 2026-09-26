package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/lib/pq"
)

// Upgrade only metadata-owned indexes whose physical definitions match the
// old or current model. The boundary migration transaction makes the rebuild
// and version update atomic, including rollback on an ownership conflict.
func migrateOrderedContextIndexes(ctx context.Context, tx *sql.Tx, schema, boundary string) error {
	table := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_orisun_es_event")
	metadata := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_orisun_boundary_index_metadata")
	if _, err := tx.ExecContext(ctx, "LOCK TABLE "+table+", "+metadata+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		return err
	}
	type index struct {
		name, combinator   string
		fields, conditions []byte
	}
	var indexes []index
	rows, err := tx.QueryContext(ctx, "SELECT name, fields, conditions, combinator FROM "+metadata+" ORDER BY name")
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
		name := pq.QuoteIdentifier(boundary + "_" + idx.name + "_idx")
		qualified := pq.QuoteIdentifier(schema) + "." + name
		var valid, sameTable bool
		var actual string
		err := tx.QueryRowContext(ctx, postgresIndexDefinitionSQL, qualified, table).Scan(&valid, &sameTable, &actual)
		if errors.Is(err, sql.ErrNoRows) {
			var occupied bool
			if err := tx.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", qualified).Scan(&occupied); err != nil {
				return err
			}
			if occupied {
				return fmt.Errorf("migrate index %q: physical name is occupied by another relation", idx.name)
			}
			// A failed or pending create has no physical index to migrate.
			continue
		}
		if err != nil {
			return err
		}
		if !sameTable {
			return fmt.Errorf("migrate index %q: physical index belongs to another table", idx.name)
		}
		var fields []orisun.BoundaryIndexField
		var conditions []orisun.BoundaryIndexCondition
		if err := json.Unmarshal(idx.fields, &fields); err != nil {
			return err
		}
		if err := json.Unmarshal(idx.conditions, &conditions); err != nil {
			return err
		}
		expressions, predicate, err := boundaryIndexExpressions(fields, conditions, idx.combinator)
		if err != nil {
			return err
		}
		expected, err := expectedPostgresIndexDefinition(ctx, tx, expressions, predicate)
		if err != nil {
			return err
		}
		if actual == expected && valid {
			continue
		}
		if actual != expected {
			oldKeys, oldPredicate, err := boundaryIndexKeyExpressions(fields, conditions, idx.combinator)
			if err != nil {
				return err
			}
			oldDefinition, err := expectedPostgresIndexDefinition(ctx, tx, oldKeys, oldPredicate)
			if err != nil {
				return err
			}
			if actual != oldDefinition {
				return fmt.Errorf("migrate index %q: physical definition differs from metadata", idx.name)
			}
		}
		if _, err := tx.ExecContext(ctx, "DROP INDEX "+qualified); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "CREATE INDEX "+name+" ON "+table+" USING btree ("+expressions+")"+predicate); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE "+metadata+" SET state = 'ready', date_updated = NOW() WHERE name = $1", idx.name); err != nil {
			return err
		}
	}
	return nil
}
