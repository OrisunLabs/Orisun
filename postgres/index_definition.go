package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/lib/pq"
)

// PostgreSQL deparses both definitions, so harmless SQL spelling differences
// do not prevent adoption of an existing index. No event data is copied.
const postgresIndexDefinitionSQL = `
SELECT i.indisvalid, i.indrelid = to_regclass($2),
 jsonb_build_object(
  'unique', i.indisunique, 'exclusion', i.indisexclusion,
  'method', am.amname, 'key_count', i.indnkeyatts,
  'columns', ARRAY(SELECT pg_get_indexdef(i.indexrelid, n, false)
                   FROM generate_series(1, i.indnatts) n),
  'predicate', pg_get_expr(i.indpred, i.indrelid)
 )::text
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_am am ON am.oid = c.relam
WHERE i.indexrelid = to_regclass($1)`

func (db *PostgresAdminDB) verifyBoundaryIndexDefinition(ctx context.Context, schema, boundary, name, expressions, predicate string) (bool, error) {
	index := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_"+name+"_idx")
	table := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_orisun_es_event")
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var valid, sameTable bool
	var actual string
	err = tx.QueryRowContext(ctx, postgresIndexDefinitionSQL, index, table).Scan(&valid, &sameTable, &actual)
	if errors.Is(err, sql.ErrNoRows) {
		var occupied bool
		if err := tx.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", index).Scan(&occupied); err != nil {
			return false, err
		}
		if occupied {
			return false, statuscode.Errorf(statuscode.AlreadyExists, "physical name for index %q is occupied by another relation", name)
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !sameTable {
		return false, statuscode.Errorf(statuscode.AlreadyExists, "index %q belongs to a different table", name)
	}
	if _, err = tx.ExecContext(ctx, `CREATE TEMP TABLE orisun_index_definition (data JSONB, transaction_id BIGINT, global_id BIGINT) ON COMMIT DROP`); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, "CREATE INDEX orisun_index_definition_idx ON orisun_index_definition USING btree ("+expressions+")"+predicate); err != nil {
		return false, err
	}
	var expected string
	var ignored bool
	if err = tx.QueryRowContext(ctx, postgresIndexDefinitionSQL, "pg_temp.orisun_index_definition_idx", "pg_temp.orisun_index_definition").Scan(&ignored, &ignored, &expected); err != nil {
		return false, err
	}
	if actual != expected {
		return false, statuscode.Errorf(statuscode.AlreadyExists, "index %q already exists with a different physical definition; drop it before replacing it", name)
	}
	return true, nil
}

func (db *PostgresAdminDB) reserveBoundaryIndex(ctx context.Context, schema, boundary, name string, fieldsJSON, conditionsJSON []byte, combinator, expressions, predicate string) (*eventstore.BoundaryIndex, error) {
	table := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_orisun_boundary_index_metadata")
	_, err := db.db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (name,fields,conditions,combinator,state,date_created,date_updated)
 VALUES ($1,$2::jsonb,$3::jsonb,$4,$5,NOW(),NOW()) ON CONFLICT(name) DO NOTHING`, table), name, string(fieldsJSON), string(conditionsJSON), combinator, eventstore.BoundaryIndexStateBuilding)
	if err != nil {
		return nil, err
	}
	existing, err := db.checkBoundaryIndexMetadata(ctx, boundary, name, expressions, predicate)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, statuscode.Errorf(statuscode.FailedPrecondition, "index %q was dropped during creation", name)
	}
	return existing, nil
}

func (db *PostgresAdminDB) checkBoundaryIndexMetadata(ctx context.Context, boundary, name, expressions, predicate string) (*eventstore.BoundaryIndex, error) {
	existing, err := db.GetBoundaryIndex(ctx, boundary, name)
	if statuscode.CodeOf(err) == statuscode.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	existingExpressions, existingPredicate, err := boundaryIndexExpressions(existing.Fields, existing.Conditions, existing.Combinator)
	if err != nil {
		return nil, err
	}
	if existingExpressions != expressions || existingPredicate != predicate {
		return nil, statuscode.Errorf(statuscode.AlreadyExists, "index %q already exists with a different definition; drop it before replacing it", name)
	}
	return existing, nil
}
