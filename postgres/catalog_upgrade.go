package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

func boundaryEventTableExists(
	ctx context.Context,
	db *sql.DB,
	schema string,
	boundary string,
) (bool, error) {
	exists, err := postgresTableExists(ctx, db, schema, boundary+"_orisun_es_event")
	if err != nil {
		return false, fmt.Errorf("inspect PostgreSQL admin event store: %w", err)
	}
	return exists, nil
}

func catalogNativeMarkerExists(
	ctx context.Context,
	db *sql.DB,
	schema string,
	boundary string,
) (bool, error) {
	exists, err := postgresTableExists(ctx, db, schema, boundary+"_orisun_catalog_native")
	if err != nil {
		return false, fmt.Errorf("inspect PostgreSQL catalog bootstrap marker: %w", err)
	}
	return exists, nil
}

func postgresTableExists(ctx context.Context, db *sql.DB, schema, table string) (bool, error) {
	const query = `
SELECT EXISTS (
	SELECT 1
	FROM pg_catalog.pg_class AS tables
	JOIN pg_catalog.pg_namespace AS schemas ON schemas.oid = tables.relnamespace
	WHERE schemas.nspname = $1
	  AND tables.relname = $2
	  AND tables.relkind IN ('r', 'p')
)`

	var exists bool
	if err := db.QueryRowContext(ctx, query, schema, table).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func markCatalogNativeInstall(
	ctx context.Context,
	db *sql.DB,
	schema string,
	boundary string,
) error {
	table := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_orisun_catalog_native")
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+table+" (singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton))"); err != nil {
		return fmt.Errorf("record PostgreSQL catalog-native installation: %w", err)
	}
	return nil
}
