package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
)

const boundarySchemaVersion = 5

// Validate the storage format before initialization can modify existing objects.
func requireBoundaryStorage(ctx context.Context, tx *sql.Tx, schema, boundary string) error {
	table := func(suffix string) string {
		return pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+suffix)
	}
	versions := table("_orisun_schema_version")
	var exists bool
	if err := tx.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", versions).Scan(&exists); err != nil {
		return err
	}
	if exists {
		var version int
		if err := tx.QueryRowContext(ctx, "SELECT version FROM "+versions+" WHERE id = 1").Scan(&version); err != nil {
			return err
		}
		if version != boundarySchemaVersion {
			return fmt.Errorf("unsupported boundary schema version %d; required version %d", version, boundarySchemaVersion)
		}
		return nil
	}
	if err := tx.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", table("_orisun_es_event")).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("unsupported unversioned boundary storage; refusing to convert existing events")
	}
	if _, err := tx.ExecContext(ctx, "CREATE TABLE "+versions+" (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)"); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO "+versions+" VALUES (1, $1)", boundarySchemaVersion)
	return err
}
