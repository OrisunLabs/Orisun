package sqlite

import (
	"fmt"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Event per-boundary tables: event log, id sequence counter, and index metadata.
const eventDDL = `
CREATE TABLE IF NOT EXISTS orisun_es_event (
    transaction_id INTEGER NOT NULL,
    global_id      INTEGER PRIMARY KEY,
    event_id       TEXT    NOT NULL,
    data           TEXT    NOT NULL CHECK (json_valid(data)),
    metadata       TEXT,
    date_created   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_global_order_covering
    ON orisun_es_event (transaction_id DESC, global_id DESC);

CREATE INDEX IF NOT EXISTS idx_event_type_order
    ON orisun_es_event (
        CASE json_type(data, '$."eventType"')
            WHEN 'true' THEN 'true'
            WHEN 'false' THEN 'false'
            ELSE CAST(json_extract(data, '$."eventType"') AS TEXT)
        END,
        transaction_id DESC,
        global_id DESC
    );

CREATE TABLE IF NOT EXISTS orisun_es_seq (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    next_id INTEGER NOT NULL DEFAULT 1
);
INSERT OR IGNORE INTO orisun_es_seq (id, next_id) VALUES (1, 1);

CREATE TABLE IF NOT EXISTS orisun_boundary_index_metadata (
    name         TEXT PRIMARY KEY,
    fields       TEXT NOT NULL CHECK (json_valid(fields)),
    conditions   TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(conditions)),
    combinator   TEXT NOT NULL DEFAULT 'AND',
    date_created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    date_updated TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
`

// Metadata tables are stored in a separate SQLite file so publisher/projector/admin
// writes do not contend with the per-boundary event-log writer.
const metadataDDL = `
CREATE TABLE IF NOT EXISTS orisun_last_published_event_position (
    boundary       TEXT    PRIMARY KEY,
    transaction_id INTEGER NOT NULL DEFAULT 0,
    global_id      INTEGER NOT NULL DEFAULT 0,
    date_created   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    date_updated   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS events_count (
    boundary    TEXT PRIMARY KEY,
    event_count INTEGER NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS projector_checkpoint (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    commit_position  INTEGER NOT NULL,
    prepare_position INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    username      TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    roles         TEXT NOT NULL,
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS users_count (
    id         TEXT PRIMARY KEY,
    user_count INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
`

// Migration steps for each schema line, applied in order. Step N brings a
// database at PRAGMA user_version N-1 to version N; the runner stamps the
// version inside the same savepoint as the step's script, so a database is
// never left between versions. Never edit or reorder a shipped step — append
// a new one. Databases created before versioning report user_version 0 and
// re-run step 1, which is safe because the baseline DDL is idempotent
// (IF NOT EXISTS everywhere); they come out stamped at the current version.
var eventMigrations = []string{eventDDL}

var metadataMigrations = []string{metadataDDL}

func applyMigrations(conn *sqlite.Conn) error {
	return applyVersionedMigrations(conn, eventMigrations)
}

func applyMetadataMigrations(conn *sqlite.Conn) error {
	return applyVersionedMigrations(conn, metadataMigrations)
}

func applyVersionedMigrations(conn *sqlite.Conn, migrations []string) error {
	version, err := schemaVersion(conn)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version > len(migrations) {
		return fmt.Errorf(
			"database schema version %d is newer than this binary supports (max %d); refusing to open",
			version, len(migrations),
		)
	}
	for i := version; i < len(migrations); i++ {
		if err := applyMigrationStep(conn, migrations[i], i+1); err != nil {
			return fmt.Errorf("migration step %d: %w", i+1, err)
		}
	}
	return nil
}

func schemaVersion(conn *sqlite.Conn) (int, error) {
	var version int
	err := sqlitex.ExecuteTransient(conn, "PRAGMA user_version", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			version = int(stmt.ColumnInt64(0))
			return nil
		},
	})
	return version, err
}

func applyMigrationStep(conn *sqlite.Conn, script string, version int) (err error) {
	releaseFn := sqlitex.Save(conn)
	defer releaseFn(&err)
	if err := sqlitex.ExecuteScript(conn, script, nil); err != nil {
		return err
	}
	// user_version lives in the database header and is transactional, so the
	// bump commits or rolls back together with the step's schema changes.
	return sqlitex.ExecuteTransient(conn, fmt.Sprintf("PRAGMA user_version = %d", version), nil)
}
