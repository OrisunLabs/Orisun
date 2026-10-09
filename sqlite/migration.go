package sqlite

import (
	"fmt"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Event per-boundary tables: event log, id sequence counter, and index metadata.
const eventSchemaVersion = 7

// Fresh databases use the current document schema directly.
const eventDDL = `
CREATE TABLE orisun_es_write (
 write_id INTEGER PRIMARY KEY,
 consistency TEXT NOT NULL CHECK (json_valid(consistency) AND json_type(consistency) = 'array')
);
CREATE TABLE orisun_es_event (
 data TEXT NOT NULL CHECK (json_valid(data) AND json_type(data) = 'object'),
 transaction_id INTEGER GENERATED ALWAYS AS (json_extract(data, '$.__commitPosition')) VIRTUAL NOT NULL,
 global_id INTEGER GENERATED ALWAYS AS (json_extract(data, '$.__preparePosition')) VIRTUAL NOT NULL UNIQUE,
 write_id INTEGER GENERATED ALWAYS AS (CAST(substr(json_extract(data, '$.__writeId'), instr(json_extract(data, '$.__writeId'), ':') + 1) AS INTEGER)) VIRTUAL NOT NULL REFERENCES orisun_es_write(write_id),
 metadata TEXT GENERATED ALWAYS AS (data -> '__metadata') VIRTUAL,
 date_created TEXT GENERATED ALWAYS AS (json_extract(data, '$.__dateCreated')) VIRTUAL NOT NULL
);
CREATE INDEX idx_global_order_covering ON orisun_es_event(transaction_id DESC, global_id DESC);
CREATE INDEX idx_event_type_order ON orisun_es_event(json_extract(data, '$.__eventType'), transaction_id DESC, global_id DESC);
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

// Metadata tables are stored in a separate SQLite file so projector/admin
// writes do not contend with the per-boundary event-log writer.
const metadataDDL = `
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

func applyMigrations(conn *sqlite.Conn) error {
	return initializeSchema(conn, eventDDL, eventSchemaVersion)
}

func applyMetadataMigrations(conn *sqlite.Conn) error {
	return initializeSchema(conn, metadataDDL, 1)
}

// Existing files must already use the current schema. Never rewrite historical data.
func initializeSchema(conn *sqlite.Conn, ddl string, supported int) (err error) {
	release := sqlitex.Save(conn)
	defer release(&err)
	version, err := schemaVersion(conn)
	if err != nil {
		return err
	}
	if version == supported {
		return nil
	}
	if version != 0 {
		return fmt.Errorf("unsupported database schema version %d; required version %d", version, supported)
	}
	var occupied bool
	if err := sqlitex.ExecuteTransient(conn, "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%')", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { occupied = stmt.ColumnBool(0); return nil }}); err != nil {
		return err
	}
	if occupied {
		return fmt.Errorf("unsupported unversioned database; refusing to convert existing storage")
	}
	if err := sqlitex.ExecuteScript(conn, ddl, nil); err != nil {
		return err
	}
	return sqlitex.ExecuteTransient(conn, fmt.Sprintf("PRAGMA user_version = %d", supported), nil)
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
