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
    event_count TEXT NOT NULL,
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
	if err := sqlitex.ExecuteScript(conn, eventDDL, nil); err != nil {
		return fmt.Errorf("event ddl: %w", err)
	}
	return nil
}

func applyMetadataMigrations(conn *sqlite.Conn) error {
	if err := sqlitex.ExecuteScript(conn, metadataDDL, nil); err != nil {
		return fmt.Errorf("metadata ddl: %w", err)
	}
	return nil
}
