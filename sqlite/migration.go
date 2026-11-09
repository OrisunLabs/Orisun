package sqlite

import (
	"fmt"
	"strings"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Event per-boundary tables: event log, id sequence counter, and index metadata.
const eventSchemaVersion = 9
const metadataSchemaVersion = 2

// eventTableDDL keeps the envelope in typed columns and application JSON in
// data. global_id aliases the rowid, so the log is clustered by position.
func eventTableDDL(name string) string {
	return `CREATE TABLE ` + name + ` (
 global_id INTEGER PRIMARY KEY,
 transaction_id INTEGER NOT NULL,
 write_id INTEGER REFERENCES orisun_es_write(write_id),
 event_id TEXT NOT NULL,
 event_type TEXT NOT NULL,
 date_created TEXT NOT NULL,
 metadata TEXT CHECK (metadata IS NULL OR json_valid(metadata)),
 data TEXT NOT NULL CHECK (json_valid(data) AND json_type(data) = 'object')
);
`
}

// The rowid is the index's implicit last column: scanned either way, this
// yields (transaction_id, global_id) order in both directions.
const eventPositionIndexDDL = "CREATE INDEX idx_event_position ON orisun_es_event(transaction_id);"

var eventIndexesDDL = eventPositionIndexDDL + eventTypeOrderIndexDDL

var eventDDL = `
CREATE TABLE orisun_es_write (
 write_id INTEGER PRIMARY KEY,
 consistency TEXT NOT NULL CHECK (json_valid(consistency) AND json_type(consistency) = 'array')
);
` + eventTableDDL("orisun_es_event") + eventIndexesDDL + `
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

// The built-in index must use the same scalar representation as reads and CCC.
// SQLite expression indexes require structural equality with the query expression.
var eventTypeOrderIndexDDL = "CREATE INDEX idx_event_type_order ON orisun_es_event(" + sqliteJSONScalarTextExpr("__eventType") + ", transaction_id DESC, global_id DESC);"

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

func applyMigrations(conn *sqlite.Conn) (err error) {
	version, err := schemaVersion(conn)
	if err != nil {
		return err
	}
	if envelopeSplitPending(version) {
		restore, err := spillTempStoreToFile(conn)
		if err != nil {
			return err
		}
		defer func() {
			if restoreErr := restore(); err == nil {
				err = restoreErr
			}
		}()
	}
	return initializeSchema(conn, eventDDL, eventSchemaVersion)
}

func envelopeSplitPending(version int) bool {
	return version >= 6 && version <= 8
}

// spillTempStoreToFile keeps the envelope split's index-build sorts on disk.
// Under temp_store=MEMORY they grow with the event count (about 1.2 GB at 5M events).
// Changing temp_store discards the connection's temporary database, so it is
// switched before the upgrade savepoint opens and restored after it closes.
func spillTempStoreToFile(conn *sqlite.Conn) (func() error, error) {
	var previous int64
	if err := sqlitex.ExecuteTransient(conn, "PRAGMA temp_store", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		previous = stmt.ColumnInt64(0)
		return nil
	}}); err != nil {
		return nil, err
	}
	if err := sqlitex.ExecuteTransient(conn, "PRAGMA temp_store = FILE", nil); err != nil {
		return nil, err
	}
	return func() error {
		return sqlitex.ExecuteTransient(conn, fmt.Sprintf("PRAGMA temp_store = %d", previous), nil)
	}, nil
}

func applyMetadataMigrations(conn *sqlite.Conn) error {
	return initializeSchema(conn, metadataDDL, metadataSchemaVersion)
}

// Upgrade supported storage schemas in one savepoint, so failure keeps the old format.
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
	if supported == eventSchemaVersion && envelopeSplitPending(version) {
		if err := splitEventEnvelope(conn); err != nil {
			return fmt.Errorf("upgrade event storage: %w", err)
		}
		return sqlitex.ExecuteTransient(conn, fmt.Sprintf("PRAGMA user_version = %d", supported), nil)
	}
	if supported == metadataSchemaVersion && version == 1 {
		if err := sqlitex.ExecuteTransient(conn, "DROP TABLE IF EXISTS orisun_last_published_event_position", nil); err != nil {
			return err
		}
		return sqlitex.ExecuteTransient(conn, fmt.Sprintf("PRAGMA user_version = %d", supported), nil)
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

// splitEventEnvelope rewrites canonical documents into envelope columns plus
// application data. Application bytes, positions, and write contexts survive;
// the table swap rebuilds every index because each one depends on the old table.
func splitEventEnvelope(conn *sqlite.Conn) error {
	indexes, err := eventIndexesToRestore(conn)
	if err != nil {
		return err
	}
	// __writeId is derived from positions after the split, so it must already agree.
	if err := requireNoRows(conn, "write IDs that differ from their positions",
		"SELECT COUNT(*) FROM orisun_es_event WHERE json_extract(data, '$.__writeId') IS NOT (transaction_id || ':' || write_id)"); err != nil {
		return err
	}
	const temporary = "orisun_es_event_v9"
	if err := sqlitex.ExecuteScript(conn, eventTableDDL(temporary)+`
INSERT INTO `+temporary+` (global_id, transaction_id, write_id, event_id, event_type, date_created, metadata, data)
SELECT global_id, transaction_id, write_id,
       json_extract(data, '$.__eventId'), json_extract(data, '$.__eventType'), date_created, metadata,
       json_remove(data, '$.__eventId', '$.__eventType', '$.__commitPosition', '$.__preparePosition', '$.__writeId', '$.__dateCreated', '$.__metadata')
FROM orisun_es_event ORDER BY global_id;
`, nil); err != nil {
		return err
	}
	// Reads used to strip every reserved key. Refuse to expose unknown ones.
	if err := requireNoRows(conn, "unknown reserved fields in event data",
		"SELECT COUNT(*) FROM "+temporary+", json_each("+temporary+".data) WHERE substr(json_each.key, 1, 2) = '__'"); err != nil {
		return err
	}
	return sqlitex.ExecuteScript(conn, "DROP TABLE orisun_es_event; ALTER TABLE "+temporary+" RENAME TO orisun_es_event;"+eventIndexesDDL+strings.Join(indexes, ";"), nil)
}

// eventIndexesToRestore rebuilds managed indexes from their definitions, because
// envelope keys now render differently, and replays any other index verbatim.
func eventIndexesToRestore(conn *sqlite.Conn) ([]string, error) {
	var indexes []string
	managed := map[string]bool{"idx_global_order_covering": true, "idx_event_type_order": true}
	err := sqlitex.ExecuteTransient(conn, "SELECT name, fields, conditions, combinator FROM orisun_boundary_index_metadata", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		index, err := sqliteBoundaryIndexFromRow(stmt.ColumnText(0), stmt.ColumnText(1), stmt.ColumnText(2), stmt.ColumnText(3))
		if err != nil {
			return err
		}
		ddl, _, err := buildSQLiteBoundaryIndexDDL(index.Name, index.Fields, index.Conditions, index.Combinator)
		if err != nil {
			return err
		}
		managed[index.Name+"_idx"] = true
		indexes = append(indexes, ddl)
		return nil
	}})
	if err != nil {
		return nil, err
	}
	err = sqlitex.ExecuteTransient(conn, "SELECT name, sql FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'orisun_es_event' AND sql IS NOT NULL", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		if !managed[stmt.ColumnText(0)] {
			indexes = append(indexes, stmt.ColumnText(1))
		}
		return nil
	}})
	return indexes, err
}

func requireNoRows(conn *sqlite.Conn, problem, countQuery string) error {
	var count int64
	if err := sqlitex.ExecuteTransient(conn, countQuery, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		count = stmt.ColumnInt64(0)
		return nil
	}}); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("found %d %s", count, problem)
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
