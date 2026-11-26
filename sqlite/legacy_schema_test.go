package sqlite

// legacyEventDDL is the v0.14.3 document schema (event version 8). Upgrade tests
// create it directly; production code must not depend on this layout.
const legacyEventDDL = `
CREATE TABLE orisun_es_write (
 write_id INTEGER PRIMARY KEY,
 consistency TEXT NOT NULL CHECK (json_valid(consistency) AND json_type(consistency) = 'array')
);
CREATE TABLE orisun_es_event (
 data TEXT NOT NULL CHECK (json_valid(data) AND json_type(data) = 'object'),
 transaction_id INTEGER GENERATED ALWAYS AS (json_extract(data, '$.__commitPosition')) VIRTUAL NOT NULL,
 global_id INTEGER GENERATED ALWAYS AS (json_extract(data, '$.__preparePosition')) VIRTUAL NOT NULL UNIQUE,
 write_id INTEGER GENERATED ALWAYS AS (CAST(substr(json_extract(data, '$.__writeId'), instr(json_extract(data, '$.__writeId'), ':') + 1) AS INTEGER)) VIRTUAL REFERENCES orisun_es_write(write_id),
 metadata TEXT GENERATED ALWAYS AS (data -> '__metadata') VIRTUAL,
 date_created TEXT GENERATED ALWAYS AS (json_extract(data, '$.__dateCreated')) VIRTUAL NOT NULL
);
CREATE INDEX idx_global_order_covering ON orisun_es_event(transaction_id DESC, global_id DESC);
CREATE INDEX idx_event_type_order ON orisun_es_event(CASE json_type(data, '$."__eventType"') WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' ELSE CAST(json_extract(data, '$."__eventType"') AS TEXT) END, transaction_id DESC, global_id DESC);
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
