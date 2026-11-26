package postgres

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/lib/pq"
)

const boundarySchemaVersion = 6

// Upgrade the supported preceding schemas in the initialization transaction.
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
		if err := tx.QueryRowContext(ctx, "SELECT version FROM "+versions+" WHERE id = 1 FOR UPDATE").Scan(&version); err != nil {
			return err
		}
		if version == 4 || version == 5 {
			if version == 4 {
				// 0.13.0 also kept publisher state that is now retired.
				if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+table("_orisun_last_published_event_position")); err != nil {
					return err
				}
			}
			if err := splitEventEnvelope(ctx, tx, schema, boundary); err != nil {
				return fmt.Errorf("upgrade event envelope: %w", err)
			}
			_, err := tx.ExecContext(ctx, "UPDATE "+versions+" SET version = $1 WHERE id = 1", boundarySchemaVersion)
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

// Envelope keys held in version 4 and 5 documents.
var legacyEnvelopeKeys = []string{"__eventId", "__eventType", "__commitPosition", "__preparePosition", "__writeId", "__dateCreated", "__metadata"}

// splitEventEnvelope rewrites version 4/5 documents into envelope columns plus
// application data. It validates before changing anything, copies events in
// position order into a fresh table, and rebuilds indexes after the copy.
// Write contexts, the sequence, and checkpoints are untouched.
func splitEventEnvelope(ctx context.Context, tx *sql.Tx, schema, boundary string) error {
	qualify := func(name string) string { return pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(name) }
	eventName := boundary + "_orisun_es_event"
	event := qualify(eventName)
	sequence := qualify(boundary + "_orisun_es_event_global_id_seq")
	digest := md5.Sum([]byte(boundary))
	previousName := "orisun_upgrade_" + hex.EncodeToString(digest[:])
	previous := qualify(previousName)

	if _, err := tx.ExecContext(ctx, "LOCK TABLE "+event+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		return err
	}
	if err := validateLegacyDocuments(ctx, tx, event); err != nil {
		return err
	}
	managed, replay, err := legacyEventIndexes(ctx, tx, schema, boundary, event)
	if err != nil {
		return err
	}

	// Free the table and index names, keeping the sequence and its value.
	for _, statement := range []string{
		"ALTER SEQUENCE " + sequence + " OWNED BY NONE",
		"ALTER TABLE " + event + " RENAME TO " + pq.QuoteIdentifier(previousName),
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := dropTableIndexes(ctx, tx, schema, previous); err != nil {
		return err
	}
	// Create the current table from its single definition, then build its
	// secondary indexes after the copy instead of maintaining them per row.
	if _, err := tx.ExecContext(ctx, "SELECT initialize_boundary_tables($1, $2)", boundary, schema); err != nil {
		return err
	}
	for _, suffix := range []string{"_idx_event_type_order", "_idx_event_order_visibility_covering"} {
		if _, err := tx.ExecContext(ctx, "DROP INDEX "+qualify(boundary+suffix)); err != nil {
			return err
		}
	}
	envelopeKeys := "ARRAY['" + strings.Join(legacyEnvelopeKeys, "','") + "']"
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+event+`
 (global_id, transaction_id, write_id, pg_xact_id, event_id, date_created, event_type, metadata, data)
SELECT global_id, transaction_id, write_id, pg_xact_id, (data->>'__eventId')::UUID, date_created,
       data->>'__eventType', metadata, data - `+envelopeKeys+`
FROM `+previous+` ORDER BY global_id`); err != nil {
		return err
	}
	for _, statement := range []string{
		"DROP TABLE " + previous,
		"ALTER SEQUENCE " + sequence + " OWNED BY " + event + ".global_id",
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	// Managed indexes are regenerated: envelope keys now resolve to columns.
	for _, index := range managed {
		expressions, predicate, err := boundaryIndexExpressions(index.fields, index.conditions, index.combinator)
		if err != nil {
			return fmt.Errorf("index %q: %w", index.name, err)
		}
		if _, err := tx.ExecContext(ctx, "CREATE INDEX "+pq.QuoteIdentifier(boundary+"_"+index.name+"_idx")+" ON "+event+" USING btree ("+expressions+")"+predicate); err != nil {
			return fmt.Errorf("index %q: %w", index.name, err)
		}
	}
	for _, definition := range replay {
		if _, err := tx.ExecContext(ctx, definition); err != nil {
			return fmt.Errorf("replay index %q: %w", definition, err)
		}
	}
	_, err = tx.ExecContext(ctx, "ANALYZE "+event)
	return err
}

// validateLegacyDocuments refuses documents the split cannot represent
// exactly, before any change is made.
func validateLegacyDocuments(ctx context.Context, tx *sql.Tx, event string) error {
	var globalID int64
	var detail string
	err := tx.QueryRowContext(ctx, `SELECT global_id, key FROM `+event+`, jsonb_object_keys(data) AS key
WHERE left(key, 2) = '__' AND key <> ALL($1::TEXT[]) LIMIT 1`, pq.Array(legacyEnvelopeKeys)).Scan(&globalID, &detail)
	if err == nil {
		return fmt.Errorf("event %d holds unknown reserved field %q; refusing to convert events", globalID, detail)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT global_id FROM `+event+`
WHERE jsonb_typeof(data->'__eventType') IS DISTINCT FROM 'string'
   OR jsonb_typeof(data->'__eventId') IS DISTINCT FROM 'string'
   OR data->>'__eventId' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
LIMIT 1`).Scan(&globalID)
	if err == nil {
		return fmt.Errorf("event %d lacks a canonical event ID or type; refusing to convert events", globalID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// Positions and the write ID are rendered from columns after the split, so
	// they must already agree with the generated columns they came from.
	err = tx.QueryRowContext(ctx, `SELECT global_id FROM `+event+`
WHERE data->>'__writeId' IS DISTINCT FROM (transaction_id::TEXT || ':' || write_id::TEXT)
   OR jsonb_typeof(data->'__commitPosition') IS DISTINCT FROM 'number'
   OR jsonb_typeof(data->'__preparePosition') IS DISTINCT FROM 'number'
LIMIT 1`).Scan(&globalID)
	if err == nil {
		return fmt.Errorf("event %d holds a write ID or position the envelope columns cannot represent; refusing to convert events", globalID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

type legacyManagedIndex struct {
	name       string
	fields     []eventstore.BoundaryIndexField
	conditions []eventstore.BoundaryIndexCondition
	combinator string
}

// legacyEventIndexes returns the managed indexes physically present on the
// event table and the definitions of any other secondary index. Built-in
// indexes are recreated by initialization and are excluded.
func legacyEventIndexes(ctx context.Context, tx *sql.Tx, schema, boundary, event string) ([]legacyManagedIndex, []string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT c.relname, pg_get_indexdef(i.indexrelid)
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
WHERE i.indrelid = $1::regclass AND NOT i.indisprimary
ORDER BY c.relname`, event)
	if err != nil {
		return nil, nil, err
	}
	physical := map[string]string{}
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			rows.Close()
			return nil, nil, err
		}
		physical[name] = definition
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	for _, suffix := range []string{"_idx_event_type_order", "_idx_event_order_visibility_covering", "_idx_global_order_covering"} {
		delete(physical, boundary+suffix)
	}

	metadata := pq.QuoteIdentifier(schema) + "." + pq.QuoteIdentifier(boundary+"_orisun_boundary_index_metadata")
	rows, err = tx.QueryContext(ctx, "SELECT name, fields, conditions, combinator FROM "+metadata+" ORDER BY name")
	if err != nil {
		return nil, nil, err
	}
	var managed []legacyManagedIndex
	for rows.Next() {
		var index legacyManagedIndex
		var fields, conditions []byte
		if err := rows.Scan(&index.name, &fields, &conditions, &index.combinator); err != nil {
			rows.Close()
			return nil, nil, err
		}
		physicalName := boundary + "_" + index.name + "_idx"
		if _, ok := physical[physicalName]; !ok {
			continue // Never built, or dropped; its metadata state is unchanged.
		}
		delete(physical, physicalName)
		if err := json.Unmarshal(fields, &index.fields); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("index %q fields: %w", index.name, err)
		}
		if err := json.Unmarshal(conditions, &index.conditions); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("index %q conditions: %w", index.name, err)
		}
		managed = append(managed, index)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}

	replay := make([]string, 0, len(physical))
	for name, definition := range physical {
		// An unmanaged index on reserved document keys would silently index
		// NULLs after the split. Refuse rather than guess its intent.
		if strings.Contains(definition, "'__") {
			return nil, nil, fmt.Errorf("index %q references reserved event fields and is not managed by Orisun; drop it before upgrading", name)
		}
		replay = append(replay, definition)
	}
	return managed, replay, nil
}

// dropTableIndexes frees every index name held by a table about to be dropped.
func dropTableIndexes(ctx context.Context, tx *sql.Tx, schema, table string) error {
	rows, err := tx.QueryContext(ctx, `SELECT c.relname, con.conname
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid AND con.conrelid = i.indrelid
WHERE i.indrelid = $1::regclass`, table)
	if err != nil {
		return err
	}
	var statements []string
	for rows.Next() {
		var index string
		var constraint sql.NullString
		if err := rows.Scan(&index, &constraint); err != nil {
			rows.Close()
			return err
		}
		if constraint.Valid {
			statements = append(statements, "ALTER TABLE "+table+" DROP CONSTRAINT "+pq.QuoteIdentifier(constraint.String))
		} else {
			statements = append(statements, "DROP INDEX "+pq.QuoteIdentifier(schema)+"."+pq.QuoteIdentifier(index))
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
