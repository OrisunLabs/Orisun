-- Initialize Boundary Tables Function
--
-- Creates or maintains the YugabyteDB objects used by one logical boundary in a
-- caller-supplied schema. Boundaries are mapped to schemas by Go configuration;
-- the boundary name is used only as the table/sequence prefix inside that schema.
--
-- Parameters:
--   boundary_name (TEXT): Boundary/table prefix, validated as a PostgreSQL identifier
--   schema_name (TEXT): YugabyteDB schema where the boundary objects live
--
-- Creates or maintains:
--   <boundary>_orisun_es_event
--   <boundary>_orisun_es_event_global_id_seq
--   <boundary>_orisun_last_published_event_position
--   <boundary>_orisun_committed_position
--   <boundary>_events_count
--   <boundary>_projector_checkpoint
--
-- transaction_id is the logical commit position used by clients, projectors,
-- and publishing checkpoints. YugabyteDB does not expose PostgreSQL xid
-- semantics, so pg_xact_id is kept only for storage-shape compatibility and the
-- committed watermark is used for stable-prefix reads.

CREATE OR REPLACE FUNCTION initialize_boundary_tables(
    boundary_name TEXT,
    schema_name TEXT
) RETURNS VOID AS
$$
DECLARE
    prefixed_seq_name TEXT;
BEGIN
    -- Validate boundary_name as a simple PostgreSQL identifier.
    -- - Must start with letter or underscore
    -- - Can contain letters, digits, underscores
    -- - Max length 63 characters
    IF boundary_name ~ '^[^a-zA-Z_]' OR boundary_name ~ '[^a-zA-Z0-9_]' OR length(boundary_name) > 63 THEN
        RAISE EXCEPTION 'Invalid boundary name: %. Must start with letter or underscore, contain only letters/digits/underscores, and be 63 chars or less', boundary_name;
    END IF;

    prefixed_seq_name := format('%I.%I', schema_name, boundary_name || '_orisun_es_event_global_id_seq');

    -- Create the durable event table for this boundary.
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        transaction_id BIGINT NOT NULL,
        pg_xact_id     BIGINT,
        global_id      BIGINT PRIMARY KEY,
        event_id       UUID NOT NULL,
        data           JSONB NOT NULL,
        metadata       JSONB,
        date_created   TIMESTAMPTZ DEFAULT (NOW() AT TIME ZONE ''UTC'') NOT NULL
    )', schema_name, boundary_name || '_orisun_es_event');

    -- Create the boundary-local global_id sequence.
    EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %I.%I
        START WITH 0
        MINVALUE 0
        OWNED BY %I.%I.%I',
                   schema_name, boundary_name || '_orisun_es_event_global_id_seq',
                   schema_name, boundary_name || '_orisun_es_event', 'global_id');

    -- YugabyteDB does not provide PostgreSQL xid semantics. Keep pg_xact_id
    -- nullable for table-shape compatibility, but do not use stored values.
    EXECUTE format('
        UPDATE %I.%I
        SET pg_xact_id = NULL
        WHERE pg_xact_id IS NOT NULL',
                   schema_name, boundary_name || '_orisun_es_event');

    EXECUTE format('SELECT setval(%L::regclass, (SELECT COALESCE(MAX(global_id) + 1, 0) FROM %I.%I), false)',
                   prefixed_seq_name,
                   schema_name,
                   boundary_name || '_orisun_es_event');

    -- Older releases included the unbounded JSONB data and metadata columns in
    -- these B-tree indexes. Drop only those legacy managed definitions so event
    -- payload size cannot determine whether a write succeeds.
    IF EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = schema_name
          AND tablename = boundary_name || '_orisun_es_event'
          AND indexname = boundary_name || '_idx_global_order_covering'
          AND indexdef ILIKE '%INCLUDE%'
          AND indexdef ILIKE '%data%'
    ) THEN
        EXECUTE format('DROP INDEX %I.%I',
                       schema_name, boundary_name || '_idx_global_order_covering');
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = schema_name
          AND tablename = boundary_name || '_orisun_es_event'
          AND indexname = boundary_name || '_idx_event_order_visibility_covering'
          AND indexdef ILIKE '%INCLUDE%'
          AND indexdef ILIKE '%data%'
    ) THEN
        EXECUTE format('DROP INDEX %I.%I',
                       schema_name, boundary_name || '_idx_event_order_visibility_covering');
    END IF;

    -- Create indexes used by latest-position checks and ordered event reads.
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I (transaction_id DESC, global_id DESC)',
                   boundary_name || '_idx_global_order_covering', schema_name, boundary_name || '_orisun_es_event');
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I ((data->>''eventType''), transaction_id DESC, global_id DESC)',
                   boundary_name || '_idx_event_type_order', schema_name, boundary_name || '_orisun_es_event');
    EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON %I.%I (transaction_id DESC, global_id DESC) INCLUDE (pg_xact_id)',
            boundary_name || '_idx_event_order_visibility_covering', schema_name, boundary_name || '_orisun_es_event');

    -- Persist definitions for indexes created through Orisun's index API.
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        name         TEXT PRIMARY KEY,
        fields       JSONB NOT NULL,
        conditions   JSONB NOT NULL DEFAULT ''[]''::JSONB,
        combinator   TEXT NOT NULL DEFAULT ''AND'',
        state        TEXT NOT NULL DEFAULT ''ready'',
        date_created TIMESTAMPTZ DEFAULT NOW() NOT NULL,
        date_updated TIMESTAMPTZ DEFAULT NOW() NOT NULL
    )', schema_name, boundary_name || '_orisun_boundary_index_metadata');

    -- Create the per-boundary NATS publisher checkpoint table.
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        boundary       TEXT PRIMARY KEY,
        transaction_id BIGINT NOT NULL DEFAULT 0,
        global_id      BIGINT NOT NULL DEFAULT 0,
        date_created   TIMESTAMPTZ DEFAULT NOW() NOT NULL,
        date_updated   TIMESTAMPTZ DEFAULT NOW() NOT NULL
    )', schema_name, boundary_name || '_orisun_last_published_event_position');

    -- Stable committed-prefix watermark for YugabyteDB ASC reads. Writers hold
    -- the per-boundary position lock until commit, and update this row in the
    -- same transaction as event inserts.
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        boundary       TEXT PRIMARY KEY,
        transaction_id BIGINT NOT NULL DEFAULT -1,
        global_id      BIGINT NOT NULL DEFAULT -1,
        date_created   TIMESTAMPTZ DEFAULT NOW() NOT NULL,
        date_updated   TIMESTAMPTZ DEFAULT NOW() NOT NULL
    )', schema_name, boundary_name || '_orisun_committed_position');

    EXECUTE format('
        INSERT INTO %I.%I (boundary, transaction_id, global_id, date_created, date_updated)
        SELECT %L,
               COALESCE(latest.transaction_id, -1),
               COALESCE(latest.global_id, -1),
               NOW(),
               NOW()
        FROM (SELECT 1) seed
        LEFT JOIN LATERAL (
            SELECT transaction_id, global_id
            FROM %I.%I
            ORDER BY transaction_id DESC, global_id DESC
            LIMIT 1
        ) latest ON TRUE
        ON CONFLICT (boundary)
        DO UPDATE SET transaction_id = EXCLUDED.transaction_id,
                      global_id = EXCLUDED.global_id,
                      date_updated = NOW()',
                   schema_name, boundary_name || '_orisun_committed_position',
                   boundary_name,
                   schema_name, boundary_name || '_orisun_es_event');

    -- Create the admin event-count cache table.
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        id          VARCHAR(255) PRIMARY KEY,
        event_count BIGINT NOT NULL,
        created_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )', schema_name, boundary_name || '_events_count');

    -- Legacy tables stored the count as VARCHAR; convert in place (one-row cache).
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = schema_name
          AND table_name = boundary_name || '_events_count'
          AND column_name = 'event_count'
          AND data_type = 'character varying'
    ) THEN
        EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN event_count TYPE BIGINT USING event_count::BIGINT',
                       schema_name, boundary_name || '_events_count');
    END IF;

    -- Create the admin/projector checkpoint table.
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        id               VARCHAR(255) PRIMARY KEY,
        name             VARCHAR(255) UNIQUE NOT NULL,
        commit_position  BIGINT NOT NULL,
        prepare_position BIGINT NOT NULL
    )', schema_name, boundary_name || '_projector_checkpoint');

END;
$$ LANGUAGE plpgsql;


-- Insert Events with Consistency Function
--
-- Inserts one non-empty event batch into a boundary event table and enforces
-- Command Context Consistency for the supplied content query. The Go saver sends
-- query as:
--   {
--     "expected_position": {"transaction_id": <commit>, "global_id": <prepare>},
--     "criteria": [{"tag": "value", ...}, ...]
--   }
--
-- Each criterion object is an AND of its tags; the criteria array is ORed. When
-- criteria are present, this function locks each criterion object, finds the
-- latest event matching the content query, and compares it with expected_position.
-- A missing expected_position or missing match is treated as (-1, -1).
--
-- The inserted batch receives consecutive global_id values. Its logical
-- transaction_id is MAX(global_id) + 1 for the batch, which keeps Orisun
-- positions durable and independent of PostgreSQL XID reuse.
--
-- Returns:
--   new_global_id: the highest global_id inserted
--   latest_transaction_id/latest_global_id: the resulting position to return to callers

CREATE OR REPLACE FUNCTION insert_events_with_consistency_v3(
    boundary_name TEXT,
    schema TEXT,
    query JSONB,
    events JSONB
)
    RETURNS TABLE
            (
                new_global_id         BIGINT,
                latest_transaction_id BIGINT,
                latest_global_id      BIGINT
            )
    LANGUAGE plpgsql
AS
$$
DECLARE
    criteria              JSONB;
    expected_tx_id        BIGINT;
    expected_gid          BIGINT;
    latest_tx_id          BIGINT;
    latest_gid            BIGINT;
    key_record            TEXT;
    criteria_tags         TEXT[];
    new_global_id         BIGINT;
    latest_transaction_id BIGINT;
    latest_global_id      BIGINT;
    prefixed_seq_name     TEXT;
    criteria_sql          TEXT;
    crit                  JSONB;
    crit_parts            TEXT[];
    all_parts             TEXT[];
    k                     TEXT;
    v                     TEXT;
BEGIN
    criteria := query -> 'criteria';
    expected_tx_id := (query -> 'expected_position' ->> 'transaction_id')::BIGINT;
    expected_gid := (query -> 'expected_position' ->> 'global_id')::BIGINT;
    -- Build a schema-qualified sequence reference for nextval. This avoids
    -- depending on search_path, including under pgbouncer transaction pooling.
    prefixed_seq_name := format('%I.%I', schema, boundary_name || '_orisun_es_event_global_id_seq');

    IF jsonb_array_length(events) = 0 THEN
        RAISE EXCEPTION 'Events array cannot be empty';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(events) AS evt
        WHERE COALESCE(evt ->> 'event_type', '') = ''
    ) THEN
        RAISE EXCEPTION 'event_type cannot be empty';
    END IF;

    -- If criteria are present, acquire granular locks for each criterion object.
    -- Each criterion object is locked as a unit (not individual fields within it).
    IF criteria IS NOT NULL THEN
        -- Extract all unique criteria. Each criterion object maps to one lock.
        SELECT ARRAY_AGG(DISTINCT criterion::text)
        INTO criteria_tags
        FROM jsonb_array_elements(criteria) AS criterion;

        -- Lock criterion objects in deterministic order for deadlock prevention.
        IF criteria_tags IS NOT NULL THEN
            criteria_tags := ARRAY(
                    SELECT DISTINCT unnest(criteria_tags)
                    ORDER BY 1 -- Alphabetical sort to ensure consistent lock order and deadlock prevention.
                             );

            FOREACH key_record IN ARRAY criteria_tags
                LOOP
                    PERFORM pg_advisory_xact_lock(hashtext(key_record));
                END LOOP;
        END IF;
    END IF;

    -- Always take the boundary position lock before evaluating the content
    -- query. Taking it after the read allowed a writer with a different (but
    -- overlapping) criterion lock to invalidate this request while it waited
    -- for the position lock. Criterion locks remain first in the hierarchy,
    -- and the position lock is always last, so the ordering is deadlock-free.
    PERFORM pg_advisory_xact_lock(hashtext(schema || '.' || boundary_name || '::position_draw'));

    IF criteria IS NOT NULL THEN
        -- Build the content query as an OR of criteria, where each criterion is
        -- an AND of tag equality checks.
        all_parts := '{}';
        FOR crit IN SELECT jsonb_array_elements(criteria)
            LOOP
                crit_parts := '{}';
                FOR k, v IN SELECT * FROM jsonb_each_text(crit)
                    LOOP
                        crit_parts := crit_parts || format('(data->>%L = %L)', k, v);
                    END LOOP;
                IF array_length(crit_parts, 1) > 0 THEN
                    all_parts := all_parts || ('(' || array_to_string(crit_parts, ' AND ') || ')');
                END IF;
            END LOOP;
        criteria_sql := CASE
                            WHEN array_length(all_parts, 1) > 0
                                THEN '(' || array_to_string(all_parts, ' OR ') || ')'
                            ELSE 'TRUE'
            END;

        -- Version check: read the latest event matching this content query from
        -- the schema-qualified boundary table.
        EXECUTE format('
            SELECT DISTINCT oe.transaction_id, oe.global_id
            FROM %I.%I oe
            WHERE %s
            ORDER BY oe.transaction_id DESC, oe.global_id DESC
            LIMIT 1',
                       schema, boundary_name || '_orisun_es_event', criteria_sql
                ) INTO latest_tx_id, latest_gid;

        IF latest_tx_id IS NULL THEN
            latest_tx_id := -1;
            latest_gid := -1;
        END IF;

        -- If expected_position is not provided, default to the empty context.
        IF expected_tx_id IS NULL OR expected_gid IS NULL THEN
            expected_tx_id := -1;
            expected_gid := -1;
        END IF;

        IF latest_tx_id <> expected_tx_id OR latest_gid <> expected_gid THEN
            RAISE EXCEPTION 'OptimisticConcurrencyException:StreamVersionConflict: Expected (%, %), Actual (%, %)',
                expected_tx_id, expected_gid, latest_tx_id, latest_gid;
        END IF;
    END IF;

    -- CTE-based insert using only schema-qualified table/sequence names.
    EXECUTE format('
        WITH events_with_ids AS MATERIALIZED (
            SELECT e,
                   nextval(%L) AS global_id,
                   jsonb_set(
                       CASE
                           WHEN jsonb_typeof(e -> ''data'') = ''string'' THEN (e ->> ''data'')::jsonb
                           ELSE COALESCE(e -> ''data'', ''{}'')
                       END,
                       ''{eventType}'',
                       to_jsonb(e ->> ''event_type''),
                       true
                   ) AS data_json,
                   CASE
                       WHEN jsonb_typeof(e -> ''metadata'') = ''string'' THEN (e ->> ''metadata'')::jsonb
                       ELSE COALESCE(e -> ''metadata'', ''{}'')
                   END AS metadata_json
            FROM jsonb_array_elements($1) AS e
        ),
        max_global_id AS (
            SELECT MAX(global_id) AS max_seq_overall,
                   MAX(global_id) + 1 AS logical_transaction_id
            FROM events_with_ids
        ),
        inserted_events AS (
            INSERT INTO %I.%I (
                                         transaction_id,
                                         pg_xact_id,
                                         event_id,
                                         global_id,
                                         data,
                                         metadata
            )
            SELECT max_global_id.logical_transaction_id,
                   NULL,
                   (e ->> ''event_id'')::UUID,
                   events_with_ids.global_id,
                   events_with_ids.data_json,
                   events_with_ids.metadata_json
            FROM events_with_ids
            CROSS JOIN max_global_id
            RETURNING transaction_id, global_id
        )
        SELECT MAX(global_id), MAX(transaction_id), MAX(global_id)
        FROM inserted_events',
                   prefixed_seq_name,
                   schema,
                   boundary_name || '_orisun_es_event'
            ) USING events INTO new_global_id, latest_transaction_id, latest_global_id;

    EXECUTE format('
        INSERT INTO %I.%I (boundary, transaction_id, global_id, date_created, date_updated)
        VALUES ($1, $2, $3, NOW(), NOW())
        ON CONFLICT (boundary)
        DO UPDATE SET transaction_id = $2,
                      global_id = $3,
                      date_updated = NOW()',
                   schema,
                   boundary_name || '_orisun_committed_position'
            ) USING boundary_name, latest_transaction_id, latest_global_id;

    PERFORM pg_notify('orisun_events_' || md5(boundary_name), new_global_id::text);

    RETURN QUERY SELECT new_global_id, latest_transaction_id, latest_global_id;
END;
$$;


-- Group-commit entry point. Each request runs in its own PL/pgSQL exception
-- block, which PostgreSQL-compatible databases implement as a subtransaction.
-- A failed CCC check therefore rolls back only that request while later
-- requests observe all earlier successful writes in this outer transaction.
CREATE OR REPLACE FUNCTION insert_event_requests_with_consistency_v1(
    boundary_name TEXT,
    schema TEXT,
    requests JSONB
)
    RETURNS TABLE
            (
                request_index         INT,
                new_global_id         BIGINT,
                latest_transaction_id BIGINT,
                latest_global_id      BIGINT,
                error_code            TEXT,
                error_message         TEXT
            )
    LANGUAGE plpgsql
AS
$$
DECLARE
    request                     JSONB;
    current_index               INT := 0;
    key_record                  TEXT;
    criteria_tags               TEXT[];
    request_new_global_id       BIGINT;
    request_transaction_id      BIGINT;
    request_global_id           BIGINT;
    request_error_code          TEXT;
    request_error_message       TEXT;
BEGIN
    IF requests IS NULL OR jsonb_typeof(requests) <> 'array' THEN
        RAISE EXCEPTION 'requests must be a JSON array';
    END IF;

    -- Preserve the Yugabyte lock hierarchy across the entire outer
    -- transaction. Without this pre-lock, request A could hold the position
    -- lock while a competing writer holds request B's criterion lock and waits
    -- for the position lock, deadlocking when this batch advances to B.
    SELECT ARRAY_AGG(DISTINCT criterion::TEXT ORDER BY criterion::TEXT)
    INTO criteria_tags
    FROM jsonb_array_elements(requests) AS request_item
    CROSS JOIN LATERAL jsonb_array_elements(request_item -> 'query' -> 'criteria') AS criterion;

    IF criteria_tags IS NOT NULL THEN
        FOREACH key_record IN ARRAY criteria_tags
            LOOP
                PERFORM pg_advisory_xact_lock(hashtext(key_record));
            END LOOP;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtext(schema || '.' || boundary_name || '::position_draw'));

    FOR request IN SELECT value FROM jsonb_array_elements(requests)
        LOOP
            request_new_global_id := NULL;
            request_transaction_id := NULL;
            request_global_id := NULL;
            request_error_code := NULL;
            request_error_message := NULL;

            BEGIN
                EXECUTE format(
                    'SELECT new_global_id, latest_transaction_id, latest_global_id
                     FROM %I.insert_events_with_consistency_v3($1, $2, $3, $4)',
                    schema
                )
                    INTO request_new_global_id, request_transaction_id, request_global_id
                    USING boundary_name, schema, request -> 'query', request -> 'events';
            EXCEPTION
                WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS
                        request_error_code = RETURNED_SQLSTATE,
                        request_error_message = MESSAGE_TEXT;
            END;

            RETURN QUERY
                SELECT current_index,
                       request_new_global_id,
                       request_transaction_id,
                       request_global_id,
                       request_error_code,
                       request_error_message;
            current_index := current_index + 1;
        END LOOP;
END;
$$;


-- Set-based fast path for canonical requests without a CCC query. The Go
-- batcher validates every event before selecting this function. One advisory
-- lock, sequence scan, INSERT, notification, statement, and commit serve every
-- request and event in the flush.
CREATE OR REPLACE FUNCTION insert_unconditional_event_requests_v1(
    boundary_name TEXT,
    schema TEXT,
    requests JSONB
)
    RETURNS TABLE
            (
                request_index         INT,
                new_global_id         BIGINT,
                latest_transaction_id BIGINT,
                latest_global_id      BIGINT,
                error_code            TEXT,
                error_message         TEXT
            )
    LANGUAGE plpgsql
AS
$$
DECLARE
    last_global_id     BIGINT;
    prefixed_seq_name  TEXT;
BEGIN
    IF requests IS NULL OR jsonb_typeof(requests) <> 'array' OR jsonb_array_length(requests) = 0 THEN
        RAISE EXCEPTION 'requests must be a non-empty JSON array';
    END IF;

    prefixed_seq_name := format('%I.%I', schema, boundary_name || '_orisun_es_event_global_id_seq');

    -- Keep position assignment commit-ordered across every Orisun process.
    PERFORM pg_advisory_xact_lock(hashtext(schema || '.' || boundary_name || '::position_draw'));

    RETURN QUERY EXECUTE format('
        WITH request_events AS MATERIALIZED (
            SELECT (request_ordinality - 1)::INT AS request_index,
                   (event_ordinality - 1)::INT AS event_index,
                   event
            FROM jsonb_array_elements($1) WITH ORDINALITY
                AS requests(request, request_ordinality)
            CROSS JOIN LATERAL jsonb_array_elements(request -> ''events'') WITH ORDINALITY
                AS events(event, event_ordinality)
            ORDER BY request_ordinality, event_ordinality
        ),
        events_with_ids AS MATERIALIZED (
            SELECT request_index,
                   event_index,
                   event,
                   nextval(%L) AS global_id
            FROM request_events
            ORDER BY request_index, event_index
        ),
        positioned_events AS MATERIALIZED (
            SELECT request_index,
                   event_index,
                   event,
                   global_id,
                   MAX(global_id) OVER (PARTITION BY request_index) + 1 AS transaction_id
            FROM events_with_ids
        ),
        inserted_events AS (
            INSERT INTO %I.%I (
                transaction_id,
                pg_xact_id,
                event_id,
                global_id,
                data,
                metadata
            )
            SELECT positioned_events.transaction_id,
                   NULL,
                   (event ->> ''event_id'')::UUID,
                   positioned_events.global_id,
                   jsonb_set(
                       COALESCE(event -> ''data'', ''{}''::JSONB),
                       ''{eventType}'',
                       to_jsonb(event ->> ''event_type''),
                       true
                   ),
                   COALESCE(event -> ''metadata'', ''{}''::JSONB)
            FROM positioned_events
            ORDER BY request_index, event_index
            RETURNING global_id
        ),
        inserted_requests AS (
            SELECT positioned_events.request_index,
                   MAX(positioned_events.global_id) AS global_id,
                   MAX(positioned_events.transaction_id) AS transaction_id
            FROM positioned_events
            JOIN inserted_events USING (global_id)
            GROUP BY positioned_events.request_index
        )
        SELECT inserted_requests.request_index,
               inserted_requests.global_id,
               inserted_requests.transaction_id,
               inserted_requests.global_id,
               NULL::TEXT,
               NULL::TEXT
        FROM inserted_requests
        ORDER BY inserted_requests.request_index',
        prefixed_seq_name,
        schema,
        boundary_name || '_orisun_es_event'
    ) USING requests;

    EXECUTE format('SELECT currval(%L)', prefixed_seq_name) INTO last_global_id;
    EXECUTE format('
        INSERT INTO %I.%I (boundary, transaction_id, global_id, date_created, date_updated)
        VALUES ($1, $2, $3, NOW(), NOW())
        ON CONFLICT (boundary)
        DO UPDATE SET transaction_id = $2,
                      global_id = $3,
                      date_updated = NOW()',
        schema,
        boundary_name || '_orisun_committed_position'
    ) USING boundary_name, last_global_id + 1, last_global_id;
    PERFORM pg_notify('orisun_events_' || md5(boundary_name), last_global_id::TEXT);
END;
$$;


-- Yugabyte keeps the generic batch implementation for the independent-context
-- entry point. It preserves the distributed criterion-lock hierarchy while
-- PostgreSQL uses a set-based indexed implementation for this specialization.
CREATE OR REPLACE FUNCTION insert_independent_event_requests_with_consistency_v1(
    boundary_name TEXT,
    schema TEXT,
    criterion_key TEXT,
    requests JSONB
)
    RETURNS TABLE
            (
                request_index         INT,
                new_global_id         BIGINT,
                latest_transaction_id BIGINT,
                latest_global_id      BIGINT,
                error_code            TEXT,
                error_message         TEXT
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT * FROM %I.insert_event_requests_with_consistency_v1($1, $2, $3)',
        schema
    ) USING boundary_name, schema, requests;
END;
$$;


-- Yugabyte keeps the generic batch implementation for canonical CCC requests:
-- that function pre-acquires the union of criterion locks before the position
-- lock, preserving its distributed lock hierarchy across the outer batch.
CREATE OR REPLACE FUNCTION insert_canonical_event_requests_with_consistency_v1(
    boundary_name TEXT,
    schema TEXT,
    requests JSONB
)
    RETURNS TABLE
            (
                request_index         INT,
                new_global_id         BIGINT,
                latest_transaction_id BIGINT,
                latest_global_id      BIGINT,
                error_code            TEXT,
                error_message         TEXT
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY EXECUTE format(
        'SELECT * FROM %I.insert_event_requests_with_consistency_v1($1, $2, $3)',
        schema
    ) USING boundary_name, schema, requests;
END;
$$;


-- Get Matching Events Function
--
-- Reads events from a boundary event table for PostgresGetEvents.Get. The
-- criteria parameter is either NULL or {"criteria": [criterion, ...]}, matching
-- the same content-query shape used by saves: tags inside one criterion are ANDed,
-- and criteria are ORed.
--
-- Parameters:
--   boundary_name (TEXT): Boundary/table prefix
--   schema (TEXT): PostgreSQL schema containing the boundary table
--   criteria (JSONB): Optional content query wrapper
--   after_position (JSONB): Optional {"transaction_id": ..., "global_id": ...}
--   sort_dir (TEXT): Sort direction ('ASC' or 'DESC')
--   max_count (INT): Maximum number of events to return, clamped to [1, 10000]
--
-- Position filtering is inclusive: ASC reads from >= after_position and DESC
-- reads from <= after_position. ASC reads also apply a stable-prefix visibility
-- barrier, hiding rows from transactions that are still in flight according to
-- the committed-position watermark. Rows beyond the watermark are not returned
-- to ASC readers, which keeps publisher scans on a stable committed prefix.

CREATE OR REPLACE FUNCTION get_matching_events_v3(
    boundary_name TEXT,
    schema TEXT,
    criteria JSONB DEFAULT NULL,
    after_position JSONB DEFAULT NULL,
    sort_dir TEXT DEFAULT 'ASC',
    max_count INT DEFAULT 1000
)
    RETURNS TABLE
            (
                transaction_id BIGINT,
                global_id      BIGINT,
                event_id       UUID,
                event_type     TEXT,
                data           JSONB,
                metadata       JSONB,
                date_created   TIMESTAMPTZ
            )
    LANGUAGE plpgsql
    STABLE
AS
$$
DECLARE
    op                   TEXT  := CASE WHEN sort_dir = 'ASC' THEN '>' ELSE '<' END;
    qualified_table_name TEXT;
    qualified_watermark_table TEXT;
    criteria_array       JSONB := criteria -> 'criteria';
    tx_id                TEXT  := (after_position ->> 'transaction_id')::text;
    global_id            TEXT  := (after_position ->> 'global_id')::text;
    criteria_sql         TEXT;
    crit                 JSONB;
    crit_parts           TEXT[];
    all_parts            TEXT[];
    k                    TEXT;
    v                    TEXT;
BEGIN
    IF sort_dir NOT IN ('ASC', 'DESC') THEN
        RAISE EXCEPTION 'Invalid sort direction: "%"', sort_dir;
    END IF;

    -- Build the schema-qualified boundary event table name.
    qualified_table_name := format('%I.%I_orisun_es_event', schema, boundary_name);
    qualified_watermark_table := format('%I.%I_orisun_committed_position', schema, boundary_name);

    -- Build the content query as an OR of criteria, where each criterion is
    -- an AND of tag equality checks.
    IF criteria_array IS NOT NULL THEN
        all_parts := '{}';
        FOR crit IN SELECT jsonb_array_elements(criteria_array)
            LOOP
                crit_parts := '{}';
                FOR k, v IN SELECT * FROM jsonb_each_text(crit)
                    LOOP
                        crit_parts := crit_parts || format('(data->>%L = %L)', k, v);
                    END LOOP;
                IF array_length(crit_parts, 1) > 0 THEN
                    all_parts := all_parts || ('(' || array_to_string(crit_parts, ' AND ') || ')');
                END IF;
            END LOOP;
        criteria_sql := CASE
                            WHEN array_length(all_parts, 1) > 0
                                THEN '(' || array_to_string(all_parts, ' OR ') || ')'
                            ELSE 'TRUE'
            END;
    ELSE
        criteria_sql := 'TRUE';
    END IF;

    -- Use dynamic SQL because the boundary table name and criteria predicate are dynamic.
    RETURN QUERY EXECUTE format(
            $q$
        SELECT transaction_id, global_id, event_id, data->>'eventType' AS event_type, data, metadata, date_created
        FROM %s
        WHERE
            %2$s AND
            (%8$L != 'ASC' OR (transaction_id, global_id) <= (
                SELECT cp.transaction_id, cp.global_id
                FROM %10$s cp
                WHERE cp.boundary = %11$L
            )) AND
            (%3$L IS NULL OR (
                    (transaction_id, global_id) %4$s= (
                        %5$L::BIGINT,
                        %6$L::BIGINT
                    )
                )
            )
        ORDER BY transaction_id %8$s, global_id %8$s
        LIMIT %9$L
        $q$,
            qualified_table_name,
            criteria_sql,
            after_position,
            op,
            tx_id,
            global_id,
            '',
            sort_dir,
            LEAST(GREATEST(max_count, 1), 10000),
            qualified_watermark_table,
            boundary_name
                         );
END;
$$;

-- get_latest_by_criteria_v1 returns the newest event matching each requested
-- criterion, all from ONE statement and therefore one PostgreSQL snapshot. The
-- Go caller computes context_position as the maximum returned event position and
-- uses it as SaveEvents.query.expected_position. One snapshot is the point:
-- assembling the same context from independent queries lets an event commit in
-- between with a position below the observed maximum, which a scalar
-- expected-position check cannot detect.
--
-- This function returns one row per matching criterion only. Criteria with no
-- matching event are omitted; the Go caller maps missing indexes back to empty
-- LatestCriterionResult entries.
CREATE OR REPLACE FUNCTION get_latest_by_criteria_v1(
    boundary_name TEXT,
    schema TEXT,
    criteria JSONB
)
    RETURNS TABLE
            (
                criterion_idx  INT,
                transaction_id BIGINT,
                global_id      BIGINT,
                event_id       UUID,
                event_type     TEXT,
                data           JSONB,
                metadata       JSONB,
                date_created   TIMESTAMPTZ
            )
    LANGUAGE plpgsql
    STABLE
AS
$$
DECLARE
    qualified_table_name TEXT;
    criteria_array       JSONB  := criteria -> 'criteria';
    crit                 JSONB;
    crit_parts           TEXT[];
    selects              TEXT[] := '{}';
    idx                  INT    := 0;
    k                    TEXT;
    v                    TEXT;
BEGIN
    IF criteria_array IS NULL OR jsonb_array_length(criteria_array) = 0 THEN
        RAISE EXCEPTION 'criteria cannot be empty';
    END IF;

    qualified_table_name := format('%I.%I', schema, boundary_name || '_orisun_es_event');

    FOR crit IN SELECT jsonb_array_elements(criteria_array)
        LOOP
            crit_parts := '{}';
            FOR k, v IN SELECT * FROM jsonb_each_text(crit)
                LOOP
                    crit_parts := crit_parts || format('(data->>%L = %L)', k, v);
                END LOOP;
            IF array_length(crit_parts, 1) IS NULL THEN
                RAISE EXCEPTION 'criterion % has no tags', idx;
            END IF;
            selects := selects || format(
                    '(SELECT %s AS criterion_idx, e.transaction_id, e.global_id, e.event_id, e.data->>''eventType'' AS event_type, e.data, e.metadata, e.date_created FROM %s e WHERE %s ORDER BY e.transaction_id DESC, e.global_id DESC LIMIT 1)',
                    idx, qualified_table_name, array_to_string(crit_parts, ' AND '));
            idx := idx + 1;
        END LOOP;

    RETURN QUERY EXECUTE array_to_string(selects, ' UNION ALL ');
END;
$$;
