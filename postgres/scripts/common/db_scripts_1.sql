-- Initialize Boundary Tables Function
--
-- Creates or maintains the PostgreSQL objects used by one logical boundary in a
-- caller-supplied schema. Boundaries are mapped to schemas by Go configuration;
-- the boundary name is used only as the table/sequence prefix inside that schema.
--
-- Parameters:
--   boundary_name (TEXT): Boundary/table prefix, validated as a PostgreSQL identifier
--   schema_name (TEXT): PostgreSQL schema where the boundary objects live
--
-- Creates or maintains:
--   <boundary>_orisun_es_event
--   <boundary>_orisun_es_event_global_id_seq
--   <boundary>_orisun_last_published_event_position
--   <boundary>_events_count
--   <boundary>_projector_checkpoint
--
-- transaction_id is the logical commit position used by clients, projectors,
-- and publishing checkpoints. pg_xact_id is only an internal visibility marker
-- for current-cluster in-flight transaction checks.

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

    -- pg_xact_id is current-cluster-only. After dump/restore or a major upgrade
    -- into a fresh cluster, the new cluster's xid8 can restart below values
    -- stored by the old cluster. Those stale values must not be used as a
    -- visibility barrier, or old committed rows can be hidden until the new
    -- cluster's XID counter catches up.
    EXECUTE format('
        UPDATE %I.%I
        SET pg_xact_id = NULL
        WHERE pg_xact_id IS NOT NULL
          AND pg_xact_id >= pg_current_xact_id()::TEXT::BIGINT',
                   schema_name, boundary_name || '_orisun_es_event');

    EXECUTE format('SELECT setval(%L::regclass, (SELECT COALESCE(MAX(global_id) + 1, 0) FROM %I.%I), false)',
                   prefixed_seq_name,
                   schema_name,
                   boundary_name || '_orisun_es_event');

    -- Older releases included the unbounded JSONB data and metadata columns in
    -- these B-tree indexes. PostgreSQL applies its index-tuple size limit to
    -- INCLUDE columns too, so sufficiently large events could not be inserted.
    -- Drop only those legacy managed definitions; the lean replacements below
    -- keep ordered reads fast without copying event payloads into the index.
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
    current_pg_xact_id    BIGINT;
    latest_tx_id          BIGINT;
    latest_gid            BIGINT;
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
    current_pg_xact_id := pg_current_xact_id()::TEXT::BIGINT;

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

    -- Per-boundary position lock: held through the context read, position draw,
    -- insert, and commit. This serialises writers per boundary so the
    -- expected-position check and assigned positions share one ordering.
    PERFORM pg_advisory_xact_lock(hashtext(schema || '.' || boundary_name || '::position_draw'));

    -- If criteria are present, verify the caller's expected content-query
    -- position against the latest committed event matching that context.
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
            FROM jsonb_array_elements($2) AS e
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
                   $1,
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
            ) USING current_pg_xact_id, events INTO new_global_id, latest_transaction_id, latest_global_id;

    PERFORM pg_notify('orisun_events_' || md5(boundary_name), new_global_id::text);

    RETURN QUERY SELECT new_global_id, latest_transaction_id, latest_global_id;
END;
$$;


-- Group-commit entry point. Each request runs in its own PL/pgSQL exception
-- block, which PostgreSQL implements as a subtransaction. A failed CCC check
-- therefore rolls back only that request while later requests observe all
-- earlier successful writes in this outer transaction.
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
    request_new_global_id       BIGINT;
    request_transaction_id      BIGINT;
    request_global_id           BIGINT;
    request_error_code          TEXT;
    request_error_message       TEXT;
BEGIN
    IF requests IS NULL OR jsonb_typeof(requests) <> 'array' THEN
        RAISE EXCEPTION 'requests must be a JSON array';
    END IF;

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
    current_pg_xact_id BIGINT;
    last_global_id     BIGINT;
    prefixed_seq_name  TEXT;
BEGIN
    IF requests IS NULL OR jsonb_typeof(requests) <> 'array' OR jsonb_array_length(requests) = 0 THEN
        RAISE EXCEPTION 'requests must be a non-empty JSON array';
    END IF;

    current_pg_xact_id := pg_current_xact_id()::TEXT::BIGINT;
    prefixed_seq_name := format('%I.%I', schema, boundary_name || '_orisun_es_event_global_id_seq');

    -- Keep position assignment commit-ordered across every Orisun process.
    PERFORM pg_advisory_xact_lock(hashtext(schema || '.' || boundary_name || '::position_draw'));

    RETURN QUERY EXECUTE format('
        WITH request_events AS MATERIALIZED (
            SELECT (request_ordinality - 1)::INT AS request_index,
                   (event_ordinality - 1)::INT AS event_index,
                   event
            FROM jsonb_array_elements($2) WITH ORDINALITY
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
                   $1,
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
    ) USING current_pg_xact_id, requests;

    EXECUTE format('SELECT currval(%L)', prefixed_seq_name) INTO last_global_id;
    PERFORM pg_notify('orisun_events_' || md5(boundary_name), last_global_id::TEXT);
END;
$$;


-- Set-based CCC fast path for independent one-tag contexts. The Go batcher
-- selects this only when every request has exactly one criterion/tag on the
-- same key, every value is unique, and each event carries its own criterion
-- value. Under those conditions no event in the batch can invalidate another
-- request, so all contexts can be checked against one position-locked snapshot
-- and every accepted event can be inserted in one statement.
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
DECLARE
    current_pg_xact_id BIGINT;
    last_global_id     BIGINT;
    prefixed_seq_name  TEXT;
BEGIN
    IF requests IS NULL OR jsonb_typeof(requests) <> 'array' OR jsonb_array_length(requests) = 0 THEN
        RAISE EXCEPTION 'requests must be a non-empty JSON array';
    END IF;
    IF criterion_key IS NULL OR criterion_key = '' THEN
        RAISE EXCEPTION 'criterion_key must not be empty';
    END IF;

    current_pg_xact_id := pg_current_xact_id()::TEXT::BIGINT;
    prefixed_seq_name := format('%I.%I', schema, boundary_name || '_orisun_es_event_global_id_seq');
    PERFORM pg_advisory_xact_lock(hashtext(schema || '.' || boundary_name || '::position_draw'));

    RETURN QUERY EXECUTE format('
        WITH request_rows AS MATERIALIZED (
            SELECT (ordinality - 1)::INT AS request_index,
                   request,
                   request -> ''query'' -> ''criteria'' -> 0 ->> %L AS context_value,
                   COALESCE(
                       (request -> ''query'' -> ''expected_position'' ->> ''transaction_id'')::BIGINT,
                       -1
                   ) AS expected_transaction_id,
                   COALESCE(
                       (request -> ''query'' -> ''expected_position'' ->> ''global_id'')::BIGINT,
                       -1
                   ) AS expected_global_id
            FROM jsonb_array_elements($2) WITH ORDINALITY AS request_items(request, ordinality)
        ),
        latest_by_context AS MATERIALIZED (
            SELECT DISTINCT ON (stored.data ->> %L)
                   stored.data ->> %L AS context_value,
                   stored.transaction_id,
                   stored.global_id
            FROM %I.%I stored
            WHERE stored.data ->> %L = ANY (
                ARRAY(SELECT request_rows.context_value FROM request_rows)
            )
            ORDER BY stored.data ->> %L,
                     stored.transaction_id DESC,
                     stored.global_id DESC
        ),
        checked AS MATERIALIZED (
            SELECT request_rows.*,
                   COALESCE(latest_by_context.transaction_id, -1) AS actual_transaction_id,
                   COALESCE(latest_by_context.global_id, -1) AS actual_global_id
            FROM request_rows
            LEFT JOIN latest_by_context USING (context_value)
        ),
        accepted AS MATERIALIZED (
            SELECT checked.request_index,
                   (event_ordinality - 1)::INT AS event_index,
                   event,
                   nextval(%L) AS global_id
            FROM checked
            CROSS JOIN LATERAL jsonb_array_elements(
                checked.request -> ''events''
            ) WITH ORDINALITY AS events(event, event_ordinality)
            WHERE checked.expected_transaction_id = checked.actual_transaction_id
              AND checked.expected_global_id = checked.actual_global_id
            ORDER BY checked.request_index, event_ordinality
        ),
        positioned AS MATERIALIZED (
            SELECT checked.request_index,
                   accepted.event_index,
                   accepted.event,
                   accepted.global_id,
                   MAX(accepted.global_id) OVER (
                       PARTITION BY accepted.request_index
                   ) + 1 AS transaction_id
            FROM accepted
            JOIN checked USING (request_index)
        ),
        inserted AS (
            INSERT INTO %I.%I (
                transaction_id,
                pg_xact_id,
                event_id,
                global_id,
                data,
                metadata
            )
            SELECT positioned.transaction_id,
                   $1,
                   (positioned.event ->> ''event_id'')::UUID,
                   positioned.global_id,
                   jsonb_set(
                       COALESCE(positioned.event -> ''data'', ''{}''::JSONB),
                       ''{eventType}'',
                       to_jsonb(positioned.event ->> ''event_type''),
                       true
                   ),
                   COALESCE(positioned.event -> ''metadata'', ''{}''::JSONB)
            FROM positioned
            ORDER BY positioned.request_index, positioned.event_index
            RETURNING global_id
        ),
        accepted_requests AS MATERIALIZED (
            SELECT request_index,
                   MAX(global_id) AS global_id,
                   MAX(transaction_id) AS transaction_id
            FROM positioned
            GROUP BY request_index
        )
        SELECT checked.request_index,
               accepted_requests.global_id,
               accepted_requests.transaction_id,
               accepted_requests.global_id,
               CASE WHEN accepted_requests.global_id IS NULL THEN ''P0001''::TEXT ELSE NULL::TEXT END,
               CASE
                   WHEN accepted_requests.global_id IS NULL THEN format(
                       ''OptimisticConcurrencyException:StreamVersionConflict: Expected (%%s, %%s), Actual (%%s, %%s)'',
                       checked.expected_transaction_id,
                       checked.expected_global_id,
                       checked.actual_transaction_id,
                       checked.actual_global_id
                   )
                   ELSE NULL::TEXT
               END
        FROM checked
        LEFT JOIN accepted_requests USING (request_index)
        CROSS JOIN (SELECT COUNT(*) FROM inserted) AS completed_insert
        ORDER BY checked.request_index',
        criterion_key,
        criterion_key,
        criterion_key,
        schema,
        boundary_name || '_orisun_es_event',
        criterion_key,
        criterion_key,
        prefixed_seq_name,
        schema,
        boundary_name || '_orisun_es_event'
    ) USING current_pg_xact_id, requests;

    EXECUTE format('SELECT last_value FROM %I.%I',
                   schema,
                   boundary_name || '_orisun_es_event_global_id_seq')
        INTO last_global_id;
    PERFORM pg_notify('orisun_events_' || md5(boundary_name), last_global_id::TEXT);
END;
$$;


-- General criterion-state path for canonical event-batch requests. Initial
-- positions and event dependencies are resolved set-wise by criterion shape;
-- requests are then evaluated in queue order before one bulk insert. This
-- preserves arbitrary AND/OR CCC semantics without one event-table query or
-- subtransaction per request.
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
DECLARE
    request                  JSONB;
    query_json                JSONB;
    criteria                  JSONB;
    all_criteria              JSONB := '[]'::JSONB;
    criterion_ids             JSONB := '{}'::JSONB;
    criterion_tx_ids          BIGINT[] := '{}'::BIGINT[];
    criterion_gids            BIGINT[] := '{}'::BIGINT[];
    event_matches             JSONB := '{}'::JSONB;
    accepted_request_indexes  INT[] := '{}'::INT[];
    accepted_event_indexes    INT[] := '{}'::INT[];
    accepted_global_ids       BIGINT[] := '{}'::BIGINT[];
    request_global_ids        BIGINT[] := '{}'::BIGINT[];
    crit                       JSONB;
    criterion_id               INT;
    criterion_count            INT := 0;
    expected_tx_id            BIGINT;
    expected_gid              BIGINT;
    latest_tx_id              BIGINT;
    latest_gid                BIGINT;
    criterion_tx_id           BIGINT;
    criterion_gid             BIGINT;
    inserted_tx_id            BIGINT;
    inserted_gid              BIGINT;
    last_inserted_gid         BIGINT;
    event_index               INT;
    event_count               INT;
    matching_event_index      INT;
    current_index             INT := 0;
    current_pg_xact_id        BIGINT;
    prefixed_seq_name         TEXT;
    has_nonempty_criterion    BOOLEAN;
    needs_global_state        BOOLEAN;
    criterion_shape           JSONB;
    shape_criteria            JSONB;
    criterion_key             TEXT;
    join_parts                TEXT[];
    event_join_parts          TEXT[];
    latest_selects            TEXT[] := '{}'::TEXT[];
    dependency_selects        TEXT[] := '{}'::TEXT[];
    latest_record             RECORD;
    dependency_record         RECORD;
BEGIN
    IF requests IS NULL OR jsonb_typeof(requests) <> 'array' OR jsonb_array_length(requests) = 0 THEN
        RAISE EXCEPTION 'requests must be a non-empty JSON array';
    END IF;

    current_pg_xact_id := pg_current_xact_id()::TEXT::BIGINT;
    prefixed_seq_name := format('%I.%I', schema, boundary_name || '_orisun_es_event_global_id_seq');
    PERFORM pg_advisory_xact_lock(hashtext(schema || '.' || boundary_name || '::position_draw'));

    -- Decompose every OR query into a batch-wide set of distinct AND
    -- criteria. A criterion's state is the latest position matching that
    -- object. The latest position for an OR query is therefore the maximum
    -- state among its criteria.
    SELECT COALESCE(jsonb_agg(criterion ORDER BY criterion::TEXT), '[]'::JSONB)
    INTO all_criteria
    FROM (
        SELECT DISTINCT criterion
        FROM jsonb_array_elements(requests) AS request_items(request_item)
        CROSS JOIN LATERAL jsonb_array_elements(
            COALESCE(request_item -> 'query' -> 'criteria', '[]'::JSONB)
        ) AS criteria_items(criterion)
        WHERE jsonb_typeof(criterion) = 'object'
          AND criterion <> '{}'::JSONB
    ) AS distinct_criteria;

    SELECT COALESCE(
               jsonb_object_agg(criterion::TEXT, (ordinality - 1)::INT),
               '{}'::JSONB
           ),
           COUNT(*)::INT
    INTO criterion_ids, criterion_count
    FROM jsonb_array_elements(all_criteria) WITH ORDINALITY
        AS criteria_items(criterion, ordinality);

    IF criterion_count > 0 THEN
        criterion_tx_ids := array_fill(-1::BIGINT, ARRAY[criterion_count]);
        criterion_gids := array_fill(-1::BIGINT, ARRAY[criterion_count]);
    END IF;

    -- Build one shape-specific branch for snapshot lookup and dependency
    -- matching, then execute all branches together. This retains indexable
    -- equality predicates without paying one SQL execution per shape.
    IF jsonb_array_length(all_criteria) > 0 THEN
        FOR criterion_shape IN
            SELECT DISTINCT (
                SELECT jsonb_agg(key ORDER BY key)
                FROM jsonb_object_keys(criterion) AS keys(key)
            )
            FROM jsonb_array_elements(all_criteria) AS criteria_items(criterion)
            LOOP
                SELECT jsonb_agg(criterion ORDER BY criterion::TEXT)
                INTO shape_criteria
                FROM jsonb_array_elements(all_criteria) AS criteria_items(criterion)
                WHERE (
                    SELECT jsonb_agg(key ORDER BY key)
                    FROM jsonb_object_keys(criterion) AS keys(key)
                ) = criterion_shape;

                join_parts := '{}';
                event_join_parts := '{}';
                FOR criterion_key IN SELECT value #>> '{}' FROM jsonb_array_elements(criterion_shape)
                    LOOP
                        join_parts := join_parts || format(
                            '(stored.data ->> %L = criteria.criterion ->> %L)',
                            criterion_key,
                            criterion_key
                        );
                        event_join_parts := event_join_parts || format(
                            '(event_rows.event_data ->> %L = criteria.criterion ->> %L)',
                            criterion_key,
                            criterion_key
                        );
                    END LOOP;

                latest_selects := latest_selects || format(
                    '(WITH criteria AS MATERIALIZED (
                         SELECT value AS criterion
                         FROM jsonb_array_elements(%L::JSONB)
                     ),
                     ranked AS (
                         SELECT criteria.criterion,
                                stored.transaction_id,
                                stored.global_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY criteria.criterion
                                    ORDER BY stored.transaction_id DESC, stored.global_id DESC
                                ) AS match_rank
                         FROM criteria
                         JOIN %I.%I stored ON %s
                     )
                     SELECT criterion, transaction_id, global_id
                     FROM ranked
                     WHERE match_rank = 1)',
                    shape_criteria::TEXT,
                    schema,
                    boundary_name || '_orisun_es_event',
                    array_to_string(join_parts, ' AND ')
                );

                -- Build event-to-criterion dependencies with an equality join
                -- for this shape. PostgreSQL can hash the in-memory batch
                -- values instead of evaluating every event/criterion pair.
                dependency_selects := dependency_selects || format(
                    'SELECT event_rows.request_index,
                            event_rows.event_index,
                            ($2 ->> criteria.criterion::TEXT)::INT AS criterion_id
                     FROM event_rows
                     JOIN (
                         SELECT value AS criterion
                         FROM jsonb_array_elements(%L::JSONB)
                     ) AS criteria ON %s',
                    shape_criteria::TEXT,
                    array_to_string(event_join_parts, ' AND ')
                );
            END LOOP;

        FOR latest_record IN EXECUTE array_to_string(latest_selects, ' UNION ALL ')
            LOOP
                criterion_id :=
                    (criterion_ids ->> latest_record.criterion::TEXT)::INT + 1;
                criterion_tx_ids[criterion_id] := latest_record.transaction_id;
                criterion_gids[criterion_id] := latest_record.global_id;
            END LOOP;

        EXECUTE format(
            'WITH event_rows AS MATERIALIZED (
                 SELECT (request_ordinality - 1)::INT AS request_index,
                        (event_ordinality - 1)::INT AS event_index,
                        event_item -> ''data'' AS event_data
                 FROM jsonb_array_elements($1) WITH ORDINALITY AS
                     request_items(request_item, request_ordinality)
                 CROSS JOIN LATERAL jsonb_array_elements(
                     request_item -> ''events''
                 ) WITH ORDINALITY AS events(event_item, event_ordinality)
             ),
             dependency_edges AS (
                 %s
             ),
             latest_matching_events AS (
                 SELECT request_index,
                        criterion_id,
                        MAX(event_index) AS event_index
                 FROM dependency_edges
                 GROUP BY request_index, criterion_id
             ),
             matches_by_request AS (
                 SELECT request_index,
                        jsonb_object_agg(
                            criterion_id::TEXT,
                            event_index
                            ORDER BY criterion_id
                        ) AS matched_criteria
                 FROM latest_matching_events
                 GROUP BY request_index
             )
             SELECT COALESCE(
                 jsonb_object_agg(request_index::TEXT, matched_criteria),
                 ''{}''::JSONB
             ) AS match_map
             FROM matches_by_request',
            array_to_string(dependency_selects, ' UNION ALL ')
        ) INTO dependency_record USING requests, criterion_ids;
        event_matches := dependency_record.match_map;
    END IF;

    -- A present but empty criteria array has historically meant "all events".
    -- Query-less saves have no criteria property and remain unconditional.
    SELECT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(requests) AS request_items(request_item)
        WHERE (request_item -> 'query') ? 'criteria'
          AND NOT EXISTS (
              SELECT 1
              FROM jsonb_array_elements(request_item -> 'query' -> 'criteria')
                  AS criteria_items(criterion)
              WHERE jsonb_typeof(criterion) = 'object'
                AND criterion <> '{}'::JSONB
          )
    ) INTO needs_global_state;

    latest_tx_id := -1;
    latest_gid := -1;
    IF needs_global_state THEN
        EXECUTE format(
            'SELECT transaction_id, global_id
             FROM %I.%I
             ORDER BY transaction_id DESC, global_id DESC
             LIMIT 1',
            schema,
            boundary_name || '_orisun_es_event'
        ) INTO latest_tx_id, latest_gid;
        latest_tx_id := COALESCE(latest_tx_id, -1);
        latest_gid := COALESCE(latest_gid, -1);
    END IF;

    FOR request IN SELECT value FROM jsonb_array_elements(requests)
        LOOP
            query_json := request -> 'query';
            criteria := query_json -> 'criteria';
            expected_tx_id := COALESCE(
                (query_json -> 'expected_position' ->> 'transaction_id')::BIGINT,
                -1
            );
            expected_gid := COALESCE(
                (query_json -> 'expected_position' ->> 'global_id')::BIGINT,
                -1
            );

            IF criteria IS NOT NULL THEN
                has_nonempty_criterion := FALSE;
                criterion_tx_id := -1;
                criterion_gid := -1;
                FOR crit IN SELECT jsonb_array_elements(criteria)
                    LOOP
                        IF jsonb_typeof(crit) = 'object' AND crit <> '{}'::JSONB THEN
                            has_nonempty_criterion := TRUE;
                            criterion_id := (criterion_ids ->> crit::TEXT)::INT + 1;
                            IF criterion_id IS NOT NULL THEN
                                IF criterion_tx_ids[criterion_id] > criterion_tx_id OR
                                   (
                                       criterion_tx_ids[criterion_id] = criterion_tx_id AND
                                       criterion_gids[criterion_id] > criterion_gid
                                   ) THEN
                                    criterion_tx_id := criterion_tx_ids[criterion_id];
                                    criterion_gid := criterion_gids[criterion_id];
                                END IF;
                            END IF;
                        END IF;
                    END LOOP;

                IF has_nonempty_criterion THEN
                    criterion_tx_id := COALESCE(criterion_tx_id, -1);
                    criterion_gid := COALESCE(criterion_gid, -1);
                ELSE
                    criterion_tx_id := latest_tx_id;
                    criterion_gid := latest_gid;
                END IF;

                IF criterion_tx_id <> expected_tx_id OR criterion_gid <> expected_gid THEN
                    RETURN QUERY
                        SELECT current_index,
                               NULL::BIGINT,
                               NULL::BIGINT,
                               NULL::BIGINT,
                               'P0001'::TEXT,
                               format(
                                   'OptimisticConcurrencyException:StreamVersionConflict: Expected (%s, %s), Actual (%s, %s)',
                                   expected_tx_id,
                                   expected_gid,
                                   criterion_tx_id,
                                   criterion_gid
                               );
                    current_index := current_index + 1;
                    CONTINUE;
                END IF;
            END IF;

            request_global_ids := '{}'::BIGINT[];
            event_count := jsonb_array_length(request -> 'events');
            FOR event_index IN 0..event_count - 1
                LOOP
                    SELECT nextval(prefixed_seq_name::regclass) INTO inserted_gid;
                    request_global_ids := array_append(request_global_ids, inserted_gid);
                    accepted_request_indexes :=
                        array_append(accepted_request_indexes, current_index);
                    accepted_event_indexes :=
                        array_append(accepted_event_indexes, event_index);
                    accepted_global_ids :=
                        array_append(accepted_global_ids, inserted_gid);
                END LOOP;

            inserted_tx_id := inserted_gid + 1;

            last_inserted_gid := inserted_gid;
            latest_tx_id := inserted_tx_id;
            latest_gid := inserted_gid;

            -- Advance every criterion matched by this accepted request. Each
            -- criterion keeps the highest matching event's global ID and the
            -- transaction ID shared by the entire multi-event request.
            FOR latest_record IN
                SELECT key, value
                FROM jsonb_each_text(
                    COALESCE(event_matches -> current_index::TEXT, '{}'::JSONB)
                )
                LOOP
                    criterion_id := latest_record.key::INT + 1;
                    matching_event_index := latest_record.value::INT;
                    criterion_tx_ids[criterion_id] := inserted_tx_id;
                    criterion_gids[criterion_id] :=
                        request_global_ids[matching_event_index + 1];
                END LOOP;

            RETURN QUERY
                SELECT current_index,
                       inserted_gid,
                       inserted_tx_id,
                       inserted_gid,
                       NULL::TEXT,
                       NULL::TEXT;
            current_index := current_index + 1;
        END LOOP;

    IF last_inserted_gid IS NOT NULL THEN
        EXECUTE format('
            INSERT INTO %I.%I (
                transaction_id,
                pg_xact_id,
                event_id,
                global_id,
                data,
                metadata
            )
            WITH accepted_events AS (
                SELECT accepted.request_index,
                       accepted.event_index,
                       accepted.global_id,
                       MAX(accepted.global_id) OVER (
                           PARTITION BY accepted.request_index
                       ) + 1 AS transaction_id
                FROM unnest($2::INT[], $3::INT[], $4::BIGINT[])
                    AS accepted(request_index, event_index, global_id)
            )
            SELECT accepted.transaction_id,
                   $1,
                   (request_item -> ''events'' -> accepted.event_index ->> ''event_id'')::UUID,
                   accepted.global_id,
                   jsonb_set(
                       COALESCE(
                           request_item -> ''events'' -> accepted.event_index -> ''data'',
                           ''{}''::JSONB
                       ),
                       ''{eventType}'',
                       to_jsonb(
                           request_item -> ''events'' -> accepted.event_index ->> ''event_type''
                       ),
                       TRUE
                   ),
                   COALESCE(
                       request_item -> ''events'' -> accepted.event_index -> ''metadata'',
                       ''{}''::JSONB
                   )
            FROM accepted_events AS accepted
            CROSS JOIN LATERAL (
                SELECT $5 -> accepted.request_index AS request_item
            ) AS accepted_requests
            ORDER BY accepted.global_id',
            schema,
            boundary_name || '_orisun_es_event'
        ) USING
            current_pg_xact_id,
            accepted_request_indexes,
            accepted_event_indexes,
            accepted_global_ids,
            requests;
        PERFORM pg_notify('orisun_events_' || md5(boundary_name), last_inserted_gid::TEXT);
    END IF;
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
-- pg_xact_id. Rows with NULL pg_xact_id are legacy/restored rows and are treated
-- as visible.

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
            (%8$L != 'ASC' OR pg_xact_id IS NULL OR pg_xact_id::TEXT::xid8 < pg_snapshot_xmin(pg_current_snapshot())) AND
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
            LEAST(GREATEST(max_count, 1), 10000)
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
