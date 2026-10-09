-- Compare JSON number tokens exactly, including values beyond float64 precision.
-- Invalid numeric targets are incomparable. Exponents do not allocate expanded
-- decimal strings.
CREATE OR REPLACE FUNCTION orisun_compare_number(a TEXT, b TEXT)
RETURNS INT LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    tokens TEXT[] := ARRAY[a,b];
    digits TEXT[] := ARRAY['',''];
    magnitudes NUMERIC[] := ARRAY[0::NUMERIC,0::NUMERIC];
    negatives BOOLEAN[] := ARRAY[FALSE,FALSE];
    token TEXT;
    exponent_at INT;
    decimal_at INT;
    fraction_length INT;
    i INT;
    comparison INT := 0;
BEGIN
    FOR i IN 1..2 LOOP
        token := tokens[i];
        IF token !~ '^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$' THEN RETURN NULL; END IF;
        negatives[i] := left(token,1) = '-';
        IF negatives[i] THEN token := substring(token FROM 2); END IF;
        exponent_at := strpos(lower(token), 'e');
        IF exponent_at > 0 THEN
            magnitudes[i] := substring(token FROM exponent_at+1)::NUMERIC;
            token := left(token, exponent_at-1);
        END IF;
        decimal_at := strpos(token, '.');
        fraction_length := 0;
        IF decimal_at > 0 THEN
            fraction_length := length(token)-decimal_at;
            token := replace(token, '.', '');
        END IF;
        digits[i] := ltrim(token, '0');
        IF digits[i] = '' THEN negatives[i] := FALSE;
        ELSE magnitudes[i] := magnitudes[i] + length(digits[i])-fraction_length;
        END IF;
    END LOOP;
    IF negatives[1] <> negatives[2] THEN
        RETURN CASE WHEN negatives[1] THEN -1 ELSE 1 END;
    END IF;
    IF digits[1] = '' AND digits[2] = '' THEN RETURN 0;
    ELSIF digits[1] = '' THEN comparison := -1;
    ELSIF digits[2] = '' THEN comparison := 1;
    ELSIF magnitudes[1] < magnitudes[2] THEN comparison := -1;
    ELSIF magnitudes[1] > magnitudes[2] THEN comparison := 1;
    ELSE
        digits[1] := rpad(digits[1], greatest(length(digits[1]),length(digits[2])), '0');
        digits[2] := rpad(digits[2], length(digits[1]), '0');
        IF digits[1] COLLATE "C" < digits[2] COLLATE "C" THEN comparison := -1;
        ELSIF digits[1] COLLATE "C" > digits[2] COLLATE "C" THEN comparison := 1;
        END IF;
    END IF;
    RETURN CASE WHEN negatives[1] THEN -comparison ELSE comparison END;
END;
$$;

-- One renderer owns predicates for reads, latest observations, persisted CCC
-- checks, and matching accepted events within a group-commit transaction.
-- data_expression is supplied only by the storage implementation, never a query.
CREATE OR REPLACE FUNCTION orisun_criterion_sql(criterion JSONB, data_expression TEXT DEFAULT 'data')
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE SET search_path FROM CURRENT AS $$
DECLARE
    field_key TEXT;
    field_value JSONB;
    predicates JSONB;
    predicate JSONB;
    operator_name TEXT;
    sql_operator TEXT;
    target TEXT;
    scalar_expression TEXT;
    parts TEXT[] := '{}';
BEGIN
    FOR field_key, field_value IN SELECT * FROM jsonb_each(criterion) ORDER BY key LOOP
        IF jsonb_typeof(field_value) = 'string' THEN
            predicates := jsonb_build_array(jsonb_build_object('operator','eq','value',field_value));
        ELSIF jsonb_typeof(field_value) = 'array' AND jsonb_array_length(field_value) > 0 THEN
            predicates := field_value;
        ELSE RAISE EXCEPTION 'invalid tag predicates';
        END IF;
        FOR predicate IN SELECT * FROM jsonb_array_elements(predicates) LOOP
            operator_name := COALESCE(NULLIF(predicate->>'operator',''), 'eq');
            sql_operator := CASE operator_name WHEN 'eq' THEN '=' WHEN 'ne' THEN '<>'
                WHEN 'gt' THEN '>' WHEN 'gte' THEN '>=' WHEN 'lt' THEN '<' WHEN 'lte' THEN '<=' END;
            IF sql_operator IS NULL THEN RAISE EXCEPTION 'unsupported tag operator %', operator_name; END IF;
            target := predicate->>'value';
            scalar_expression := format('(%s ->> %L)', data_expression, field_key);
            IF operator_name IN ('eq','ne') THEN
                parts := parts || format('(%s %s %L)', scalar_expression, sql_operator, target);
            ELSE
                parts := parts || format(
                    '(CASE jsonb_typeof(%s -> %L) WHEN ''string'' THEN (%s COLLATE "C" %s %L COLLATE "C") WHEN ''number'' THEN (orisun_compare_number(%s, %L) %s 0) ELSE FALSE END)',
                    data_expression, field_key, scalar_expression, sql_operator, target,
                    scalar_expression, target, sql_operator);
            END IF;
        END LOOP;
    END LOOP;
    RETURN CASE WHEN cardinality(parts) = 0 THEN 'TRUE' ELSE '(' || array_to_string(parts, ' AND ') || ')' END;
END;
$$;

-- Canonical envelope encoding is owned by the storage write path.
CREATE OR REPLACE FUNCTION orisun_event_document(payload JSONB, meta JSONB, tx BIGINT, gid BIGINT, wid BIGINT, created TIMESTAMPTZ)
RETURNS JSONB LANGUAGE SQL STABLE AS $$
 SELECT payload || jsonb_build_object(
  '__commitPosition', tx, '__preparePosition', gid,
  '__writeId', CASE WHEN wid IS NULL THEN NULL ELSE tx::TEXT || ':' || wid::TEXT END,
  '__metadata', meta,
  '__dateCreated', regexp_replace(to_char(created AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'), '\.?0+$', '') || 'Z'
 )
$$;

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

    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        write_id BIGINT PRIMARY KEY,
        consistency JSONB NOT NULL CHECK (jsonb_typeof(consistency) = ''array'')
    )', schema_name, boundary_name || '_orisun_es_write');
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        data JSONB NOT NULL CHECK (jsonb_typeof(data) = ''object''),
        pg_xact_id BIGINT,
        transaction_id BIGINT GENERATED ALWAYS AS ((data->>''__commitPosition'')::BIGINT) STORED NOT NULL,
        global_id BIGINT GENERATED ALWAYS AS ((data->>''__preparePosition'')::BIGINT) STORED NOT NULL PRIMARY KEY,
        write_id BIGINT GENERATED ALWAYS AS (NULLIF(split_part(data->>''__writeId'', '':'', 2), '''')::BIGINT) STORED REFERENCES %I.%I(write_id),
        metadata JSONB GENERATED ALWAYS AS (data->''__metadata'') STORED,
        date_created TEXT GENERATED ALWAYS AS (data->>''__dateCreated'') STORED NOT NULL,
        CHECK ((jsonb_typeof(data->''__dateCreated'') = ''string'' AND (data->>''__dateCreated'')::timestamptz IS NOT NULL) IS TRUE)
    )', schema_name, boundary_name || '_orisun_es_event', schema_name, boundary_name || '_orisun_es_write');

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



    -- Create indexes used by latest-position checks and ordered event reads.
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I (transaction_id DESC, global_id DESC)',
                   boundary_name || '_idx_global_order_covering', schema_name, boundary_name || '_orisun_es_event');
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I.%I ((data->>''__eventType''), transaction_id DESC, global_id DESC)',
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

    -- Create the admin event-count cache table.
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        id          VARCHAR(255) PRIMARY KEY,
        event_count BIGINT NOT NULL,
        created_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )', schema_name, boundary_name || '_events_count');

    -- Create the admin/projector checkpoint table.
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        id               VARCHAR(255) PRIMARY KEY,
        name             VARCHAR(255) UNIQUE NOT NULL,
        commit_position  BIGINT NOT NULL,
        prepare_position BIGINT NOT NULL
    )', schema_name, boundary_name || '_projector_checkpoint');

END;
$$ LANGUAGE plpgsql;


-- Retire alternate write implementations when upgrading an existing schema.

-- Single group-commit entry point for prepared event-batch requests. Initial
-- positions are resolved with literal latest-match lookups per criterion.
-- Criteria are grouped once by shape for lookups and event matching.
-- Requests are evaluated in queue order; dependencies use each accepted
-- request's final document before one bulk insert. This preserves arbitrary
-- AND/OR CCC semantics without one event-table query or subtransaction per request.
CREATE OR REPLACE FUNCTION insert_event_requests_v2(
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
    SET search_path FROM CURRENT
AS
$$
DECLARE
    request                  JSONB;
    observation              JSONB;
    criteria                  JSONB;
    request_rejected         BOOLEAN;
    all_criteria              JSONB := '[]'::JSONB;
    criterion_ids             JSONB := '{}'::JSONB;
    criterion_tx_ids          BIGINT[] := '{}'::BIGINT[];
    criterion_gids            BIGINT[] := '{}'::BIGINT[];
    accepted_request_indexes  INT[] := '{}'::INT[];
    accepted_event_indexes    INT[] := '{}'::INT[];
    accepted_global_ids       BIGINT[] := '{}'::BIGINT[];
    request_global_ids        BIGINT[] := '{}'::BIGINT[];
    crit                       JSONB;
    criterion_id               INT;
    criterion_count            INT := 0;
    expected_tx_id            BIGINT;
    expected_gid              BIGINT;
    criterion_tx_id           BIGINT;
    criterion_gid             BIGINT;
    inserted_tx_id            BIGINT;
    inserted_gid              BIGINT;
    last_inserted_gid         BIGINT;
    event_index               INT;
    event_count               INT;
    current_index             INT := 0;
    current_pg_xact_id        BIGINT;
    prefixed_seq_name         TEXT;
    criterion_shape           JSONB;
    shape_criteria            JSONB;
    latest_selects            TEXT[] := '{}'::TEXT[];
    criterion_shapes          JSONB[] := '{}'::JSONB[];
    predicate_selects         TEXT[] := '{}'::TEXT[];
    matching_criterion_id     INT;
    event_record              RECORD;
    event_criterion           JSONB;
    latest_record             RECORD;
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
            COALESCE(request_item -> 'consistency', '[]'::JSONB)
        ) AS observation_items(observation_item)
        CROSS JOIN LATERAL jsonb_array_elements(
            COALESCE(observation_item -> 'query' -> 'criteria', '[]'::JSONB)
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

    -- Group criteria once for persisted lookups and direct event matching,
    -- without rescanning all criteria for each shape.
    IF criterion_count > 0 THEN
        FOR criterion_shape, shape_criteria IN
            SELECT shaped.shape, jsonb_agg(shaped.criterion ORDER BY shaped.criterion::TEXT)
            FROM (
                SELECT criterion, (
                    SELECT jsonb_agg(key ORDER BY key)
                    FROM jsonb_object_keys(criterion) AS keys(key)
                ) AS shape
                FROM jsonb_array_elements(all_criteria) AS criteria_items(criterion)
            ) AS shaped
            GROUP BY shaped.shape
            LOOP
                -- Literal predicates expose event types to the partial-index
                -- planner; each lookup needs only the latest stored match.
                FOR crit IN SELECT value FROM jsonb_array_elements(shape_criteria)
                    LOOP
                        IF EXISTS (SELECT 1 FROM jsonb_each(crit) WHERE jsonb_typeof(value) <> 'string') THEN
                            predicate_selects := predicate_selects || format(
                                'SELECT %s AS id WHERE %s',
                                (criterion_ids ->> crit::TEXT)::INT + 1,
                                orisun_criterion_sql(crit, '$1')
                            );
                        END IF;
                        latest_selects := latest_selects || format(
                            '(SELECT %L::JSONB AS criterion, stored.transaction_id, stored.global_id
                              FROM %I.%I stored
                              WHERE %s
                              ORDER BY stored.transaction_id DESC, stored.global_id DESC
                              LIMIT 1)',
                            crit::TEXT, schema, boundary_name || '_orisun_es_event',
                            orisun_criterion_sql(crit, 'stored.data')
                        );
                    END LOOP;

                criterion_shapes := array_append(criterion_shapes, criterion_shape);
            END LOOP;

        FOR latest_record IN EXECUTE array_to_string(latest_selects, ' UNION ALL ')
            LOOP
                criterion_id :=
                    (criterion_ids ->> latest_record.criterion::TEXT)::INT + 1;
                criterion_tx_ids[criterion_id] := latest_record.transaction_id;
                criterion_gids[criterion_id] := latest_record.global_id;
            END LOOP;


    END IF;

    FOR request IN SELECT value FROM jsonb_array_elements(requests)
        LOOP
            request_rejected := FALSE;
            FOR observation IN
                SELECT value
                FROM jsonb_array_elements(
                    COALESCE(request -> 'consistency', '[]'::JSONB)
                )
                LOOP
                    criteria := observation -> 'query' -> 'criteria';
                    IF criteria IS NULL OR jsonb_typeof(criteria) <> 'array' OR
                       jsonb_array_length(criteria) = 0 THEN
                        RAISE EXCEPTION 'consistency query has no criteria';
                    END IF;

                    expected_tx_id :=
                        (observation -> 'position' ->> 'transaction_id')::BIGINT;
                    expected_gid :=
                        (observation -> 'position' ->> 'global_id')::BIGINT;
                    criterion_tx_id := -1;
                    criterion_gid := -1;

                    FOR crit IN SELECT jsonb_array_elements(criteria)
                        LOOP
                            IF jsonb_typeof(crit) <> 'object' OR crit = '{}'::JSONB THEN
                                RAISE EXCEPTION 'consistency criterion has no tags';
                            END IF;
                            criterion_id := (criterion_ids ->> crit::TEXT)::INT + 1;
                            IF criterion_id IS NULL THEN
                                RAISE EXCEPTION 'consistency criterion was not planned';
                            END IF;
                            IF criterion_tx_ids[criterion_id] > criterion_tx_id OR
                               (
                                   criterion_tx_ids[criterion_id] = criterion_tx_id AND
                                   criterion_gids[criterion_id] > criterion_gid
                               ) THEN
                                criterion_tx_id := criterion_tx_ids[criterion_id];
                                criterion_gid := criterion_gids[criterion_id];
                            END IF;
                        END LOOP;

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
                        request_rejected := TRUE;
                        EXIT;
                    END IF;
                END LOOP;

            IF request_rejected THEN
                CONTINUE;
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
            -- Project each final event document onto each distinct key shape
            -- and look up its criterion ID directly. Work scales with shapes,
            -- not the number of criteria or requests in the batch. Processing
            -- events in order leaves each criterion at its last matching event.
            IF criterion_count > 0 THEN
                FOR event_record IN
                    SELECT request_global_ids[event_ordinality] AS global_id,
                           orisun_event_document(
                               (event_item -> 'data') || jsonb_build_object(
                                   '__eventId', (event_item ->> 'event_id')::UUID,
                                   '__eventType', event_item ->> 'event_type'
                               ),
                               COALESCE(event_item -> 'metadata', '{}'::JSONB),
                               inserted_tx_id, request_global_ids[event_ordinality],
                               inserted_gid, statement_timestamp()
                           ) AS document
                    FROM jsonb_array_elements(request -> 'events') WITH ORDINALITY
                        AS events(event_item, event_ordinality)
                    ORDER BY event_ordinality
                    LOOP
                        FOREACH criterion_shape IN ARRAY criterion_shapes
                            LOOP
                                SELECT jsonb_object_agg(key, event_record.document ->> key)
                                INTO event_criterion
                                FROM jsonb_array_elements_text(criterion_shape) AS keys(key);

                                criterion_id := (criterion_ids ->> event_criterion::TEXT)::INT + 1;
                                IF criterion_id IS NOT NULL THEN
                                    criterion_tx_ids[criterion_id] := inserted_tx_id;
                                    criterion_gids[criterion_id] := event_record.global_id;
                                END IF;
                            END LOOP;
                        IF cardinality(predicate_selects) > 0 THEN
                            FOR matching_criterion_id IN EXECUTE array_to_string(predicate_selects, ' UNION ALL ') USING event_record.document LOOP
                                criterion_tx_ids[matching_criterion_id] := inserted_tx_id;
                                criterion_gids[matching_criterion_id] := event_record.global_id;
                            END LOOP;
                        END IF;
                    END LOOP;
            END IF;

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
            WITH inserted_writes AS (
                INSERT INTO %I.%I (write_id, consistency)
                SELECT MAX(global_id), COALESCE($5 -> request_index -> ''consistency'', ''[]''::JSONB)
                FROM unnest($2::INT[], $4::BIGINT[]) AS accepted(request_index, global_id)
                GROUP BY request_index
                RETURNING write_id
            )
            INSERT INTO %I.%I (pg_xact_id, data)
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
            SELECT $1,
                   orisun_event_document(jsonb_set(
                       COALESCE(
                           request_item -> ''events'' -> accepted.event_index -> ''data'',
                           ''{}''::JSONB
                       ),
                       ''{__eventType}'',
                       to_jsonb(
                           request_item -> ''events'' -> accepted.event_index ->> ''event_type''
                       ),
                       TRUE
                   ) || jsonb_build_object(''__eventId'', (request_item -> ''events'' -> accepted.event_index ->> ''event_id'')::UUID),
                   COALESCE(
                       request_item -> ''events'' -> accepted.event_index -> ''metadata'',
                       ''{}''::JSONB
                   ), accepted.transaction_id, accepted.global_id, inserted_writes.write_id, statement_timestamp())
            FROM accepted_events AS accepted
            JOIN inserted_writes ON inserted_writes.write_id = accepted.transaction_id - 1
            CROSS JOIN LATERAL (
                SELECT $5 -> accepted.request_index AS request_item
            ) AS accepted_requests
            ORDER BY accepted.global_id',
            schema,
            boundary_name || '_orisun_es_write',
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
-- pg_xact_id. Rows with NULL pg_xact_id were restored into this cluster and are treated
-- as visible.

CREATE OR REPLACE FUNCTION get_matching_events_v4(
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
                date_created   TIMESTAMPTZ,
                write_id       TEXT
            )
    LANGUAGE plpgsql
    STABLE
    SET search_path FROM CURRENT
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
    -- an AND of tag predicates.
    IF criteria_array IS NOT NULL THEN
        all_parts := '{}';
        FOR crit IN SELECT jsonb_array_elements(criteria_array)
            LOOP
                IF crit <> '{}'::JSONB THEN
                    all_parts := all_parts || orisun_criterion_sql(crit);
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
        SELECT transaction_id, global_id, (data->>'__eventId')::UUID AS event_id, data->>'__eventType' AS event_type, data - ARRAY(SELECT key FROM jsonb_object_keys(data) AS key WHERE left(key, 2) = '__') AS data, metadata, date_created::timestamptz, COALESCE(data->>'__writeId', '')
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

-- get_latest_by_criteria_v2 returns the newest event matching each requested
-- criterion, all from ONE statement and therefore one PostgreSQL snapshot. The
-- Go caller computes the complete OR query's position as the maximum returned
-- event position and returns that query-level observation to the caller.
--
-- This function returns one row per matching criterion only. Criteria with no
-- matching event are omitted; the Go caller maps missing indexes back to empty
-- LatestCriterionResult entries.
CREATE OR REPLACE FUNCTION get_latest_by_criteria_v2(
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
                date_created   TIMESTAMPTZ,
                write_id       TEXT
            )
    LANGUAGE plpgsql
    STABLE
    SET search_path FROM CURRENT
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
            IF crit = '{}'::JSONB THEN
                RAISE EXCEPTION 'criterion % has no tags', idx;
            END IF;
            selects := selects || format(
                    '(SELECT %s AS criterion_idx, e.transaction_id, e.global_id, (e.data->>''__eventId'')::UUID AS event_id, e.data->>''__eventType'' AS event_type, e.data - ARRAY(SELECT key FROM jsonb_object_keys(e.data) AS key WHERE left(key, 2) = ''__'') AS data, e.metadata, e.date_created::timestamptz, COALESCE(e.data->>''__writeId'', '''') FROM %s e WHERE %s ORDER BY e.transaction_id DESC, e.global_id DESC LIMIT 1)',
                    idx, qualified_table_name, orisun_criterion_sql(crit, 'e.data'));
            idx := idx + 1;
        END LOOP;

    RETURN QUERY EXECUTE array_to_string(selects, ' UNION ALL ');
END;
$$;
