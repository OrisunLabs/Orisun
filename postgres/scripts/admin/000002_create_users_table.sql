-- Initialize Admin Tables Function
--
-- This function creates boundary-prefixed admin tables (users, users_count)
-- for a given boundary in a specified schema.
--
-- Parameters:
--   boundary_name (TEXT): The boundary name to use as a prefix for all tables
--   schema_name (TEXT): The PostgreSQL schema where tables will be created
--
-- Example:
--   SELECT initialize_admin_tables('admin', 'public');
--
-- Creates tables like: public.admin_users, public.admin_users_count

CREATE OR REPLACE FUNCTION initialize_admin_tables(
    boundary_name TEXT,
    schema_name TEXT
) RETURNS VOID AS $$
BEGIN
    -- Validate boundary_name is a valid PostgreSQL identifier
    IF boundary_name ~ '^[^a-zA-Z_]' OR boundary_name ~ '[^a-zA-Z0-9_]' OR length(boundary_name) > 63 THEN
        RAISE EXCEPTION 'Invalid boundary name: %. Must start with letter or underscore, contain only letters/digits/underscores, and be 63 chars or less', boundary_name;
    END IF;

    -- Create users table
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        id UUID PRIMARY KEY NOT NULL,
        name VARCHAR(255) NOT NULL,
        username VARCHAR(255) UNIQUE NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        roles TEXT[] NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )', schema_name, boundary_name || '_users');

    -- Create users_count table
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I.%I (
        id VARCHAR(255) PRIMARY KEY,
        user_count BIGINT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )', schema_name, boundary_name || '_users_count');

    -- Legacy tables stored the count as VARCHAR; convert in place (one-row cache).
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = schema_name
          AND table_name = boundary_name || '_users_count'
          AND column_name = 'user_count'
          AND data_type = 'character varying'
    ) THEN
        EXECUTE format('ALTER TABLE %I.%I ALTER COLUMN user_count TYPE BIGINT USING user_count::BIGINT',
                       schema_name, boundary_name || '_users_count');
    END IF;
END;
$$ LANGUAGE plpgsql;