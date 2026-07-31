package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
)

//go:embed scripts/common/*.sql
var sqlScripts embed.FS

//go:embed scripts/admin/*.sql
var adminSqlScripts embed.FS

// RunDbScripts initializes database tables for a specific boundary.
// It validates the boundary name, creates the schema if needed, and calls
// the PostgreSQL initialization functions to create boundary-prefixed tables.
func RunDbScripts(db *sql.DB, boundary string, schema string, isAdminSchema bool, ctx context.Context) error {
	// Validate boundary name
	if err := validateBoundaryName(boundary); err != nil {
		return fmt.Errorf("invalid boundary name %s: %w", boundary, err)
	}

	// Create schema if not exists (idempotent - safe to call multiple times)
	if schema != "public" {
		if err := createSchemaIfNotExists(db, schema, ctx); err != nil {
			return fmt.Errorf("failed to create schema %s: %w", schema, err)
		}
	}

	// Load and execute SQL scripts to define the initialization functions
	// These scripts define initialize_boundary_tables() and initialize_admin_tables()
	// We need to load them once per schema, but since we're calling them per-boundary,
	// we just ensure the functions exist first
	if err := ensureFunctionsDefined(db, schema, ctx); err != nil {
		return fmt.Errorf("failed to ensure functions are defined: %w", err)
	}

	// Initialize boundary-specific tables by calling the PostgreSQL functions
	if err := initializeBoundaryTables(db, boundary, schema, isAdminSchema, ctx); err != nil {
		fmt.Printf("Error: %v", err)
		return fmt.Errorf("failed to initialize tables for boundary %s: %w", boundary, err)
	}

	return nil
}

// validateBoundaryName checks if a boundary name is a valid PostgreSQL identifier.
// PostgreSQL identifiers:
// - Must start with a letter (a-z, A-Z) or underscore (_)
// - Can contain letters, digits (0-9), and underscores
// - Maximum length is 63 characters
func validateBoundaryName(boundary string) error {
	if len(boundary) == 0 || len(boundary) > 63 {
		return fmt.Errorf("boundary name must be 1-63 characters, got %d", len(boundary))
	}

	firstChar := boundary[0]
	if !isLetter(firstChar) && firstChar != '_' {
		return fmt.Errorf("boundary name must start with a letter or underscore, got '%c'", firstChar)
	}

	for i := 1; i < len(boundary); i++ {
		c := boundary[i]
		if !isLetter(c) && !isDigit(c) && c != '_' {
			return fmt.Errorf("boundary name can only contain letters, digits, and underscores, invalid char '%c' at position %d", c, i)
		}
	}

	return nil
}

// isLetter checks if a byte is an ASCII letter (a-z or A-Z)
func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isDigit checks if a byte is an ASCII digit (0-9)
func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// createSchemaIfNotExists creates a PostgreSQL schema if it doesn't already exist
func createSchemaIfNotExists(db *sql.DB, schema string, ctx context.Context) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Use schema parameter as-is since we're creating it, not accessing it
	// The caller is responsible for validating schema names
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema)); err != nil {
		return fmt.Errorf("failed to create schema %s: %w", schema, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit schema creation: %w", err)
	}

	return nil
}

// ensureFunctionsDefined ensures the SQL functions (initialize_boundary_tables, etc.)
// are defined in the specified schema. This loads the SQL scripts that define these functions.
func ensureFunctionsDefined(db *sql.DB, schema string, ctx context.Context) error {
	// We need to execute the SQL scripts that define the functions
	// but only execute the function definitions, not the table creation (since we do that per-boundary)

	// For now, we'll load the scripts and execute them - they contain CREATE OR REPLACE FUNCTION
	// which is idempotent and safe to run multiple times
	// The scripts also contain CREATE TABLE IF NOT EXISTS which we're replacing with our functions

	// Load common scripts (contains initialize_boundary_tables function)
	if err := executeSQLScripts(db, sqlScripts, schema, ctx); err != nil {
		return fmt.Errorf("failed to execute common SQL scripts: %w", err)
	}

	return nil
}

// executeSQLScripts loads and executes SQL scripts from an embedded filesystem
func executeSQLScripts(db *sql.DB, scripts embed.FS, schema string, ctx context.Context) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Set search path for the transaction
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL search_path TO %s", schema)); err != nil {
		return fmt.Errorf("failed to set search path: %w", err)
	}

	// Execute the SQL scripts which define our functions
	// These use CREATE OR REPLACE FUNCTION so they're idempotent
	var scriptContent string
	if scripts == sqlScripts {
		// For common scripts, we read and execute the file
		content, err := sqlScripts.ReadFile("scripts/common/db_scripts_1.sql")
		if err != nil {
			return fmt.Errorf("failed to read db_scripts_1.sql: %w", err)
		}
		scriptContent = string(content)
	} else if scripts == adminSqlScripts {
		// For admin scripts, read the main file (users table)
		content, err := adminSqlScripts.ReadFile("scripts/admin/000002_create_users_table.sql")
		if err != nil {
			return fmt.Errorf("failed to read admin SQL script: %w", err)
		}
		scriptContent = string(content)
	} else {
		return fmt.Errorf("unknown script filesystem")
	}

	if _, err := tx.ExecContext(ctx, scriptContent); err != nil {
		return fmt.Errorf("failed to execute SQL scripts: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

// initializeBoundaryTables calls the PostgreSQL initialization functions
// to create all boundary-prefixed tables for a given boundary
func initializeBoundaryTables(db *sql.DB, boundary string, schema string, isAdminSchema bool, ctx context.Context) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Set search path to ensure PostgreSQL can find the tables
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL search_path TO %s", schema)); err != nil {
		return fmt.Errorf("failed to set search path: %w", err)
	}

	// Call the PostgreSQL function to create prefixed tables
	// This function validates the boundary name and creates all tables
	_, err = tx.ExecContext(ctx, "SELECT initialize_boundary_tables($1, $2)", boundary, schema)
	if err != nil {
		return fmt.Errorf("failed to initialize boundary tables: %w", err)
	}

	// If admin boundary, also initialize admin tables
	if isAdminSchema {
		// First ensure admin scripts are loaded
		if err := executeSQLScripts(db, adminSqlScripts, schema, ctx); err != nil {
			return fmt.Errorf("failed to load admin scripts: %w", err)
		}

		_, err = tx.ExecContext(ctx, "SELECT initialize_admin_tables($1, $2)", boundary, schema)
		if err != nil {
			return fmt.Errorf("failed to initialize admin tables: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}
