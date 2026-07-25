package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func openMigrationTestConn(t *testing.T, dbPath string) *sqlite.Conn {
	t.Helper()
	conn, err := sqlite.OpenConn(dbPath, sqlite.OpenReadWrite, sqlite.OpenCreate)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func connSchemaVersion(t *testing.T, conn *sqlite.Conn) int {
	t.Helper()
	version, err := schemaVersion(conn)
	if err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	return version
}

func TestMigrationsStampFreshDatabase(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "fresh.db"))

	if err := applyMigrations(conn); err != nil {
		t.Fatalf("apply event migrations: %v", err)
	}
	if got, want := connSchemaVersion(t, conn), len(eventMigrations); got != want {
		t.Fatalf("user_version = %d, want %d", got, want)
	}

	var found bool
	err := sqlitex.Execute(conn,
		"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'orisun_es_event'",
		&sqlitex.ExecOptions{ResultFunc: func(*sqlite.Stmt) error {
			found = true
			return nil
		}})
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	if !found {
		t.Fatal("orisun_es_event table missing after migrations")
	}
}

func TestMigrationsAdoptPreVersioningDatabase(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "legacy.db"))

	// Simulate a database created before versioning: baseline schema present,
	// user_version still 0, existing rows in place.
	if err := sqlitex.ExecuteScript(conn, eventDDL, nil); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	err := sqlitex.Execute(conn,
		`INSERT INTO orisun_es_event (transaction_id, global_id, event_id, data) VALUES (1, 1, 'e-1', '{"eventType":"Legacy"}')`,
		nil)
	if err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	if got := connSchemaVersion(t, conn); got != 0 {
		t.Fatalf("pre-migration user_version = %d, want 0", got)
	}

	if err := applyMigrations(conn); err != nil {
		t.Fatalf("apply event migrations: %v", err)
	}
	if got, want := connSchemaVersion(t, conn), len(eventMigrations); got != want {
		t.Fatalf("user_version = %d, want %d", got, want)
	}

	var count int64
	err = sqlitex.Execute(conn, "SELECT COUNT(*) FROM orisun_es_event", &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			count = stmt.ColumnInt64(0)
			return nil
		}})
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("legacy row count = %d, want 1", count)
	}
}

func TestMigrationsRefuseNewerSchema(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "future.db"))

	if err := sqlitex.ExecuteTransient(conn, "PRAGMA user_version = 999", nil); err != nil {
		t.Fatalf("set future version: %v", err)
	}
	err := applyMigrations(conn)
	if err == nil {
		t.Fatal("expected error opening database with future schema version")
	}
	if !strings.Contains(err.Error(), "newer than this binary") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMigrationStepFailureRollsBackAtomically(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "partial.db"))

	steps := []string{
		"CREATE TABLE step_one (id INTEGER PRIMARY KEY);",
		// Second statement fails after the first succeeds: the whole step,
		// including the version bump, must roll back.
		"CREATE TABLE step_two (id INTEGER PRIMARY KEY);\nCREATE TABLE step_one (id INTEGER PRIMARY KEY);",
	}
	err := applyVersionedMigrations(conn, steps)
	if err == nil {
		t.Fatal("expected step 2 to fail")
	}
	if !strings.Contains(err.Error(), "migration step 2") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := connSchemaVersion(t, conn); got != 1 {
		t.Fatalf("user_version = %d, want 1 (step 2 rolled back)", got)
	}

	var stepTwoExists bool
	qerr := sqlitex.Execute(conn,
		"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'step_two'",
		&sqlitex.ExecOptions{ResultFunc: func(*sqlite.Stmt) error {
			stepTwoExists = true
			return nil
		}})
	if qerr != nil {
		t.Fatalf("query sqlite_master: %v", qerr)
	}
	if stepTwoExists {
		t.Fatal("step_two table exists despite failed step")
	}

	// A rerun with the step fixed resumes from where it left off.
	steps[1] = "CREATE TABLE step_two (id INTEGER PRIMARY KEY);"
	if err := applyVersionedMigrations(conn, steps); err != nil {
		t.Fatalf("rerun after fix: %v", err)
	}
	if got := connSchemaVersion(t, conn); got != 2 {
		t.Fatalf("user_version = %d, want 2", got)
	}
}

func TestOpenBoundaryPoolsStampsSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	bp, err := OpenBoundaryPools(context.Background(), dir, "verstamp", "verstamp")
	if err != nil {
		t.Fatalf("open pools: %v", err)
	}
	defer bp.Close()

	conn, err := bp.Read.Take(context.Background())
	if err != nil {
		t.Fatalf("take read conn: %v", err)
	}
	defer bp.Read.Put(conn)
	if got, want := connSchemaVersion(t, conn), len(eventMigrations); got != want {
		t.Fatalf("user_version = %d, want %d", got, want)
	}
}
