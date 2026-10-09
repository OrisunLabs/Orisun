package sqlite

import (
	"context"
	"fmt"
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
	if got, want := connSchemaVersion(t, conn), eventSchemaVersion; got != want {
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

// Simulate a database created before versioning: baseline schema present,
// user_version still 0, existing rows in place.

func TestMigrationsRefuseNewerSchema(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "future.db"))

	if err := sqlitex.ExecuteTransient(conn, "PRAGMA user_version = 999", nil); err != nil {
		t.Fatalf("set future version: %v", err)
	}
	err := applyMigrations(conn)
	if err == nil {
		t.Fatal("expected error opening database with future schema version")
	}
	if !strings.Contains(err.Error(), "unsupported database schema") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Second statement fails after the first succeeds: the whole step,
// including the version bump, must roll back.

// A rerun with the step fixed resumes from where it left off.

func TestOpenBoundaryPoolsStampsSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	bp, err := OpenBoundaryPools(context.Background(), dir, "verstamp")
	if err != nil {
		t.Fatalf("open pools: %v", err)
	}
	defer bp.Close()

	conn, err := bp.Read.Take(context.Background())
	if err != nil {
		t.Fatalf("take read conn: %v", err)
	}
	defer bp.Read.Put(conn)
	if got, want := connSchemaVersion(t, conn), eventSchemaVersion; got != want {
		t.Fatalf("user_version = %d, want %d", got, want)
	}
}

func TestStorageRejectsOlderFormatsWithoutChangingData(t *testing.T) {
	for _, version := range []int{0, 1, 6} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "old.db"))
			if err := sqlitex.ExecuteScript(conn, fmt.Sprintf("CREATE TABLE orisun_es_event(data TEXT); INSERT INTO orisun_es_event VALUES ('original'); PRAGMA user_version=%d;", version), nil); err != nil {
				t.Fatal(err)
			}
			if err := applyMigrations(conn); err == nil {
				t.Fatal("older storage was accepted")
			}
			if got := connSchemaVersion(t, conn); got != version {
				t.Fatalf("version changed to %d", got)
			}
			err := sqlitex.Execute(conn, "SELECT data FROM orisun_es_event", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
				if stmt.ColumnText(0) != "original" {
					t.Fatal("data changed")
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSchemaInitializationFailureRollsBack(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "failed.db"))
	if err := initializeSchema(conn, "CREATE TABLE example(id INTEGER); INVALID SQL;", eventSchemaVersion); err == nil {
		t.Fatal("expected failure")
	}
	if got := connSchemaVersion(t, conn); got != 0 {
		t.Fatalf("version changed to %d", got)
	}
	var count int64
	if err := sqlitex.ExecuteTransient(conn, "SELECT COUNT(*) FROM sqlite_schema WHERE name='example'", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { count = stmt.ColumnInt64(0); return nil }}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("partial schema remained")
	}
	if err := applyMigrations(conn); err != nil {
		t.Fatal(err)
	}
	if err := applyMigrations(conn); err != nil {
		t.Fatal(err)
	}
}
