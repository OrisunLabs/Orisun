package sqlite

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	eventstore "github.com/OrisunLabs/Orisun/orisun"

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
	for _, version := range []int{0, 1, 5} {
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

func TestStorageUpgradesV013WithoutRewritingDocuments(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "previous.db"))
	if err := sqlitex.ExecuteScript(conn, eventDDL+`
 INSERT INTO orisun_es_write VALUES(10, '[]');
 INSERT INTO orisun_es_event(data) VALUES
 ('{"__eventId":"a","__eventType":"Created","__commitPosition":11,"__preparePosition":10,"__writeId":"11:10","__dateCreated":"2026-10-09T00:00:00Z","__metadata":{},"account":"a"}'),
 ('{"__eventId":"b","__eventType":"Historical","__commitPosition":1,"__preparePosition":0,"__writeId":null,"__dateCreated":"2026-10-08T00:00:00Z","__metadata":{},"account":"b"}');
 CREATE INDEX custom_account ON orisun_es_event(json_extract(data, '$.account'));
 UPDATE orisun_es_seq SET next_id=100;
 INSERT INTO orisun_boundary_index_metadata(name,fields,conditions,combinator) VALUES('managed_account','[{"JsonKey":"account","ValueType":"text"}]','[]','AND');
 PRAGMA user_version=6;`, nil); err != nil {
		t.Fatal(err)
	}
	ddl, _, err := buildSQLiteBoundaryIndexDDL("managed_account", []eventstore.BoundaryIndexField{{JsonKey: "account", ValueType: "text"}}, nil, "AND")
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlitex.ExecuteTransient(conn, ddl, nil); err != nil {
		t.Fatal(err)
	}
	if err := sqlitex.ExecuteTransient(conn, "SELECT sql FROM sqlite_schema WHERE name='managed_account_idx'", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		ddl = stmt.ColumnText(0)
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	var before []string
	read := func(out *[]string) error {
		return sqlitex.ExecuteTransient(conn, "SELECT data FROM orisun_es_event ORDER BY global_id", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { *out = append(*out, stmt.ColumnText(0)); return nil }})
	}
	if err := read(&before); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := applyMigrations(conn); err != nil {
			t.Fatal(err)
		}
	}
	if connSchemaVersion(t, conn) != eventSchemaVersion {
		t.Fatal("version not upgraded")
	}
	if err := sqlitex.ExecuteTransient(conn, "SELECT sql FROM sqlite_schema WHERE name='managed_account_idx'", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		if stmt.ColumnText(0) != ddl {
			t.Fatal("managed index changed")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}

	var after []string
	if err := read(&after); err != nil {
		t.Fatal(err)
	}
	if strings.Join(before, "\n") != strings.Join(after, "\n") {
		t.Fatal("documents changed")
	}
	if err := sqlitex.ExecuteTransient(conn, "SELECT next_id FROM orisun_es_seq", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		if stmt.ColumnInt64(0) != 100 {
			t.Fatal("sequence changed")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sqlitex.ExecuteTransient(conn, "SELECT consistency FROM orisun_es_write WHERE write_id=10", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		if stmt.ColumnText(0) != "[]" {
			t.Fatal("context changed")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sqlitex.ExecuteTransient(conn, "SELECT count(*) FROM sqlite_schema WHERE name='custom_account'", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		if stmt.ColumnInt64(0) != 1 {
			t.Fatal("index lost")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
}

func TestMetadataUpgradesV013(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "metadata.db"))
	if err := sqlitex.ExecuteScript(conn, metadataDDL+`
 CREATE TABLE orisun_last_published_event_position(position INTEGER);
 INSERT INTO projector_checkpoint VALUES('id', 'projection', 11, 10);
 PRAGMA user_version=1;`, nil); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := applyMetadataMigrations(conn); err != nil {
			t.Fatal(err)
		}
	}
	if connSchemaVersion(t, conn) != metadataSchemaVersion {
		t.Fatal("version not upgraded")
	}
	if err := sqlitex.ExecuteTransient(conn, "SELECT commit_position, prepare_position FROM projector_checkpoint", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		if stmt.ColumnInt64(0) != 11 || stmt.ColumnInt64(1) != 10 {
			t.Fatal("projector cursor changed")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := sqlitex.ExecuteTransient(conn, "SELECT count(*) FROM sqlite_schema WHERE name='orisun_last_published_event_position'", &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		if stmt.ColumnInt64(0) != 0 {
			t.Fatal("publisher state remains")
		}
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
}
