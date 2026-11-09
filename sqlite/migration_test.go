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

func execScript(t *testing.T, conn *sqlite.Conn, script string) {
	t.Helper()
	if err := sqlitex.ExecuteScript(conn, script, nil); err != nil {
		t.Fatal(err)
	}
}

func queryStrings(t *testing.T, conn *sqlite.Conn, query string) []string {
	t.Helper()
	var rows []string
	if err := sqlitex.ExecuteTransient(conn, query, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error {
		rows = append(rows, stmt.ColumnText(0))
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestStorageUpgradeSplitsEnvelopeFromDocuments(t *testing.T) {
	for _, version := range []int{6, 7, 8} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "previous.db"))
			execScript(t, conn, legacyEventDDL+`
 INSERT INTO orisun_es_write VALUES(10, '[]');
 INSERT INTO orisun_es_event(data) VALUES
 ('{"__eventId":"a","__eventType":"Created","__commitPosition":11,"__preparePosition":10,"__writeId":"11:10","__dateCreated":"2026-10-09T00:00:00Z","__metadata":{"source":"api"},"account":"a","big":9007199254740993,"ratio":0.123456789012345678901,"text":"caf\u00e9"}'),
 ('{"__eventId":"b","__eventType":"Historical","__commitPosition":1,"__preparePosition":0,"__writeId":null,"__dateCreated":"2026-10-08T00:00:00Z","__metadata":null,"account":"b"}');
 CREATE INDEX custom_account ON orisun_es_event(json_extract(data, '$.account'));
 UPDATE orisun_es_seq SET next_id=100;
 INSERT INTO orisun_boundary_index_metadata(name,fields,conditions,combinator) VALUES('managed_account','[{"JsonKey":"account","ValueType":"text"}]','[]','AND');
 INSERT INTO orisun_boundary_index_metadata(name,fields,conditions,combinator) VALUES('created_accounts','[{"JsonKey":"account","ValueType":"text"}]','[{"Key":"__eventType","Operator":"=","Value":"Created"}]','AND');
`+fmt.Sprintf("PRAGMA user_version=%d;", version))
			execScript(t, conn, "PRAGMA temp_store = MEMORY;")
			for range 2 {
				if err := applyMigrations(conn); err != nil {
					t.Fatal(err)
				}
			}
			// The upgrade spills its sorts to disk, then restores the configured setting.
			if got := queryStrings(t, conn, "PRAGMA temp_store"); got[0] != "2" {
				t.Fatalf("temp_store not restored: %v", got)
			}
			if connSchemaVersion(t, conn) != eventSchemaVersion {
				t.Fatal("version not upgraded")
			}
			got := queryStrings(t, conn, "SELECT json_array(global_id, transaction_id, write_id, event_id, event_type, date_created, metadata) || data FROM orisun_es_event ORDER BY global_id")
			want := []string{
				`[0,1,null,"b","Historical","2026-10-08T00:00:00Z","null"]{"account":"b"}`,
				`[10,11,10,"a","Created","2026-10-09T00:00:00Z","{\"source\":\"api\"}"]{"account":"a","big":9007199254740993,"ratio":0.123456789012345678901,"text":"caf\u00e9"}`,
			}
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("rows:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
			}
			if got := queryStrings(t, conn, "SELECT next_id FROM orisun_es_seq"); got[0] != "100" {
				t.Fatalf("sequence changed: %v", got)
			}
			if got := queryStrings(t, conn, "SELECT consistency FROM orisun_es_write WHERE write_id=10"); got[0] != "[]" {
				t.Fatalf("context changed: %v", got)
			}
			indexes := strings.Join(queryStrings(t, conn, "SELECT name FROM sqlite_schema WHERE type='index' AND tbl_name='orisun_es_event' ORDER BY name"), ",")
			if indexes != "created_accounts_idx,custom_account,idx_event_position,idx_event_type_order,managed_account_idx" {
				t.Fatalf("indexes: %s", indexes)
			}
			// A managed index on an envelope key must match its freshly built form.
			ddl, _, err := buildSQLiteBoundaryIndexDDL("created_accounts", []eventstore.BoundaryIndexField{{JsonKey: "account", ValueType: "text"}}, []eventstore.BoundaryIndexCondition{{Key: "__eventType", Operator: "=", Value: "Created"}}, "AND")
			if err != nil {
				t.Fatal(err)
			}
			if err := verifyBoundaryIndexDefinition(conn, "created_accounts", ddl); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(queryStrings(t, conn, "SELECT sql FROM sqlite_schema WHERE name='created_accounts_idx'")[0], "WHERE event_type = 'Created'") {
				t.Fatal("managed condition still reads the document")
			}
		})
	}
}

func TestStorageUpgradeRefusesUnrepresentableEventsWithoutChangingData(t *testing.T) {
	for name, document := range map[string]string{
		"unknown reserved field": `{"__eventId":"a","__eventType":"T","__commitPosition":1,"__preparePosition":1,"__writeId":null,"__dateCreated":"2026-10-09T00:00:00Z","__metadata":{},"__custom":1}`,
		"foreign write id":       `{"__eventId":"a","__eventType":"T","__commitPosition":1,"__preparePosition":1,"__writeId":"9:1","__dateCreated":"2026-10-09T00:00:00Z","__metadata":{}}`,
		"missing event id":       `{"__eventType":"T","__commitPosition":1,"__preparePosition":1,"__writeId":null,"__dateCreated":"2026-10-09T00:00:00Z","__metadata":{}}`,
	} {
		t.Run(name, func(t *testing.T) {
			conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "previous.db"))
			execScript(t, conn, legacyEventDDL+"INSERT INTO orisun_es_write VALUES(1, '[]'); INSERT INTO orisun_es_event(data) VALUES('"+document+"'); PRAGMA user_version=8; PRAGMA temp_store=MEMORY;")
			if err := applyMigrations(conn); err == nil {
				t.Fatal("upgrade accepted unrepresentable event")
			}
			if got := queryStrings(t, conn, "PRAGMA temp_store"); got[0] != "2" {
				t.Fatalf("temp_store not restored: %v", got)
			}
			if got := connSchemaVersion(t, conn); got != 8 {
				t.Fatalf("version changed to %d", got)
			}
			if got := queryStrings(t, conn, "SELECT data FROM orisun_es_event"); len(got) != 1 || got[0] != document {
				t.Fatalf("document changed: %v", got)
			}
			if got := queryStrings(t, conn, "SELECT name FROM sqlite_schema WHERE name='orisun_es_event_v9'"); len(got) != 0 {
				t.Fatal("temporary table remained")
			}
		})
	}
}

// v0.14.3 and earlier binaries reject this version instead of misreading rows.
func TestFreshSchemaIsNotTheLegacyDocumentFormat(t *testing.T) {
	conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "fresh.db"))
	if err := applyMigrations(conn); err != nil {
		t.Fatal(err)
	}
	if connSchemaVersion(t, conn) <= 8 {
		t.Fatal("split storage must not share a legacy version")
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
