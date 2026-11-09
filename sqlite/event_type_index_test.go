package sqlite

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/OrisunLabs/Orisun/orisun"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Use the production read/CCC renderer to prove the built-in index remains
// usable for both absent event types and runtime-keyed membership lookups.
func TestBuiltInEventTypeIndexForFreshAndUpgradedStorage(t *testing.T) {
	for _, version := range []int{0, 6, 7, 8} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), "events.db"))
			if version != 0 {
				if err := sqlitex.ExecuteScript(conn, legacyEventDDL+fmt.Sprintf("PRAGMA user_version=%d;", version), nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := applyMigrations(conn); err != nil {
				t.Fatal(err)
			}
			if err := sqlitex.ExecuteScript(conn, `INSERT INTO orisun_es_write VALUES(1, '[]'); INSERT INTO orisun_es_event(global_id,transaction_id,write_id,event_id,event_type,date_created,metadata,data) VALUES(1,1,1,'id','Posted','2026-10-12T00:00:00Z','{}','{"accountBalances.account-a.accountCreatedId":"account-a"}');`, nil); err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"Posted", "Absent"} {
				criteria := []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "__eventType", Value: kind}, {Key: "accountBalances.account-a.accountCreatedId", Value: "account-a"}}}}
				queries, err := latestCriteriaQueries(criteria)
				if err != nil {
					t.Fatal(err)
				}
				for _, query := range queries {
					var plan []string
					err := sqlitex.ExecuteTransient(conn, "EXPLAIN QUERY PLAN "+query, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { plan = append(plan, stmt.ColumnText(3)); return nil }})
					if err != nil {
						t.Fatal(err)
					}
					detail := strings.Join(plan, " ")
					if !strings.Contains(detail, "SEARCH orisun_es_event USING INDEX idx_event_type_order") || strings.Contains(detail, "TEMP B-TREE") {
						t.Fatalf("unindexed production lookup: %s", detail)
					}
				}
				tx, gid, err := latestCriteriaPosition(conn, criteria)
				if err != nil {
					t.Fatal(err)
				}
				want := int64(-1)
				if kind == "Posted" {
					want = 1
				}
				if tx != want || gid != want {
					t.Fatalf("position = (%d,%d), want (%d,%d)", tx, gid, want, want)
				}
			}
		})
	}
}
