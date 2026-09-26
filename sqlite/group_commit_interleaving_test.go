package sqlite

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// The oracle reads the original, unsimplified OR query through GetBatch before
// each serial save. Batch acceptance is therefore checked independently of the
// optimized CCC lookup and its criterion reduction, not against another batch.
func TestGroupCommitInterleavingsMatchSerialReads(t *testing.T) {
	for _, seed := range []int64{41719, 20260926, 9917} {
		for _, indexed := range []bool{false, true} {
			for _, batchSize := range []int{1, 7, 128} {
				t.Run(fmt.Sprintf("seed=%d/indexes=%v/batch=%d", seed, indexed, batchSize), func(t *testing.T) {
					reference, refPool, refCleanup := newGCTestSaver(t)
					defer refCleanup()
					saver, pool, cleanup := newGCTestSaver(t)
					defer cleanup()
					for _, p := range []*BoundaryPools{refPool, pool} {
						conn, err := p.Write.Take(t.Context())
						require.NoError(t, err)
						require.NoError(t, sqlitex.Execute(conn, `CREATE TRIGGER fail_insert BEFORE INSERT ON orisun_es_event WHEN json_extract(NEW.data,'$.__eventType') = 'Fail' BEGIN SELECT RAISE(ABORT,'injected interleaving failure'); END`, nil))
						p.Write.Put(conn)
						if indexed {
							admin := NewSqliteAdminDB(map[string]*BoundaryPools{gcBoundary: p}, gcBoundary, saver.logger)
							for _, key := range []string{"tenant", "region", "reference", "value"} {
								require.NoError(t, admin.CreateBoundaryIndex(t.Context(), gcBoundary, key, []orisun.BoundaryIndexField{{JsonKey: key, ValueType: "text"}}, nil, ""))
							}
							require.NoError(t, admin.CreateBoundaryIndex(t.Context(), gcBoundary, "fact_tenant", []orisun.BoundaryIndexField{{JsonKey: "tenant", ValueType: "text"}}, []orisun.BoundaryIndexCondition{{Key: "__eventType", Operator: "=", Value: "Fact"}}, "AND"))
						}
					}
					getter := NewSqliteGetEvents(map[string]*BoundaryPools{gcBoundary: refPool}, reference.logger)
					tag := func(key, value string) orisun.ReadTag { return orisun.ReadTag{Key: key, Value: value} }
					criterion := func(tags ...orisun.ReadTag) orisun.ReadCriterion { return orisun.ReadCriterion{Tags: tags} }
					catalog := []orisun.ReadCriterion{
						criterion(tag("tenant", "a")), criterion(tag("tenant", "b")),
						criterion(tag("tenant", "a"), tag("region", "east")),
						criterion(tag("region", "west"), tag("flag", "true")),
						criterion(tag("reference", "b")), criterion(tag("value", "42")),
						criterion(tag("value", "042")), criterion(tag("flag", "false")),
						criterion(tag("nullable", "null")), criterion(tag("nested", `{"a":1}`)),
						criterion(tag("__eventType", "Fact"), tag("tenant", "b")),
						criterion(tag("quoted'key", "it's")),
					}
					type expectation struct {
						code statuscode.Code
						tx   string
						gid  int64
					}
					var requests []*sqliteSaveRequest
					var expected []expectation
					rng := rand.New(rand.NewSource(seed))
					accepted, conflicts, failed, cancelled := 0, 0, 0, 0
					for n := 0; n < 120; n++ {
						req := &sqliteSaveRequest{ctx: t.Context(), result: make(chan sqliteSaveResult, 1)}
						want := expectation{}
						observations := 1 + rng.Intn(3)
						for j := 0; j < observations; j++ {
							var criteria []orisun.ReadCriterion
							branches := 2 + rng.Intn(3)
							for k := 0; k < branches; k++ {
								criteria = append(criteria, catalog[rng.Intn(len(catalog))])
							}
							// Deliberately overlap observations and include both redundant orders.
							if n%3 == 0 {
								criteria = append(criteria, catalog[0], catalog[2], catalog[0])
							}
							if n%3 == 1 {
								criteria = append(criteria, catalog[2], catalog[0])
							}
							if n%11 == 0 && n > 0 {
								criteria = append(criteria, criterion(tag("__eventId", fmt.Sprintf("r%d-e0", n-1))))
							}
							query := &orisun.Query{}
							for _, c := range criteria {
								q := &orisun.Criterion{}
								for _, v := range c.Tags {
									q.Tags = append(q.Tags, &orisun.Tag{Key: v.Key, Value: v.Value})
								}
								query.Criteria = append(query.Criteria, q)
							}
							rows, err := getter.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: gcBoundary, Query: query, Direction: orisun.Direction_DESC, Count: 1})
							require.NoError(t, err)
							position := orisun.NotExistsPosition()
							if len(rows) > 0 {
								position = orisun.Position{CommitPosition: rows[0].CommitPosition, PreparePosition: rows[0].PreparePosition}
							}
							if n%5 == 1 && j == n%observations {
								if len(rows) > 0 {
									position = orisun.NotExistsPosition()
								} else {
									position.PreparePosition++
								}
								want.code = statuscode.AlreadyExists
							}
							req.consistency = append(req.consistency, orisun.ConsistencyCheck{Criteria: criteria, Position: position})
						}
						count := 1 + rng.Intn(3)
						for e := 0; e < count; e++ {
							data := map[string]any{"tenant": []string{"a", "b", "c"}[rng.Intn(3)], "region": []string{"east", "west"}[rng.Intn(2)], "reference": []string{"a", "b", "c"}[rng.Intn(3)], "flag": rng.Intn(2) == 0, "value": []any{42, "42", "042"}[rng.Intn(3)], "nullable": nil, "nested": map[string]any{"a": 1}, "quoted'key": "it's"}
							encoded, err := json.Marshal(data)
							require.NoError(t, err)
							kind := "Fact"
							if e == count-1 && n%2 == 0 {
								kind = "Other"
							}
							if n%13 == 4 && e == count-1 {
								kind = "Fail"
								if want.code == statuscode.OK {
									want.code = statuscode.Internal
								}
							}
							req.inserts = append(req.inserts, orisun.PreparedEvent{EventId: fmt.Sprintf("r%d-e%d", n, e), EventType: kind, DataJSON: string(encoded), MetadataJSON: "{}"})
						}
						if n%17 == 6 {
							ctx, cancel := context.WithCancel(t.Context())
							cancel()
							req.ctx = ctx
							want.code = statuscode.Canceled
						}
						if want.code == statuscode.OK || want.code == statuscode.Internal {
							tx, gid, err := reference.SavePrepared(t.Context(), req.inserts, gcBoundary, req.consistency)
							require.Equal(t, want.code, statuscode.CodeOf(err), "reference request %d", n)
							if err == nil {
								want.tx, want.gid = tx, gid
								accepted++
							} else {
								failed++
							}
						} else if want.code == statuscode.AlreadyExists {
							conflicts++
						} else {
							cancelled++
						}
						requests = append(requests, req)
						expected = append(expected, want)
					}
					prepareGCWriteContexts(t, requests)
					for start := 0; start < len(requests); start += batchSize {
						saver.runFlush(gcBoundary, pool, requests[start:min(start+batchSize, len(requests))])
					}
					for n, req := range requests {
						got := <-req.result
						require.Equal(t, expected[n].code, statuscode.CodeOf(got.err), "request %d checks=%+v error=%v", n, req.consistency, got.err)
						if got.err == nil {
							require.Equal(t, expected[n].tx, got.transactionID)
							require.Equal(t, expected[n].gid, got.globalID)
						}
					}
					// Compare persisted documents except timestamps, plus exact write contexts
					// and the next sequence value. Rejected events must not leave any residue.
					snapshot := func(p *BoundaryPools) []string {
						conn, err := p.Read.Take(t.Context())
						require.NoError(t, err)
						defer p.Read.Put(conn)
						var rows []string
						require.NoError(t, sqlitex.Execute(conn, `SELECT json_remove(data,'$.__dateCreated') FROM orisun_es_event ORDER BY transaction_id,global_id`, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { rows = append(rows, stmt.ColumnText(0)); return nil }}))
						require.NoError(t, sqlitex.Execute(conn, `SELECT consistency FROM orisun_es_write ORDER BY write_id`, &sqlitex.ExecOptions{ResultFunc: func(stmt *sqlite.Stmt) error { rows = append(rows, stmt.ColumnText(0)); return nil }}))
						return rows
					}
					require.Equal(t, snapshot(refPool), snapshot(pool))
					require.Equal(t, readSeqNextID(t, refPool), readSeqNextID(t, pool))
					require.Positive(t, accepted)
					require.Positive(t, conflicts)
					require.Positive(t, failed)
					require.Positive(t, cancelled)
					t.Logf("accepted=%d conflicts=%d insertion failures=%d canceled=%d", accepted, conflicts, failed, cancelled)
				})
			}
		}
	}
}
