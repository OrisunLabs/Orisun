package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/OrisunLabs/Orisun/orisun/grpcapi"
)

// runLedgerWorkload drives a double-entry general ledger — the canonical CCC
// workload — through the public gRPC API, so the same test runs against every
// backend:
//
//   - every event carries the account's resulting balance, so a command reads
//     only the LATEST event per account (Count=1 DESC through the index), not
//     the whole history — the CCC check is what makes the carried balance
//     trustworthy, because any concurrent write to either account invalidates
//     the save,
//   - every transfer appends a TransferDebited + TransferCredited pair in ONE
//     SaveEvents batch (atomicity across two accounts),
//   - the consistency context is "either account touched" (two OR'd criteria),
//   - workers race on a small account set and retry on ALREADY_EXISTS.
//
// Afterwards it audits the full event log for the invariants a real ledger
// cannot violate:
//
//  1. every event's carried balance equals the balance recomputed by replay —
//     a mismatch means a command read a stale balance that CCC should have
//     rejected,
//  2. no account's running balance ever goes negative at any point in its
//     history,
//  3. money is conserved: final balances sum to the seeded total,
//  4. every transfer id appears as exactly one debit and one credit of equal
//     amount (batch atomicity),
//  5. per-account criteria reads return strictly increasing positions.
func runLedgerWorkload(t *testing.T, suite *E2ETestSuite) {
	t.Helper()
	runLedgerWorkloadWithConfig(t, suite, ledgerWorkloadConfig{
		accounts:           8,
		initialBalance:     1000,
		workers:            6,
		transfersPerWorker: 15,
		maxAttempts:        200,
	})
}

type ledgerWorkloadConfig struct {
	accounts           int
	initialBalance     int64
	workers            int
	transfersPerWorker int
	maxAttempts        int
}

func runLedgerWorkloadWithConfig(t *testing.T, suite *E2ETestSuite, cfg ledgerWorkloadConfig) {
	t.Helper()
	ctx := createAuthenticatedContext("admin", "changeit")
	client := suite.eventStoreClient
	const boundary = "orisun_test_1"

	accountID := func(i int) string { return fmt.Sprintf("acct-%02d", i) }
	accountCriteria := func(ids ...string) *pb.Query {
		criteria := make([]*pb.Criterion, len(ids))
		for i, id := range ids {
			criteria[i] = &pb.Criterion{Tags: []*pb.Tag{{Key: "account_id", Value: id}}}
		}
		return &pb.Query{Criteria: criteria}
	}

	// Covering index on account_id through the public CreateIndex RPC. The
	// FoundationDB backend requires it for criteria reads and consistency
	// checks; PostgreSQL and SQLite use it as a partial btree index.
	_, err := client.CreateIndex(ctx, &pb.CreateIndexRequest{
		Boundary: boundary,
		Name:     "account",
		Fields:   []*pb.IndexField{{JsonKey: "account_id", ValueType: pb.ValueType_TEXT}},
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	type entry struct {
		eventType string
		accountID string
		amount    int64
		balance   int64
		transfer  string
	}
	decodeEntry := func(event *pb.Event) entry {
		data := map[string]any{}
		if err := json.Unmarshal([]byte(event.Data), &data); err != nil {
			t.Fatalf("decode event %s: %v", event.EventId, err)
		}
		amount, _ := data["amount"].(float64)
		balance, _ := data["balance"].(float64)
		account, _ := data["account_id"].(string)
		transfer, _ := data["transfer_id"].(string)
		return entry{
			eventType: event.EventType,
			accountID: account,
			amount:    int64(amount),
			balance:   int64(balance),
			transfer:  transfer,
		}
	}
	apply := func(balance int64, e entry) int64 {
		switch e.eventType {
		case "AccountOpened", "TransferCredited":
			return balance + e.amount
		case "TransferDebited":
			return balance - e.amount
		default:
			return balance
		}
	}
	mustJSON := func(v map[string]any) string {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal event data: %v", err)
		}
		return string(raw)
	}

	// Seed accounts: consistency context "this account is still empty".
	notExists := pb.Position{CommitPosition: -1, PreparePosition: -1}
	for i := 0; i < cfg.accounts; i++ {
		_, err := client.SaveEventsV2(ctx, &pb.SaveEventsV2Request{
			Boundary: boundary,
			Consistency: []*pb.ConsistencyObservation{{
				Position: &notExists,
				Query:    accountCriteria(accountID(i)),
			}},
			Events: []*pb.EventToSave{{
				EventId:   uuid.NewString(),
				EventType: "AccountOpened",
				Data: mustJSON(map[string]any{
					"account_id": accountID(i),
					"amount":     cfg.initialBalance,
					"balance":    cfg.initialBalance,
				}),
				Metadata: `{}`,
			}},
		})
		if err != nil {
			t.Fatalf("open account %d: %v", i, err)
		}
	}

	// readContext fetches each account's latest event — whose carried balance
	// IS the account state — through GetLatestByCriteria, which assembles the
	// whole context from ONE server-side read snapshot and returns the
	// context_position to use as the optimistic-lock token. Assembling the
	// same context from independent GetEvents calls is not equivalent: an
	// event can commit between the calls with a position below the observed
	// maximum, and a scalar expected position cannot detect that.
	readContext := func(src, dst string) (srcBal, dstBal int64, last *pb.Position, err error) {
		resp, err := client.GetLatestByCriteria(ctx, &pb.GetLatestByCriteriaRequest{
			Boundary: boundary,
			Criteria: accountCriteria(src, dst).Criteria,
		})
		if err != nil {
			return 0, 0, nil, err
		}
		if len(resp.Results) != 2 || resp.Results[0].Event == nil || resp.Results[1].Event == nil {
			return 0, 0, nil, fmt.Errorf("context for %s/%s incomplete: %v", src, dst, resp.Results)
		}
		srcBal = decodeEntry(resp.Results[0].Event).balance
		dstBal = decodeEntry(resp.Results[1].Event).balance
		return srcBal, dstBal, resp.ContextPosition, nil
	}

	var transferred, declined, conflicts int64
	var wg sync.WaitGroup
	errs := make(chan error, cfg.workers)
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 42))
			for i := 0; i < cfg.transfersPerWorker; i++ {
				src := accountID(rng.Intn(cfg.accounts))
				dst := accountID(rng.Intn(cfg.accounts))
				for dst == src {
					dst = accountID(rng.Intn(cfg.accounts))
				}
				amount := int64(1 + rng.Intn(50))

				attempts := 0
				for {
					attempts++
					if attempts > cfg.maxAttempts {
						errs <- fmt.Errorf("worker %d transfer %d: gave up after %d attempts (livelock?)", w, i, cfg.maxAttempts)
						return
					}
					srcBal, dstBal, last, err := readContext(src, dst)
					if err != nil {
						errs <- fmt.Errorf("read context: %w", err)
						return
					}
					if srcBal < amount {
						// Business decline: no event appended.
						atomic.AddInt64(&declined, 1)
						break
					}
					transferID := uuid.NewString()
					_, err = client.SaveEventsV2(ctx, &pb.SaveEventsV2Request{
						Boundary: boundary,
						Consistency: []*pb.ConsistencyObservation{{
							Position: last,
							Query:    accountCriteria(src, dst),
						}},
						Events: []*pb.EventToSave{
							{
								EventId:   uuid.NewString(),
								EventType: "TransferDebited",
								Data: mustJSON(map[string]any{
									"account_id":  src,
									"amount":      amount,
									"balance":     srcBal - amount,
									"transfer_id": transferID,
								}),
								Metadata: `{}`,
							},
							{
								EventId:   uuid.NewString(),
								EventType: "TransferCredited",
								Data: mustJSON(map[string]any{
									"account_id":  dst,
									"amount":      amount,
									"balance":     dstBal + amount,
									"transfer_id": transferID,
								}),
								Metadata: `{}`,
							},
						},
					})
					if status.Code(err) == codes.AlreadyExists {
						// Lost the race on one of the two accounts; re-read and retry.
						atomic.AddInt64(&conflicts, 1)
						continue
					}
					if err != nil {
						errs <- fmt.Errorf("transfer save: %w", err)
						return
					}
					atomic.AddInt64(&transferred, 1)
					break
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("transfers executed=%d declined=%d optimistic conflicts retried=%d", transferred, declined, conflicts)

	// Audit the complete log in position order through paged GetEvents.
	var all []*pb.Event
	var cursor *pb.Position
	for {
		req := &pb.GetEventsRequest{Boundary: boundary, Count: 500, Direction: pb.Direction_ASC}
		if cursor != nil {
			req.FromPosition = &pb.Position{
				CommitPosition:  cursor.CommitPosition,
				PreparePosition: cursor.PreparePosition + 1,
			}
		}
		resp, err := client.GetEvents(ctx, req)
		if err != nil {
			t.Fatalf("audit read: %v", err)
		}
		if len(resp.Events) == 0 {
			break
		}
		all = append(all, resp.Events...)
		cursor = resp.Events[len(resp.Events)-1].Position
	}
	wantEvents := cfg.accounts + int(transferred)*2
	if len(all) != wantEvents {
		t.Fatalf("expected %d events in log, got %d", wantEvents, len(all))
	}

	// Invariants 1-3: replay running balances in order; every carried balance
	// must match the replayed truth, never go negative, and money is conserved.
	balances := map[string]int64{}
	type leg struct {
		debits, credits int
		amount          int64
	}
	legs := map[string]*leg{}
	for _, event := range all {
		e := decodeEntry(event)
		balances[e.accountID] = apply(balances[e.accountID], e)
		if e.balance != balances[e.accountID] {
			for _, ev := range all {
				d := decodeEntry(ev)
				if d.accountID == e.accountID {
					t.Logf("  %s pos=(%d,%d) type=%s amount=%d carried=%d transfer=%s",
						d.accountID, ev.Position.CommitPosition, ev.Position.PreparePosition,
						d.eventType, d.amount, d.balance, d.transfer)
				}
			}
			t.Fatalf("account %s event at %v carries balance %d but replay says %d — stale read slipped past CCC",
				e.accountID, event.Position, e.balance, balances[e.accountID])
		}
		if balances[e.accountID] < 0 {
			t.Fatalf("account %s running balance went negative at position %v — overdraft slipped past CCC", e.accountID, event.Position)
		}
		if e.transfer != "" {
			l := legs[e.transfer]
			if l == nil {
				l = &leg{amount: e.amount}
				legs[e.transfer] = l
			}
			if e.amount != l.amount {
				t.Fatalf("transfer %s legs disagree on amount: %d vs %d", e.transfer, e.amount, l.amount)
			}
			switch e.eventType {
			case "TransferDebited":
				l.debits++
			case "TransferCredited":
				l.credits++
			}
		}
	}
	var total int64
	for account, balance := range balances {
		if balance < 0 {
			t.Fatalf("account %s final balance negative: %d", account, balance)
		}
		total += balance
	}
	if want := int64(cfg.accounts) * cfg.initialBalance; total != want {
		t.Fatalf("money not conserved: total %d, want %d", total, want)
	}

	// Invariant 3: every transfer has exactly one debit and one credit.
	if len(legs) != int(transferred) {
		t.Fatalf("expected %d transfer ids, got %d", transferred, len(legs))
	}
	for id, l := range legs {
		if l.debits != 1 || l.credits != 1 {
			t.Fatalf("transfer %s has %d debits / %d credits, want 1/1", id, l.debits, l.credits)
		}
	}

	// Invariant 5: per-account criteria reads come back in strictly increasing
	// position order.
	for i := 0; i < cfg.accounts; i++ {
		resp, err := client.GetEvents(ctx, &pb.GetEventsRequest{
			Boundary:  boundary,
			Count:     10000,
			Direction: pb.Direction_ASC,
			Query:     accountCriteria(accountID(i)),
		})
		if err != nil {
			t.Fatalf("account %d history: %v", i, err)
		}
		for j := 1; j < len(resp.Events); j++ {
			if cmpPositions(resp.Events[j].Position, resp.Events[j-1].Position) <= 0 {
				t.Fatalf("account %s history out of order at %d", accountID(i), j)
			}
		}
	}
}

func cmpPositions(a, b *pb.Position) int {
	if a.CommitPosition == b.CommitPosition && a.PreparePosition == b.PreparePosition {
		return 0
	}
	if a.CommitPosition < b.CommitPosition || (a.CommitPosition == b.CommitPosition && a.PreparePosition < b.PreparePosition) {
		return -1
	}
	return 1
}

// TestE2E_GetLatestByCriteria_SQLite pins the RPC contract: one result per
// criterion in request order, unset event for unmatched criteria, and
// context_position = max observed position (not-exists sentinel when nothing
// matched).
func TestE2E_GetLatestByCriteria_SQLite(t *testing.T) {
	suite := setupSQLiteE2ETest(t)
	defer suite.teardown(t)
	ctx := createAuthenticatedContext("admin", "changeit")
	client := suite.eventStoreClient
	const boundary = "orisun_test_2"

	criteria := []*pb.Criterion{
		{Tags: []*pb.Tag{{Key: "entity", Value: "a"}}},
		{Tags: []*pb.Tag{{Key: "entity", Value: "b"}}},
	}

	// Nothing matches yet.
	resp, err := client.GetLatestByCriteria(ctx, &pb.GetLatestByCriteriaRequest{Boundary: boundary, Criteria: criteria})
	if err != nil {
		t.Fatalf("GetLatestByCriteria (empty): %v", err)
	}
	if len(resp.Results) != 2 || resp.Results[0].Event != nil || resp.Results[1].Event != nil {
		t.Fatalf("expected 2 empty results, got %v", resp.Results)
	}
	if resp.ContextPosition.CommitPosition != -1 || resp.ContextPosition.PreparePosition != -1 {
		t.Fatalf("expected not-exists context position, got %v", resp.ContextPosition)
	}

	// Two events for entity a, one for entity b; latest-per-criterion plus
	// context_position = the newest of them all.
	save := func(entity, eventType string) *pb.Position {
		res, err := client.SaveEventsV2(ctx, &pb.SaveEventsV2Request{
			Boundary: boundary,
			Events: []*pb.EventToSave{{
				EventId:   uuid.NewString(),
				EventType: eventType,
				Data:      fmt.Sprintf(`{"entity": %q}`, entity),
				Metadata:  `{}`,
			}},
		})
		if err != nil {
			t.Fatalf("save %s/%s: %v", entity, eventType, err)
		}
		return res.LogPosition
	}
	save("a", "A1")
	bPos := save("b", "B1")
	a2Pos := save("a", "A2")

	resp, err = client.GetLatestByCriteria(ctx, &pb.GetLatestByCriteriaRequest{Boundary: boundary, Criteria: criteria})
	if err != nil {
		t.Fatalf("GetLatestByCriteria: %v", err)
	}
	if got := resp.Results[0].Event; got == nil || got.EventType != "A2" {
		t.Fatalf("expected latest for entity a = A2, got %v", got)
	}
	if got := resp.Results[1].Event; got == nil || got.EventType != "B1" {
		t.Fatalf("expected latest for entity b = B1, got %v", got)
	}
	if cmpPositions(resp.Results[1].Event.Position, bPos) != 0 {
		t.Fatalf("entity b position mismatch: %v vs %v", resp.Results[1].Event.Position, bPos)
	}
	if cmpPositions(resp.ContextPosition, a2Pos) != 0 {
		t.Fatalf("context position should be the newest observed (%v), got %v", a2Pos, resp.ContextPosition)
	}

	// A criterion with no tags is rejected.
	_, err = client.GetLatestByCriteria(ctx, &pb.GetLatestByCriteriaRequest{
		Boundary: boundary,
		Criteria: []*pb.Criterion{{}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected INVALID_ARGUMENT for empty criterion, got %v", err)
	}
}

func TestE2E_LedgerWorkload_SQLite(t *testing.T) {
	suite := setupSQLiteE2ETest(t)
	defer suite.teardown(t)
	runLedgerWorkload(t, suite)
}

func TestE2E_LedgerWorkload_Postgres(t *testing.T) {
	suite := setupE2ETest(t)
	defer suite.teardown(t)
	runLedgerWorkload(t, suite)
}
