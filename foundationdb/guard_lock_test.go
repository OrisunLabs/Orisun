//go:build foundationdb

package foundationdb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/goccy/go-json"
)

func TestFoundationDBSaveRejectsOversizedBatch(t *testing.T) {
	backend := newTestBackend(t)
	ctx := context.Background()

	big := strings.Repeat("x", maxTransactionBytes+1024)
	_, _, err := backend.Save(ctx, []eventstore.EventWithMapTags{{
		EventId:   "oversized",
		EventType: "Big",
		Data:      map[string]any{"blob": big},
		Metadata:  map[string]any{},
	}}, "test", nil)

	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("expected InvalidArgument for oversized batch, got %v", err)
	}
}

func TestFDBLockMutualExclusionAndRelease(t *testing.T) {
	backend := newTestBackend(t)
	logger := logging.InitializeDefaultLogger(configForTestLogger())

	a := newFDBLockProvider(backend.db, backend.root, logger)
	b := newFDBLockProvider(backend.db, backend.root, logger)

	aCtx, cancelA := context.WithCancel(context.Background())
	if _, err := a.acquireLock(aCtx, "boundary-x"); err != nil {
		t.Fatalf("provider A failed to acquire: %v", err)
	}

	// A holds it: B must be refused while A is live.
	if _, err := b.acquireLock(context.Background(), "boundary-x"); err == nil {
		t.Fatalf("provider B acquired a lock held by A")
	}

	// Re-acquire by the same owner is a no-op success (publisher loop restart).
	if _, err := a.acquireLock(aCtx, "boundary-x"); err != nil {
		t.Fatalf("provider A re-acquire failed: %v", err)
	}

	// Releasing A (ctx cancel) must let B take over.
	cancelA()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := b.acquireLock(context.Background(), "boundary-x"); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("provider B never acquired after A released")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestFDBLockLostLeaseCancelsProtectedContext(t *testing.T) {
	backend := newTestBackend(t)
	logger := logging.InitializeDefaultLogger(configForTestLogger())

	a := newFDBLockProvider(backend.db, backend.root, logger)
	b := newFDBLockProvider(backend.db, backend.root, logger)

	lease, err := a.acquireLock(context.Background(), "boundary-lost")
	if err != nil {
		t.Fatalf("provider A failed to acquire: %v", err)
	}

	replaceLock(t, backend.db, a.lockKey("boundary-lost"), lockValue{
		Owner:           b.ownerID,
		Token:           "replacement",
		ExpiresUnixNano: time.Now().Add(lockLease).UnixNano(),
	})

	if err := lease.Check(context.Background()); err == nil {
		t.Fatalf("expected lost lease check to fail")
	}
	if lease.Context().Err() == nil {
		t.Fatalf("lost lease must cancel protected context")
	}
}

func TestFDBLockStaleReleaseDoesNotClearNewerToken(t *testing.T) {
	backend := newTestBackend(t)
	logger := logging.InitializeDefaultLogger(configForTestLogger())

	a := newFDBLockProvider(backend.db, backend.root, logger)
	lease, err := a.acquireLock(context.Background(), "boundary-stale-release")
	if err != nil {
		t.Fatalf("provider A failed to acquire: %v", err)
	}

	replacement := lockValue{
		Owner:           a.ownerID,
		Token:           "newer-token",
		ExpiresUnixNano: time.Now().Add(lockLease).UnixNano(),
	}
	key := a.lockKey("boundary-stale-release")
	replaceLock(t, backend.db, key, replacement)

	lease.Release()

	raw, err := backend.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		return rt.Get(key).MustGet(), nil
	})
	if err != nil {
		t.Fatalf("read lock: %v", err)
	}
	if raw == nil {
		t.Fatalf("stale release cleared replacement lock")
	}
	var got lockValue
	if err := json.Unmarshal(raw.([]byte), &got); err != nil {
		t.Fatalf("decode lock: %v", err)
	}
	if got.Token != replacement.Token {
		t.Fatalf("replacement token changed: got %q want %q", got.Token, replacement.Token)
	}
}

func TestFDBLockExpiredOwnerFailoverFencesOldLease(t *testing.T) {
	backend := newTestBackend(t)
	logger := logging.InitializeDefaultLogger(configForTestLogger())

	a := newFDBLockProvider(backend.db, backend.root, logger)
	b := newFDBLockProvider(backend.db, backend.root, logger)

	aLease, err := a.acquireLock(context.Background(), "boundary-failover")
	if err != nil {
		t.Fatalf("provider A failed to acquire: %v", err)
	}

	replaceLock(t, backend.db, a.lockKey("boundary-failover"), lockValue{
		Owner:           a.ownerID,
		Token:           aLease.token,
		ExpiresUnixNano: time.Now().Add(-time.Second).UnixNano(),
	})

	bLease, err := b.acquireLock(context.Background(), "boundary-failover")
	if err != nil {
		t.Fatalf("provider B should acquire expired lock: %v", err)
	}
	defer bLease.Release()

	if err := aLease.Check(context.Background()); err == nil {
		t.Fatalf("provider A lease should be fenced after B takeover")
	}
	if aLease.Context().Err() == nil {
		t.Fatalf("provider A protected context should be canceled after fencing")
	}

	aLease.Release()
	raw, err := backend.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		return rt.Get(b.lockKey("boundary-failover")).MustGet(), nil
	})
	if err != nil {
		t.Fatalf("read lock after stale release: %v", err)
	}
	if raw == nil {
		t.Fatalf("stale provider A release cleared provider B lock")
	}
	var got lockValue
	if err := json.Unmarshal(raw.([]byte), &got); err != nil {
		t.Fatalf("decode lock after failover: %v", err)
	}
	if got.Owner != b.ownerID || got.Token != bLease.token {
		t.Fatalf("lock owner after failover = %+v, want provider B token", got)
	}
}

func replaceLock(t *testing.T, db fdb.Database, key fdb.Key, value lockValue) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode lock: %v", err)
	}
	if _, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(key, raw)
		return nil, nil
	}); err != nil {
		t.Fatalf("replace lock: %v", err)
	}
}
