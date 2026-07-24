package orisun

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goccy/go-json"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type lockTestLogger struct{}

func (lockTestLogger) IsDebugEnabled() bool  { return false }
func (lockTestLogger) Debug(...any)          {}
func (lockTestLogger) Debugf(string, ...any) {}
func (lockTestLogger) Info(...any)           {}
func (lockTestLogger) Infof(string, ...any)  {}
func (lockTestLogger) Warn(...any)           {}
func (lockTestLogger) Warnf(string, ...any)  {}
func (lockTestLogger) Error(...any)          {}
func (lockTestLogger) Errorf(string, ...any) {}
func (lockTestLogger) Fatal(...any)          {}
func (lockTestLogger) Fatalf(string, ...any) {}

func testJetStreamLockConfig() jetStreamLockConfig {
	return jetStreamLockConfig{
		leaseDuration:     150 * time.Millisecond,
		heartbeatInterval: 30 * time.Millisecond,
		operationTimeout:  250 * time.Millisecond,
		retryDelay:        10 * time.Millisecond,
		maxRetries:        3,
	}
}

func testLockBucket(t *testing.T) jetstream.KeyValue {
	t.Helper()
	srv, err := natsserver.NewServer(&natsserver.Options{
		ServerName: "orisun-lock-test",
		Port:       -1,
		JetStream:  true,
		StoreDir:   t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatal("NATS server did not become ready")
	}

	nc, err := natsgo.Connect("", natsgo.InProcessServer(srv))
	if err != nil {
		srv.Shutdown()
		t.Fatalf("connect to NATS: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		srv.Shutdown()
	})

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("create JetStream context: %v", err)
	}
	bucket, err := js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket:  "ORISUN_LOCK_TEST",
		Storage: jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("create lock bucket: %v", err)
	}
	return bucket
}

func TestJetStreamLockReleaseAllowsImmediateReacquisition(t *testing.T) {
	bucket := testLockBucket(t)
	config := testJetStreamLockConfig()
	a := newJetStreamLockProvider(bucket, lockTestLogger{}, config)
	b := newJetStreamLockProvider(bucket, lockTestLogger{}, config)

	lease, err := a.AcquireLock(context.Background(), "subscription")
	if err != nil {
		t.Fatalf("provider A acquire: %v", err)
	}
	lease.Release()
	lease.Release()

	replacement, err := b.AcquireLock(context.Background(), "subscription")
	if err != nil {
		t.Fatalf("provider B acquire after release: %v", err)
	}
	replacement.Release()
}

func TestJetStreamLockContextCancellationReleases(t *testing.T) {
	bucket := testLockBucket(t)
	config := testJetStreamLockConfig()
	a := newJetStreamLockProvider(bucket, lockTestLogger{}, config)
	b := newJetStreamLockProvider(bucket, lockTestLogger{}, config)

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := a.AcquireLock(ctx, "subscription"); err != nil {
		t.Fatalf("provider A acquire: %v", err)
	}
	cancel()

	deadline := time.Now().Add(time.Second)
	for {
		lease, err := b.AcquireLock(context.Background(), "subscription")
		if err == nil {
			lease.Release()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("provider B did not acquire after cancellation: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestJetStreamLockHeartbeatKeepsLeaseLive(t *testing.T) {
	bucket := testLockBucket(t)
	config := testJetStreamLockConfig()
	a := newJetStreamLockProvider(bucket, lockTestLogger{}, config)
	b := newJetStreamLockProvider(bucket, lockTestLogger{}, config)

	lease, err := a.AcquireLock(context.Background(), "subscription")
	if err != nil {
		t.Fatalf("provider A acquire: %v", err)
	}
	defer lease.Release()

	time.Sleep(3 * config.leaseDuration)
	if err := lease.Check(context.Background()); err != nil {
		t.Fatalf("live lease check: %v", err)
	}
	if _, err := b.AcquireLock(context.Background(), "subscription"); err == nil {
		t.Fatal("provider B acquired a live lease")
	}
}

func TestJetStreamLockExpiredLeaseCanBeReclaimed(t *testing.T) {
	bucket := testLockBucket(t)
	config := testJetStreamLockConfig()
	provider := newJetStreamLockProvider(bucket, lockTestLogger{}, config)

	expired := jetStreamLockValue{
		Version:         lockValueVersion,
		OwnerID:         "dead-owner",
		Token:           "dead-token",
		ExpiresUnixNano: time.Now().Add(-time.Second).UnixNano(),
	}
	raw, err := json.Marshal(expired)
	if err != nil {
		t.Fatalf("encode expired lock: %v", err)
	}
	if _, err := bucket.Create(context.Background(), "subscription", raw); err != nil {
		t.Fatalf("seed expired lock: %v", err)
	}

	lease, err := provider.AcquireLock(context.Background(), "subscription")
	if err != nil {
		t.Fatalf("reclaim expired lock: %v", err)
	}
	lease.Release()
}

func TestJetStreamLockRejectsUnsupportedValueFormat(t *testing.T) {
	bucket := testLockBucket(t)
	config := testJetStreamLockConfig()
	provider := newJetStreamLockProvider(bucket, lockTestLogger{}, config)

	if _, err := bucket.Create(context.Background(), "subscription", []byte(`{"version":0}`)); err != nil {
		t.Fatalf("seed unsupported lock: %v", err)
	}
	if _, err := provider.AcquireLock(context.Background(), "subscription"); err == nil {
		t.Fatal("unsupported lock value was accepted")
	} else if !strings.Contains(err.Error(), "unsupported value format") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJetStreamStaleReleaseDoesNotDeleteReplacement(t *testing.T) {
	bucket := testLockBucket(t)
	config := testJetStreamLockConfig()
	provider := newJetStreamLockProvider(bucket, lockTestLogger{}, config)

	acquired, err := provider.AcquireLock(context.Background(), "subscription")
	if err != nil {
		t.Fatalf("acquire original lock: %v", err)
	}
	original := acquired.(*jetStreamLockLeaseHandle)
	entry, err := bucket.Get(context.Background(), "subscription")
	if err != nil {
		t.Fatalf("get original lock: %v", err)
	}
	replacement := jetStreamLockValue{
		Version:         lockValueVersion,
		OwnerID:         "replacement-owner",
		Token:           "replacement-token",
		ExpiresUnixNano: time.Now().Add(time.Minute).UnixNano(),
	}
	raw, err := json.Marshal(replacement)
	if err != nil {
		t.Fatalf("encode replacement: %v", err)
	}
	if _, err := bucket.Update(context.Background(), "subscription", raw, entry.Revision()); err != nil {
		t.Fatalf("replace lock: %v", err)
	}

	if err := original.Check(context.Background()); err == nil {
		t.Fatal("stale lease still reported ownership")
	}
	if original.Context().Err() == nil {
		t.Fatal("losing ownership did not cancel the lease context")
	}
	original.Release()
	remaining, err := bucket.Get(context.Background(), "subscription")
	if err != nil {
		t.Fatalf("replacement was deleted: %v", err)
	}
	got, ok := decodeJetStreamLockValue(remaining.Value())
	if !ok || got.Token != replacement.Token {
		t.Fatalf("unexpected replacement after stale release: %+v", got)
	}
}

type failingDeleteLockKV struct {
	lockKeyValue
	remaining atomic.Int32
}

func (f *failingDeleteLockKV) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	if f.remaining.Add(-1) >= 0 {
		return errors.New("injected delete failure")
	}
	return f.lockKeyValue.Delete(ctx, key, opts...)
}

func TestJetStreamFailedReleaseRecoversAfterExpiry(t *testing.T) {
	bucket := testLockBucket(t)
	config := testJetStreamLockConfig()
	config.maxRetries = 2
	brokenBucket := &failingDeleteLockKV{lockKeyValue: bucket}
	brokenBucket.remaining.Store(int32(config.maxRetries))
	a := newJetStreamLockProvider(brokenBucket, lockTestLogger{}, config)
	b := newJetStreamLockProvider(bucket, lockTestLogger{}, config)

	lease, err := a.AcquireLock(context.Background(), "subscription")
	if err != nil {
		t.Fatalf("provider A acquire: %v", err)
	}
	lease.Release()

	time.Sleep(config.leaseDuration)
	replacement, err := b.AcquireLock(context.Background(), "subscription")
	if err != nil {
		t.Fatalf("provider B did not reclaim after failed release: %v", err)
	}
	replacement.Release()
}
