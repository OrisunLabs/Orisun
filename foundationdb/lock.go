//go:build foundationdb

package foundationdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OrisunLabs/Orisun/logging"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

const (
	// lockLease is how long an acquired lock stays valid without a renewal. If
	// the holder crashes, another node may take the lock once the lease expires —
	// this is what gives the FoundationDB backend crash failover that the
	// in-memory NATS lock (no TTL) cannot.
	lockLease = 15 * time.Second
	// lockHeartbeat renews a held lease well inside lockLease so a brief stall
	// does not lose the lock.
	lockHeartbeat = 5 * time.Second
)

// fdbLockProvider is a lease-based distributed lock stored in FoundationDB. It
// replaces the in-memory NATS JetStream lock for the FDB backend: durable
// (survives a NATS or process restart) and self-healing (a dead holder's lease
// expires, so publishing fails over to another node).
type fdbLockProvider struct {
	db      fdb.Database
	root    string
	ownerID string
	logger  logging.Logger

	mu   sync.Mutex
	held map[string]*fdbLockLease
}

type fdbLockLease struct {
	provider *fdbLockProvider
	name     string
	token    string
	ctx      context.Context
	cancel   context.CancelFunc
	stop     chan struct{}
	once     sync.Once
	expires  atomic.Int64
}

type lockValue struct {
	Owner           string `json:"owner"`
	Token           string `json:"token"`
	ExpiresUnixNano int64  `json:"expires_unix_nano"`
}

func newFDBLockProvider(db fdb.Database, root string, logger logging.Logger) *fdbLockProvider {
	return &fdbLockProvider{
		db:      db,
		root:    root,
		ownerID: uuid.NewString(),
		held:    make(map[string]*fdbLockLease),
		logger:  logger,
	}
}

func (p *fdbLockProvider) lockKey(name string) fdb.Key {
	return fdb.Key(tuple.Tuple{p.root, "lock", name}.Pack())
}

// Lock acquires the named lock for the lifetime of ctx. A lock already held by a
// different live owner returns an error so the caller backs off and retries; an
// expired or self-owned lease is taken over. On success a heartbeat renews the
// lease until ctx is done, then the lock is released.
func (p *fdbLockProvider) Lock(ctx context.Context, lockName string) error {
	_, err := p.AcquireLock(ctx, lockName)
	return err
}

// AcquireLock returns a token-fenced lease. The caller should use the lease
// context for protected work and call Check before externally visible effects.
func (p *fdbLockProvider) AcquireLock(ctx context.Context, lockName string) (*fdbLockLease, error) {
	if err := contextStatusErr(ctx); err != nil {
		return nil, err
	}

	p.mu.Lock()
	if held, ok := p.held[lockName]; ok && held.ctx.Err() == nil {
		// Already held by this process (e.g. the publisher re-acquiring after a
		// transient loop restart on the same ctx). The heartbeat keeps it live.
		p.mu.Unlock()
		return held, nil
	}
	if held, ok := p.held[lockName]; ok && held.ctx.Err() != nil {
		delete(p.held, lockName)
	}
	p.mu.Unlock()

	token := uuid.NewString()
	acquired, expires, err := p.tryAcquire(lockName, token)
	if err != nil {
		return nil, fmt.Errorf("acquire lock %s: %w", lockName, err)
	}
	if !acquired {
		return nil, fmt.Errorf("lock %s is held by another node", lockName)
	}

	leaseCtx, cancel := context.WithCancel(ctx)
	h := &fdbLockLease{
		provider: p,
		name:     lockName,
		token:    token,
		ctx:      leaseCtx,
		cancel:   cancel,
		stop:     make(chan struct{}),
	}
	h.expires.Store(expires)
	p.mu.Lock()
	if held, ok := p.held[lockName]; ok && held.ctx.Err() == nil {
		p.mu.Unlock()
		cancel()
		p.releaseRemote(lockName, token)
		return held, nil
	}
	p.held[lockName] = h
	p.mu.Unlock()

	p.logger.Infof("Lock acquired: %s (owner %s)", lockName, p.ownerID)
	go p.maintain(ctx, lockName, h)
	return h, nil
}

// tryAcquire takes the lock when it is vacant, expired, or already ours. The
// Get adds a read conflict on the lock key, so two nodes racing for an expired
// lease cannot both win — the loser conflicts, retries, and sees the fresh lease.
func (p *fdbLockProvider) tryAcquire(lockName, token string) (bool, int64, error) {
	res, err := p.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		now := time.Now().UnixNano()
		raw := tr.Get(p.lockKey(lockName)).MustGet()
		if raw != nil {
			var cur lockValue
			if err := json.Unmarshal(raw, &cur); err != nil {
				return false, err
			}
			if cur.Owner != p.ownerID && cur.ExpiresUnixNano > now {
				return lockValue{}, nil
			}
		}
		expires := now + int64(lockLease)
		next := lockValue{Owner: p.ownerID, Token: token, ExpiresUnixNano: expires}
		value, err := json.Marshal(next)
		if err != nil {
			return lockValue{}, err
		}
		tr.Set(p.lockKey(lockName), value)
		return next, nil
	})
	if err != nil {
		return false, 0, err
	}
	value := res.(lockValue)
	if value.Token == "" {
		return false, 0, nil
	}
	return true, value.ExpiresUnixNano, nil
}

// maintain renews the lease on a timer and releases the lock when ctx ends or
// the lease is lost to another owner.
func (p *fdbLockProvider) maintain(ctx context.Context, lockName string, h *fdbLockLease) {
	ticker := time.NewTicker(lockHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			p.release(lockName, h)
			return
		case <-h.stop:
			return
		case <-ticker.C:
			ok, expires, err := p.renew(h)
			if err != nil {
				p.logger.Warnf("Failed to renew lock %s: %v", lockName, err)
				continue
			}
			if !ok {
				// Lease lost or expired. Cancel protected work before allowing any
				// later Lock call to re-contend.
				p.logger.Warnf("Lost lock %s to another owner", lockName)
				p.forget(lockName, h)
				return
			}
			h.expires.Store(expires)
		}
	}
}

func (p *fdbLockProvider) renew(h *fdbLockLease) (bool, int64, error) {
	res, err := p.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		now := time.Now().UnixNano()
		raw := tr.Get(p.lockKey(h.name)).MustGet()
		if raw == nil {
			return lockValue{}, nil
		}
		var cur lockValue
		if err := json.Unmarshal(raw, &cur); err != nil {
			return lockValue{}, err
		}
		if cur.Owner != p.ownerID || cur.Token != h.token || cur.ExpiresUnixNano <= now {
			return lockValue{}, nil
		}
		next := lockValue{Owner: p.ownerID, Token: h.token, ExpiresUnixNano: now + int64(lockLease)}
		value, err := json.Marshal(next)
		if err != nil {
			return lockValue{}, err
		}
		tr.Set(p.lockKey(h.name), value)
		return next, nil
	})
	if err != nil {
		return false, 0, err
	}
	value := res.(lockValue)
	if value.Token == "" {
		return false, 0, nil
	}
	return true, value.ExpiresUnixNano, nil
}

// release clears the lock key if we still own it, then forgets it locally.
func (p *fdbLockProvider) release(lockName string, h *fdbLockLease) {
	p.releaseRemote(lockName, h.token)
	p.forget(lockName, h)
}

func (p *fdbLockProvider) releaseRemote(lockName, token string) {
	_, err := p.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		raw := tr.Get(p.lockKey(lockName)).MustGet()
		if raw == nil {
			return nil, nil
		}
		var cur lockValue
		if err := json.Unmarshal(raw, &cur); err != nil {
			return nil, err
		}
		if cur.Owner == p.ownerID && cur.Token == token {
			tr.Clear(p.lockKey(lockName))
		}
		return nil, nil
	})
	if err != nil {
		p.logger.Warnf("Failed to release lock %s: %v", lockName, err)
	} else {
		p.logger.Infof("Lock released: %s", lockName)
	}
}

func (p *fdbLockProvider) forget(lockName string, h *fdbLockLease) {
	h.once.Do(func() {
		h.cancel()
		close(h.stop)
	})
	p.mu.Lock()
	if p.held[lockName] == h {
		delete(p.held, lockName)
	}
	p.mu.Unlock()
}

func (h *fdbLockLease) Context() context.Context {
	return h.ctx
}

func (h *fdbLockLease) Check(ctx context.Context) error {
	if err := contextStatusErr(ctx); err != nil {
		return err
	}
	if err := contextStatusErr(h.ctx); err != nil {
		return err
	}
	ok, err := h.provider.stillOwns(h)
	if err != nil {
		return fmt.Errorf("check lock %s: %w", h.name, err)
	}
	if !ok {
		h.provider.logger.Warnf("Lost lock %s before protected operation", h.name)
		h.provider.forget(h.name, h)
		return fmt.Errorf("lock %s lost", h.name)
	}
	return nil
}

func (h *fdbLockLease) Release() {
	h.provider.release(h.name, h)
}

func (p *fdbLockProvider) stillOwns(h *fdbLockLease) (bool, error) {
	res, err := p.db.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {
		raw := rt.Get(p.lockKey(h.name)).MustGet()
		if raw == nil {
			return false, nil
		}
		var cur lockValue
		if err := json.Unmarshal(raw, &cur); err != nil {
			return false, err
		}
		return cur.Owner == p.ownerID && cur.Token == h.token && cur.ExpiresUnixNano > time.Now().UnixNano(), nil
	})
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}
