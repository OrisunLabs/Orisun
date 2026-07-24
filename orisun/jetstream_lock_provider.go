//go:build !orisun_embedded

package orisun

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OrisunLabs/Orisun/logging"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	lockBucketName       = "ORISUN_LOCKS"
	lockRetryDelay       = 500 * time.Millisecond
	maxRetries           = 3
	jetStreamLockLease   = 15 * time.Second
	jetStreamLockRenewal = 5 * time.Second
	lockOperationTimeout = 2 * time.Second
	lockValueVersion     = 1
)

type lockKeyValue interface {
	Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error)
	Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error)
	Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error)
	Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error
}

type jetStreamLockConfig struct {
	leaseDuration     time.Duration
	heartbeatInterval time.Duration
	operationTimeout  time.Duration
	retryDelay        time.Duration
	maxRetries        int
}

func defaultJetStreamLockConfig() jetStreamLockConfig {
	return jetStreamLockConfig{
		leaseDuration:     jetStreamLockLease,
		heartbeatInterval: jetStreamLockRenewal,
		operationTimeout:  lockOperationTimeout,
		retryDelay:        lockRetryDelay,
		maxRetries:        maxRetries,
	}
}

type jetStreamLockValue struct {
	Version         int    `json:"version"`
	OwnerID         string `json:"owner_id"`
	Token           string `json:"token"`
	ExpiresUnixNano int64  `json:"expires_unix_nano"`
}

// JetStreamLockProvider implements a renewable, revision-fenced distributed
// lock using NATS JetStream KV.
type JetStreamLockProvider struct {
	bucket  lockKeyValue
	ownerID string
	logger  logging.Logger
	config  jetStreamLockConfig
}

type jetStreamLockLeaseHandle struct {
	provider *JetStreamLockProvider
	name     string
	token    string
	ctx      context.Context
	cancel   context.CancelFunc
	stop     chan struct{}
	lost     chan struct{}
	done     chan struct{}

	revision atomic.Uint64
	expires  atomic.Int64
	stopOnce sync.Once
	lostOnce sync.Once
}

// NewJetStreamLockProvider creates a new JetStreamLockProvider.
func NewJetStreamLockProvider(ctx context.Context, js jetstream.JetStream, logger logging.Logger) (*JetStreamLockProvider, error) {
	bucket, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      lockBucketName,
		Description: "Distributed locks for Orisun",
		Storage:     jetstream.MemoryStorage,
		Replicas:    1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create lock bucket: %w", err)
	}

	return newJetStreamLockProvider(bucket, logger, defaultJetStreamLockConfig()), nil
}

func newJetStreamLockProvider(bucket lockKeyValue, logger logging.Logger, config jetStreamLockConfig) *JetStreamLockProvider {
	return &JetStreamLockProvider{
		bucket:  bucket,
		ownerID: uuid.NewString(),
		logger:  logger,
		config:  config,
	}
}

// Lock acquires the named lock for the lifetime of ctx. Callers that need to
// prove ongoing ownership should use AcquireLock and its returned lease.
func (p *JetStreamLockProvider) Lock(ctx context.Context, lockName string) error {
	_, err := p.AcquireLock(ctx, lockName)
	return err
}

// AcquireLock acquires a renewable, token-fenced lease. An expired lease may be
// replaced with a revision-guarded update.
func (p *JetStreamLockProvider) AcquireLock(ctx context.Context, lockName string) (LockLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	lockID := fmt.Sprint(lockName)
	token := uuid.NewString()
	p.logger.Infof("Locking: %s", lockID)

	for attempt := 0; attempt < p.config.maxRetries; attempt++ {
		now := time.Now()
		value := jetStreamLockValue{
			Version:         lockValueVersion,
			OwnerID:         p.ownerID,
			Token:           token,
			ExpiresUnixNano: now.Add(p.config.leaseDuration).UnixNano(),
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode lock %s: %w", lockID, err)
		}

		revision, err := p.bucket.Create(ctx, lockID, encoded)
		if err == nil {
			return p.startLease(ctx, lockID, token, revision, value.ExpiresUnixNano), nil
		}
		if !errors.Is(err, jetstream.ErrKeyExists) {
			return nil, fmt.Errorf("acquire lock %s: %w", lockID, err)
		}

		entry, getErr := p.bucket.Get(ctx, lockID)
		if getErr != nil {
			if errors.Is(getErr, jetstream.ErrKeyNotFound) || errors.Is(getErr, jetstream.ErrKeyDeleted) {
				continue
			}
			return nil, fmt.Errorf("read existing lock %s: %w", lockID, getErr)
		}
		current, valid := decodeJetStreamLockValue(entry.Value())
		if !valid {
			return nil, fmt.Errorf("lock %s has an unsupported value format", lockID)
		}
		if current.ExpiresUnixNano <= now.UnixNano() {
			revision, err = p.bucket.Update(ctx, lockID, encoded, entry.Revision())
			if err == nil {
				p.logger.Infof("Reclaimed expired lock: %s", lockID)
				return p.startLease(ctx, lockID, token, revision, value.ExpiresUnixNano), nil
			}
			p.logger.Warnf("Failed to reclaim expired lock %s: %v", lockID, err)
		}

		if attempt+1 < p.config.maxRetries {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(p.config.retryDelay):
			}
		}
	}

	return nil, fmt.Errorf("failed to acquire lock %s after %d retries", lockID, p.config.maxRetries)
}

func decodeJetStreamLockValue(raw []byte) (jetStreamLockValue, bool) {
	var value jetStreamLockValue
	if err := json.Unmarshal(raw, &value); err != nil || value.Version != lockValueVersion || value.Token == "" {
		return jetStreamLockValue{}, false
	}
	return value, true
}

func (p *JetStreamLockProvider) startLease(
	ctx context.Context,
	lockName string,
	token string,
	revision uint64,
	expires int64,
) *jetStreamLockLeaseHandle {
	leaseCtx, cancel := context.WithCancel(ctx)
	lease := &jetStreamLockLeaseHandle{
		provider: p,
		name:     lockName,
		token:    token,
		ctx:      leaseCtx,
		cancel:   cancel,
		stop:     make(chan struct{}),
		lost:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	lease.revision.Store(revision)
	lease.expires.Store(expires)
	p.logger.Infof("Lock acquired: %s (revision: %d, owner: %s)", lockName, revision, p.ownerID)
	go p.maintain(ctx, lease)
	return lease
}

func (p *JetStreamLockProvider) maintain(parent context.Context, lease *jetStreamLockLeaseHandle) {
	ticker := time.NewTicker(p.config.heartbeatInterval)
	defer ticker.Stop()
	defer lease.cancel()
	defer close(lease.done)

	for {
		select {
		case <-parent.Done():
			p.releaseRemote(lease)
			return
		case <-lease.stop:
			p.releaseRemote(lease)
			return
		case <-lease.lost:
			return
		case <-ticker.C:
			owned, err := p.renew(lease)
			if err != nil {
				if time.Now().UnixNano() >= lease.expires.Load() {
					p.logger.Warnf("Lock lease expired after renewal failure for %s: %v", lease.name, err)
					lease.markLost()
					return
				}
				p.logger.Warnf("Failed to renew lock %s: %v", lease.name, err)
				continue
			}
			if !owned {
				p.logger.Warnf("Lost lock %s to another owner", lease.name)
				lease.markLost()
				return
			}
		}
	}
}

func (p *JetStreamLockProvider) renew(lease *jetStreamLockLeaseHandle) (bool, error) {
	expires := time.Now().Add(p.config.leaseDuration).UnixNano()
	value := jetStreamLockValue{
		Version:         lockValueVersion,
		OwnerID:         p.ownerID,
		Token:           lease.token,
		ExpiresUnixNano: expires,
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return false, err
	}

	opCtx, cancel := context.WithTimeout(lease.ctx, p.config.operationTimeout)
	defer cancel()
	revision, err := p.bucket.Update(opCtx, lease.name, encoded, lease.revision.Load())
	if err != nil {
		owned, checkErr := p.stillOwns(opCtx, lease)
		if checkErr == nil && !owned {
			return false, nil
		}
		return true, err
	}
	lease.revision.Store(revision)
	lease.expires.Store(expires)
	return true, nil
}

func (p *JetStreamLockProvider) stillOwns(ctx context.Context, lease *jetStreamLockLeaseHandle) (bool, error) {
	entry, err := p.bucket.Get(ctx, lease.name)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
			return false, nil
		}
		return false, err
	}
	value, ok := decodeJetStreamLockValue(entry.Value())
	if !ok {
		return false, nil
	}
	return value.OwnerID == p.ownerID && value.Token == lease.token && value.ExpiresUnixNano > time.Now().UnixNano(), nil
}

func (p *JetStreamLockProvider) releaseRemote(lease *jetStreamLockLeaseHandle) {
	for attempt := 0; attempt < p.config.maxRetries; attempt++ {
		opCtx, cancel := context.WithTimeout(context.WithoutCancel(lease.ctx), p.config.operationTimeout)
		entry, err := p.bucket.Get(opCtx, lease.name)
		if err != nil {
			cancel()
			if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
				p.logger.Infof("Lock released: %s", lease.name)
				return
			}
			p.logger.Warnf("Failed to inspect lock %s before release: %v", lease.name, err)
		} else {
			value, valid := decodeJetStreamLockValue(entry.Value())
			if !valid || value.OwnerID != p.ownerID || value.Token != lease.token {
				cancel()
				p.logger.Infof("Lock %s is no longer owned; skipping release", lease.name)
				return
			}
			err = p.bucket.Delete(opCtx, lease.name, jetstream.LastRevision(entry.Revision()))
			cancel()
			if err == nil || errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
				p.logger.Infof("Lock released: %s", lease.name)
				return
			}
			p.logger.Warnf("Failed to release lock %s: %v", lease.name, err)
		}

		if attempt+1 < p.config.maxRetries {
			time.Sleep(p.config.retryDelay)
		}
	}
	p.logger.Warnf("Lock %s could not be explicitly released; it can be reclaimed after lease expiry", lease.name)
}

func (lease *jetStreamLockLeaseHandle) Context() context.Context {
	return lease.ctx
}

func (lease *jetStreamLockLeaseHandle) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := lease.ctx.Err(); err != nil {
		return err
	}
	if time.Now().UnixNano() >= lease.expires.Load() {
		lease.markLost()
		return fmt.Errorf("lock %s lease expired", lease.name)
	}

	opCtx, cancel := context.WithTimeout(ctx, lease.provider.config.operationTimeout)
	defer cancel()
	owned, err := lease.provider.stillOwns(opCtx, lease)
	if err != nil {
		return fmt.Errorf("check lock %s: %w", lease.name, err)
	}
	if !owned {
		lease.markLost()
		return fmt.Errorf("lock %s lost", lease.name)
	}
	return nil
}

func (lease *jetStreamLockLeaseHandle) Release() {
	lease.stopOnce.Do(func() {
		close(lease.stop)
	})
	<-lease.done
}

func (lease *jetStreamLockLeaseHandle) markLost() {
	lease.lostOnce.Do(func() {
		close(lease.lost)
		lease.cancel()
	})
}
