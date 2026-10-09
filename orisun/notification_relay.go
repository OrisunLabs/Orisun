//go:build !orisun_embedded

package orisun

import (
	"context"
	"fmt"
	"sync"
	"time"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	"github.com/OrisunLabs/Orisun/logging"
)

// GetNotificationSubjectName identifies transient boundary change hints.
func GetNotificationSubjectName(boundary string) string {
	return "ORISUN_NOTIFICATIONS___" + boundary + ".changed.v1"
}

type notificationPublisher interface{ Publish(string, []byte) error }

// BoundaryNotificationManager forwards backend signals. It deliberately has
// no event retriever or publisher-checkpoint dependency.
type BoundaryNotificationManager struct {
	ctx            context.Context
	cancel         context.CancelFunc
	lockProvider   LockProvider
	conn           notificationPublisher
	signalProvider func(string) EventSignal
	logger         logging.Logger
	mu             sync.Mutex
	running        map[string]struct{}
	stopped        bool
	wg             sync.WaitGroup
}

func StartNotificationRelays(ctx context.Context, lockProvider LockProvider, conn notificationPublisher, signalProvider func(string) EventSignal, logger logging.Logger) *BoundaryNotificationManager {
	relayCtx, cancel := context.WithCancel(ctx)
	return &BoundaryNotificationManager{ctx: relayCtx, cancel: cancel, lockProvider: lockProvider, conn: conn, signalProvider: signalProvider, logger: logger, running: make(map[string]struct{})}
}

func (m *BoundaryNotificationManager) StartBoundary(boundary string) error {
	if m == nil || m.ctx == nil || m.lockProvider == nil || m.conn == nil || m.logger == nil {
		return fmt.Errorf("boundary notification manager is not configured")
	}
	if err := boundarymodel.ValidateName(boundary); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.ctx.Err() != nil {
		return fmt.Errorf("boundary notification manager has stopped")
	}
	if _, exists := m.running[boundary]; exists {
		return nil
	}
	// Without backend notifications, subscription idle watchdogs supply hints.
	if m.signalProvider == nil {
		return nil
	}
	m.running[boundary] = struct{}{}
	m.wg.Add(1)
	go func() { defer m.wg.Done(); m.runBoundary(boundary) }()
	return nil
}

// Stop cancels relays and waits for their signals and leases to be released.
func (m *BoundaryNotificationManager) Stop() {
	// Cancel before waiting for relay ownership and signal cleanup.
	m.cancel()
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()
	m.wg.Wait()
}

func (m *BoundaryNotificationManager) runBoundary(boundary string) {
	backoff := Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	for m.ctx.Err() == nil {
		lease, err := m.lockProvider.AcquireLock(m.ctx, boundary)
		if err == nil {
			signal := m.signalProvider(boundary)
			if signal == nil {
				err = fmt.Errorf("nil notification signal for %s", boundary)
			} else {
				err = relayBoundaryNotifications(lease.Context(), m.conn, boundary, signal, lease, m.logger)
			}
			lease.Release()
		}
		if m.ctx.Err() != nil {
			return
		}
		m.logger.Warnf("Boundary notification relay stopped for %s: %v", boundary, err)
		if err := backoff.Wait(m.ctx); err != nil {
			return
		}
	}
}

func relayBoundaryNotifications(ctx context.Context, conn notificationPublisher, boundary string, signal EventSignal, lease LockLease, logger logging.Logger) error {
	defer signal.Stop()
	backoff := Backoff{Base: 100 * time.Millisecond, Max: 2 * time.Second}
	for {
		// Publish once immediately on ownership, then once per coalesced signal.
		// Retain pending work across publish failures without touching storage.
		for {
			if err := lease.Check(ctx); err != nil {
				return err
			}
			err := conn.Publish(GetNotificationSubjectName(boundary), nil)
			if err == nil {
				backoff.Reset()
				break
			}
			logger.Warnf("Failed to publish boundary notification for %s: %v", boundary, err)
			if err := backoff.Wait(ctx); err != nil {
				return err
			}
		}
		if err := signal.Wait(ctx); err != nil {
			return err
		}
	}
}
