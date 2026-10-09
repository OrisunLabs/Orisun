//go:build !orisun_embedded

package orisun

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"errors"
	"sync"

	"github.com/stretchr/testify/require"
)

type controlledNotificationSignal struct {
	pending chan struct{}
	stopped atomic.Bool
}

func (s *controlledNotificationSignal) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.pending:
		return nil
	}
}
func (s *controlledNotificationSignal) Stop() { s.stopped.Store(true) }

func TestNotificationRelayPublishesEmptyVersionedHintsAndCoalescesSignals(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	signal := &controlledNotificationSignal{pending: make(chan struct{}, 1)}
	js := &fakeNotificationPublisher{}
	result := make(chan error, 1)
	go func() {
		result <- relayBoundaryNotifications(ctx, js, "orders", signal, contextLockLease{ctx: ctx}, noopLogger{})
	}()
	require.Eventually(t, func() bool { return js.publishedCount() == 1 }, time.Second, time.Millisecond)
	// Queue a coalesced burst while the relay is idle.
	signal.pending <- struct{}{}
	require.Eventually(t, func() bool { return js.publishedCount() == 2 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	require.True(t, signal.stopped.Load())
	js.mu.Lock()
	defer js.mu.Unlock()
	for i, payload := range js.published {
		require.Empty(t, payload, "no event data, IDs, metadata, or positions may reach NATS")
		require.Equal(t, GetNotificationSubjectName("orders"), js.subjects[i])
	}
}

func TestNotificationRelayRetainsPendingHintAcrossPublishFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	signal := &controlledNotificationSignal{pending: make(chan struct{}, 1)}
	js := &fakeNotificationPublisher{failFirst: 2}
	result := make(chan error, 1)
	go func() {
		result <- relayBoundaryNotifications(ctx, js, "orders", signal, contextLockLease{ctx: ctx}, noopLogger{})
	}()
	require.Eventually(t, func() bool { return js.publishedCount() == 1 }, 2*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	require.True(t, signal.stopped.Load())
	js.mu.Lock()
	defer js.mu.Unlock()
	require.Equal(t, 3, js.attempts)
}

func TestNotificationRelayCancellationDuringPublishBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	signal := &controlledNotificationSignal{pending: make(chan struct{}, 1)}
	js := &fakeNotificationPublisher{failFirst: 100}
	result := make(chan error, 1)
	go func() {
		result <- relayBoundaryNotifications(ctx, js, "orders", signal, contextLockLease{ctx: ctx}, noopLogger{})
	}()
	require.Eventually(t, func() bool { js.mu.Lock(); defer js.mu.Unlock(); return js.attempts > 0 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("publish retry did not cancel")
	}
	require.True(t, signal.stopped.Load())
}

type fakeNotificationPublisher struct {
	mu        sync.Mutex
	attempts  int
	failFirst int
	published [][]byte
	subjects  []string
}

func (p *fakeNotificationPublisher) Publish(subject string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts++
	if p.attempts <= p.failFirst {
		return errors.New("unavailable")
	}
	p.published = append(p.published, payload)
	p.subjects = append(p.subjects, subject)
	return nil
}
func (p *fakeNotificationPublisher) publishedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

func TestNotificationManagerStopReleasesSignal(t *testing.T) {
	signal := &controlledNotificationSignal{pending: make(chan struct{}, 1)}
	manager := StartNotificationRelays(t.Context(), newContextOnlyLockProvider(), &fakeNotificationPublisher{}, func(string) EventSignal { return signal }, noopLogger{})
	require.NoError(t, manager.StartBoundary("orders"))
	require.Eventually(t, func() bool { return manager.conn.(*fakeNotificationPublisher).publishedCount() > 0 }, time.Second, time.Millisecond)
	manager.Stop()
	require.True(t, signal.stopped.Load())
	require.Error(t, manager.StartBoundary("orders"))
}
