//go:build !orisun_embedded

package orisun

import (
	"context"
	"fmt"
	"sync"
	"time"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	natsgo "github.com/nats-io/nats.go"
)

const defaultSubscriptionIdleThreshold = time.Second * 10

// ConfigureSubscriptions configures an embedded server before subscriptions or
// boundary provisioning start.
func (s *OrisunServer) ConfigureSubscriptions(interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("subscriptions require a positive notification idle threshold")
	}
	s.eventStore.subscriptionIdleThreshold = interval
	return nil
}

func (s *EventStore) SubscribeToAllEvents(ctx context.Context, request coreeventstore.SubscribeRequest, handler coreeventstore.EventHandler) error {
	if handler == nil {
		return statuscode.New(statuscode.InvalidArgument, "event handler is required")
	}
	if err := s.RequireBoundaryActive(request.Boundary); err != nil {
		return err
	}
	query := subscriptionQuery(request.Query)
	if err := ValidateQuery(query); err != nil {
		return err
	}
	if s.js == nil || s.js.Conn() == nil {
		return statuscode.New(statuscode.FailedPrecondition, "subscription notification connection is required")
	}
	subscriptionCtx, cancelSubscription := context.WithCancel(ctx)
	defer cancelSubscription()
	lease, err := s.lockProvider.AcquireLock(subscriptionCtx, request.Boundary+"__"+request.SubscriberName)
	if err != nil {
		return statuscode.Errorf(statuscode.AlreadyExists, "failed to acquire lock: %v", err)
	}
	defer lease.Release()
	workCtx, cancelWork := context.WithCancel(lease.Context())
	defer cancelWork()
	drain := newSubscriptionDrain(s.getEventsFn, request.Boundary, query, subscriptionPosition(request.AfterPosition), handler, func(ctx context.Context) error {
		if err := s.RequireBoundaryActive(request.Boundary); err != nil {
			return err
		}
		return lease.Check(ctx)
	})

	// Register before the first backend read. Setup failures retry registration;
	// there is no independent backend polling path.
	activity := newSubscriptionHintActivity()
	onHint := func() { activity.received(time.Now()); drain.wake() }
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.receiveSubscriptionNotifications(workCtx, request.Boundary, onHint, activity)
	}()
	defer func() {
		cancelWork()
		<-done
	}()

	return drain.run(workCtx)
}

// A Core NATS callback records only a coalesced wake-up. Event payloads and
// delivery progress remain entirely in the backend drain.
func (s *EventStore) openSubscriptionNotifications(ctx context.Context, boundary string, wake func()) (*natsgo.Subscription, error) {
	conn := s.js.Conn()
	subscription, err := conn.Subscribe(GetNotificationSubjectName(boundary), func(*natsgo.Msg) { wake() })
	if err != nil {
		return nil, err
	}
	setupCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := conn.FlushWithContext(setupCtx); err != nil {
		_ = subscription.Unsubscribe()
		return nil, err
	}
	return subscription, nil
}

func (s *EventStore) receiveSubscriptionNotifications(ctx context.Context, boundary string, onHint func(), activity *subscriptionHintActivity) {
	conn := s.js.Conn()
	// Core NATS restores subscriptions automatically after connection recovery.
	// Watch status without replacing the connection's application callbacks.
	connected := conn.StatusChanged(natsgo.CONNECTED)
	defer conn.RemoveStatusListener(connected)
	backoff := Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	var listener *natsgo.Subscription
	for ctx.Err() == nil {
		var err error
		listener, err = s.openSubscriptionNotifications(ctx, boundary, onHint)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		s.logger.Warnf("Subscription notification registration failed for %s: %v", boundary, err)
		if err := backoff.Wait(ctx); err != nil {
			return
		}
	}
	if listener == nil {
		return
	}
	defer listener.Unsubscribe()
	// Startup, reconnect, and idle recovery all publish the same hint. Only
	// listener receipt records activity and wakes the backend drain.
	ping := func() {
		if ctx.Err() != nil {
			return
		}
		if err := conn.Publish(GetNotificationSubjectName(boundary), nil); err != nil {
			s.logger.Warnf("Subscription notification ping failed for %s: %v", boundary, err)
		}
	}
	ping()
	threshold := s.subscriptionIdleThreshold
	if threshold <= 0 {
		threshold = defaultSubscriptionIdleThreshold
	}
	timer := time.NewTimer(threshold)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-activity.changed:
			timer.Reset(activity.remaining(time.Now(), threshold))
		case <-timer.C:
			remaining := activity.remaining(time.Now(), threshold)
			if remaining > 0 {
				timer.Reset(remaining)
				continue
			}
			ping()
			// Do not advance lastReceived on publish: receipt is the liveness signal.
			timer.Reset(threshold)
		case _, ok := <-connected:
			if !ok {
				connected = nil
				continue
			}
			ping()
		}
	}
}

// Activity belongs to each subscription, even when many share one boundary.
// time.Time preserves monotonic elapsed time across wall-clock adjustments.
type subscriptionHintActivity struct {
	mu           sync.Mutex
	lastReceived time.Time
	changed      chan struct{}
}

func newSubscriptionHintActivity() *subscriptionHintActivity {
	return &subscriptionHintActivity{lastReceived: time.Now(), changed: make(chan struct{}, 1)}
}
func (a *subscriptionHintActivity) received(now time.Time) {
	a.mu.Lock()
	a.lastReceived = now
	a.mu.Unlock()
	select {
	case a.changed <- struct{}{}:
	default:
	}
}
func (a *subscriptionHintActivity) remaining(now time.Time, threshold time.Duration) time.Duration {
	a.mu.Lock()
	elapsed := now.Sub(a.lastReceived)
	a.mu.Unlock()
	if elapsed >= threshold {
		return 0
	}
	return threshold - elapsed
}
