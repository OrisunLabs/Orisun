package sqlite

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSqliteEventNotifierWakeDelayCoalescesNotifications(t *testing.T) {
	notifier := NewSqliteEventNotifierWithWakeDelay(50 * time.Millisecond)
	signal := notifier.Signal("test")
	defer signal.Stop()

	notifier.Notify("test")
	notifier.Notify("test")

	immediateCtx, immediateCancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer immediateCancel()
	if err := signal.Wait(immediateCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected delayed notification, got %v", err)
	}

	delayedCtx, delayedCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer delayedCancel()
	if err := signal.Wait(delayedCtx); err != nil {
		t.Fatalf("expected delayed notification to fire: %v", err)
	}

	coalescedCtx, coalescedCancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer coalescedCancel()
	if err := signal.Wait(coalescedCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected coalesced notifications to produce one wake, got %v", err)
	}
}
