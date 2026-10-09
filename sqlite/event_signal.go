package sqlite

import (
	"context"
	"sync"
	"time"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

type SqliteEventNotifier struct {
	wakeDelay time.Duration
	mu        sync.Mutex
	signals   map[string]map[*sqliteEventSignal]struct{}
	timers    map[string]*time.Timer
}

func NewSqliteEventNotifier() *SqliteEventNotifier {
	return NewSqliteEventNotifierWithWakeDelay(0)
}

func NewSqliteEventNotifierWithWakeDelay(wakeDelay time.Duration) *SqliteEventNotifier {
	if wakeDelay < 0 {
		wakeDelay = 0
	}
	return &SqliteEventNotifier{
		wakeDelay: wakeDelay,
		signals:   make(map[string]map[*sqliteEventSignal]struct{}),
		timers:    make(map[string]*time.Timer),
	}
}

func (n *SqliteEventNotifier) Signal(boundary string) eventstore.EventSignal {
	signal := &sqliteEventSignal{
		notifier: n,
		boundary: boundary,
		ch:       make(chan struct{}, 1),
	}
	n.mu.Lock()
	if n.signals[boundary] == nil {
		n.signals[boundary] = make(map[*sqliteEventSignal]struct{})
	}
	n.signals[boundary][signal] = struct{}{}
	n.mu.Unlock()
	return signal
}

func (n *SqliteEventNotifier) Notify(boundary string) {
	if n == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.signals[boundary]) == 0 {
		return
	}
	if n.wakeDelay > 0 {
		if n.timers[boundary] != nil {
			return
		}
		n.timers[boundary] = time.AfterFunc(n.wakeDelay, func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			delete(n.timers, boundary)
			n.notifyLocked(boundary)
		})
		return
	}
	n.notifyLocked(boundary)
}

func (n *SqliteEventNotifier) notifyLocked(boundary string) {
	for signal := range n.signals[boundary] {
		select {
		case signal.ch <- struct{}{}:
		default:
		}
	}
}

func (n *SqliteEventNotifier) unregister(signal *sqliteEventSignal) {
	n.mu.Lock()
	defer n.mu.Unlock()
	signals := n.signals[signal.boundary]
	delete(signals, signal)
	if len(signals) == 0 {
		delete(n.signals, signal.boundary)
		if timer := n.timers[signal.boundary]; timer != nil {
			timer.Stop()
			delete(n.timers, signal.boundary)
		}
	}
}

type sqliteEventSignal struct {
	notifier *SqliteEventNotifier
	boundary string
	ch       chan struct{}
	stopOnce sync.Once
}

func (s *sqliteEventSignal) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ch:
		return nil
	}
}

func (s *sqliteEventSignal) Stop() {
	s.stopOnce.Do(func() {
		s.notifier.unregister(s)
	})
}
