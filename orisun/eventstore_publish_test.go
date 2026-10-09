package orisun

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// --- test doubles ---------------------------------------------------------

type noopLogger struct{}

func (noopLogger) IsDebugEnabled() bool  { return false }
func (noopLogger) Debug(...any)          {}
func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Info(...any)           {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Warn(...any)           {}
func (noopLogger) Warnf(string, ...any)  {}
func (noopLogger) Error(...any)          {}
func (noopLogger) Errorf(string, ...any) {}
func (noopLogger) Fatal(...any)          {}
func (noopLogger) Fatalf(string, ...any) {}

// fakeRetriever serves events with PreparePosition >= req.FromPosition, capped
// at req.Count, mimicking the position-inclusive paginated Get.
type fakeRetriever struct {
	mu      sync.Mutex
	events  ReadEventBatch
	calls   int
	errOnce bool
}

func (r *fakeRetriever) add(events ...ReadEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, events...)
}

func (r *fakeRetriever) GetLatestByCriteria(ctx context.Context, query LatestByCriteriaQuery) (LatestByCriteriaBatch, error) {
	return LatestByCriteriaBatch{}, errors.New("not implemented in fake")
}

func (r *fakeRetriever) GetBatch(ctx context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.errOnce {
		r.errOnce = false
		return nil, errors.New("transient get error")
	}
	out := make(ReadEventBatch, 0, req.Count)
	for step := range r.events {
		i := step
		if req.Direction == Direction_DESC {
			i = len(r.events) - 1 - step
		}
		e := r.events[i]
		if req.FromPosition != nil {
			cmp := ComparePositions(&Position{CommitPosition: e.CommitPosition, PreparePosition: e.PreparePosition}, req.FromPosition)
			if (req.Direction == Direction_ASC && cmp == IsLessThan) || (req.Direction == Direction_DESC && cmp == IsGreaterThan) {
				continue
			}
		}
		out = append(out, e)
		if uint32(len(out)) >= req.Count {
			break
		}
	}
	return out, nil
}

// fakeJS embeds jetstream.JetStream so only Publish needs an implementation;
// any other method call would nil-panic, which is fine — the loop only Publishes.
type fakeJS struct {
	jetstream.JetStream
	mu           sync.Mutex
	published    [][]byte
	subjects     []string
	optionCounts []int
	attempts     int
	failFirst    int
	failAfter    int
}

func (f *fakeJS) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.failFirst > 0 {
		f.failFirst--
		return nil, errors.New("nats unavailable")
	}
	if f.failAfter > 0 && len(f.published) >= f.failAfter {
		return nil, errors.New("nats unavailable")
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	f.published = append(f.published, cp)
	f.subjects = append(f.subjects, subject)
	f.optionCounts = append(f.optionCounts, len(opts))
	return &jetstream.PubAck{}, nil
}

func (f *fakeJS) publishedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

// pulseSignal wakes the loop repeatedly, simulating polling/NOTIFY ticks.
type pulseSignal struct {
	interval time.Duration
	stopped  atomic.Bool
}

func (s *pulseSignal) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.interval):
		return nil
	}
}

func (s *pulseSignal) Stop() { s.stopped.Store(true) }

func makeEvent(i int) ReadEvent {
	return ReadEvent{
		EventId:         "e" + string(rune('0'+i)),
		EventType:       "TestEvent",
		Data:            "{}",
		CommitPosition:  int64(i),
		PreparePosition: int64(i),
		DateCreated:     time.Unix(int64(i), 0).UTC(),
	}
}

// --- tests ----------------------------------------------------------------
