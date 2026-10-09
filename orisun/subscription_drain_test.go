//go:build !orisun_embedded

package orisun

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
)

func drainEvent(commit, prepare int64) ReadEvent {
	return ReadEvent{CommitPosition: commit, PreparePosition: prepare, Data: "backend-owned criteria evaluation"}
}

func TestSubscriptionDrainForwardsQueryAndOrdersPositionTuples(t *testing.T) {
	query := &Query{Criteria: []*Criterion{{Tags: []*Tag{{Key: "amount", Value: "10", Operator: "gte"}}}, {Tags: []*Tag{{Key: "status", Value: "closed", Operator: "ne"}}}}}
	after := &Position{CommitPosition: 7, PreparePosition: 200}
	var delivered []coreeventstore.Position
	d := newSubscriptionDrain(subscriptionReadFunc(func(_ context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
		if req.Query != query || req.Boundary != "orders" || req.Direction != Direction_ASC || req.Count < 2 || !reflect.DeepEqual(req.FromPosition, after) {
			t.Fatalf("backend request lost subscription semantics: %#v", req)
		}
		return ReadEventBatch{drainEvent(7, 200), drainEvent(7, 201), drainEvent(8, 0)}, nil
	}), "orders", query, after, func(_ context.Context, event coreeventstore.ReadEvent) error {
		delivered = append(delivered, event.Position)
		return nil
	}, func(context.Context) error { return nil })
	if err := d.drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []coreeventstore.Position{{CommitPosition: 7, PreparePosition: 201}, {CommitPosition: 8, PreparePosition: 0}}
	if !reflect.DeepEqual(delivered, want) || *after != (Position{CommitPosition: 7, PreparePosition: 200}) {
		t.Fatalf("delivered %v; supplied cursor %v", delivered, after)
	}
}

func TestSubscriptionDrainRejectsMalformedBatchBeforeDelivery(t *testing.T) {
	for _, test := range []struct {
		name  string
		batch ReadEventBatch
	}{
		{"duplicate", ReadEventBatch{drainEvent(7, 201), drainEvent(7, 201)}},
		{"backwards", ReadEventBatch{drainEvent(8, 0), drainEvent(7, 201)}},
		{"before cursor", ReadEventBatch{drainEvent(7, 199), drainEvent(7, 201)}},
		{"oversized", make(ReadEventBatch, 101)},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			d := newSubscriptionDrain(subscriptionReadFunc(func(context.Context, *GetEventsRequest) (ReadEventBatch, error) { return test.batch, nil }), "orders", nil,
				&Position{CommitPosition: 7, PreparePosition: 200}, func(context.Context, coreeventstore.ReadEvent) error { calls++; return nil }, func(context.Context) error { return nil })
			if err := d.drain(t.Context()); err == nil || !strings.Contains(err.Error(), "invalid subscription batch") {
				t.Fatalf("error = %v", err)
			}
			if calls != 0 || d.cursor.PreparePosition != 200 {
				t.Fatalf("invalid batch delivered %d events or moved cursor to %v", calls, d.cursor)
			}
		})
	}
}

func TestSubscriptionDrainHandlerFailureRetainsLastSuccessfulCursor(t *testing.T) {
	failure := errors.New("handler failed")
	reads := 0
	var delivered []int64
	d := newSubscriptionDrain(subscriptionReadFunc(func(_ context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
		reads++
		if reads == 1 {
			return ReadEventBatch{drainEvent(7, 200), drainEvent(7, 201), drainEvent(7, 202)}, nil
		}
		if req.FromPosition.PreparePosition != 201 {
			t.Fatalf("failed event advanced cursor: %v", req.FromPosition)
		}
		return ReadEventBatch{drainEvent(7, 201), drainEvent(7, 202)}, nil
	}), "orders", nil, &Position{CommitPosition: 7, PreparePosition: 200}, func(_ context.Context, event coreeventstore.ReadEvent) error {
		if reads == 1 && event.Position.PreparePosition == 202 {
			return failure
		}
		delivered = append(delivered, event.Position.PreparePosition)
		return nil
	}, func(context.Context) error { return nil })
	if err := d.drain(t.Context()); err == nil || !strings.Contains(err.Error(), failure.Error()) || d.cursor.PreparePosition != 201 {
		t.Fatalf("error = %v, cursor = %v", err, d.cursor)
	}
	if err := d.drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(delivered, []int64{201, 202}) {
		t.Fatalf("deliveries = %v", delivered)
	}
}

func TestSubscriptionDrainEmptyInitialMatchDoesNotRepeatLatestSelection(t *testing.T) {
	reads := 0
	d := newSubscriptionDrain(subscriptionReadFunc(func(_ context.Context, req *GetEventsRequest) (ReadEventBatch, error) {
		reads++
		if reads == 1 {
			if req.Direction != Direction_DESC || req.Count != 1 {
				t.Fatalf("initial request = %#v", req)
			}
			return nil, nil
		}
		if req.Direction != Direction_ASC || req.FromPosition != nil || req.Count < 2 {
			t.Fatalf("future matches must be read ascending: %#v", req)
		}
		return ReadEventBatch{drainEvent(7, 201), drainEvent(7, 202)}, nil
	}), "orders", nil, nil, func(context.Context, coreeventstore.ReadEvent) error { return nil }, func(context.Context) error { return nil })
	if err := d.drain(t.Context()); err != nil || d.cursor != nil {
		t.Fatalf("empty initial read: error %v cursor %v", err, d.cursor)
	}
	if err := d.drain(t.Context()); err != nil || d.cursor.PreparePosition != 202 {
		t.Fatalf("future match read: error %v cursor %v", err, d.cursor)
	}
}

func TestSubscriptionDrainLeaseLossStopsDelivery(t *testing.T) {
	lost := errors.New("lease lost")
	checks := 0
	deliveries := 0
	d := newSubscriptionDrain(subscriptionReadFunc(func(context.Context, *GetEventsRequest) (ReadEventBatch, error) {
		return ReadEventBatch{drainEvent(7, 201), drainEvent(7, 202)}, nil
	}), "orders", nil, &Position{CommitPosition: 7, PreparePosition: 200}, func(context.Context, coreeventstore.ReadEvent) error { deliveries++; return nil }, func(context.Context) error {
		checks++
		if checks == 3 {
			return lost
		}
		return nil
	})
	if err := d.drain(t.Context()); !errors.Is(err, lost) || deliveries != 1 || d.cursor.PreparePosition != 201 {
		t.Fatalf("error %v deliveries %d cursor %v", err, deliveries, d.cursor)
	}
}

func TestSubscriptionDrainRecoversFromHintDuringRead(t *testing.T) {
	for _, trigger := range []string{"hint during read"} {
		t.Run(trigger, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started := make(chan struct{})
			release := make(chan struct{})
			reads := 0
			var delivered []int64
			d := newSubscriptionDrain(subscriptionReadFunc(func(ctx context.Context, _ *GetEventsRequest) (ReadEventBatch, error) {
				reads++
				if reads == 1 {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return nil, nil // Hint arrives after this read's snapshot.
				}
				return ReadEventBatch{drainEvent(7, 201)}, nil
			}), "orders", nil, &Position{CommitPosition: 7, PreparePosition: 200}, func(_ context.Context, event coreeventstore.ReadEvent) error {
				delivered = append(delivered, event.Position.PreparePosition)
				cancel()
				return nil
			}, func(context.Context) error { return nil })
			result := make(chan error, 1)
			d.wake() // Simulate receipt of the initial NATS ping.
			go func() { result <- d.run(ctx) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("initial read did not start")
			}
			if trigger == "hint during read" {
				for range 1000 {
					d.wake()
				}
				if len(d.pending) != 1 {
					t.Fatal("hints must coalesce")
				}
			}
			close(release)
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(delivered, []int64{201}) {
					t.Fatalf("error %v deliveries %v", err, delivered)
				}
			case <-time.After(time.Second):
				t.Fatal("wake-up did not recover event")
			}
		})
	}
}

func TestSubscriptionDrainCancellationDuringRead(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	d := newSubscriptionDrain(subscriptionReadFunc(func(ctx context.Context, _ *GetEventsRequest) (ReadEventBatch, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}), "orders", nil, nil, func(context.Context, coreeventstore.ReadEvent) error {
		t.Error("cancelled read delivered event")
		return nil
	}, func(context.Context) error { return nil })
	result := make(chan error, 1)
	d.wake() // Simulate receipt of the initial NATS ping.
	go func() { result <- d.run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not stop read")
	}
}

func TestSubscriptionDrainReadFailureDoesNotMoveCursor(t *testing.T) {
	d := newSubscriptionDrain(subscriptionReadFunc(func(context.Context, *GetEventsRequest) (ReadEventBatch, error) {
		return ReadEventBatch{drainEvent(7, 201)}, errors.New("backend unavailable")
	}), "orders", nil, &Position{CommitPosition: 7, PreparePosition: 200}, func(context.Context, coreeventstore.ReadEvent) error {
		t.Fatal("failed read must not deliver a partial batch")
		return nil
	}, func(context.Context) error { return nil })
	if err := d.drain(t.Context()); err == nil || d.cursor.PreparePosition != 200 {
		t.Fatalf("error %v cursor %v", err, d.cursor)
	}
}

func TestSubscriptionDrainCancellationWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	read := make(chan struct{})
	d := newSubscriptionDrain(subscriptionReadFunc(func(context.Context, *GetEventsRequest) (ReadEventBatch, error) {
		close(read)
		return nil, nil
	}), "orders", nil, nil, func(context.Context, coreeventstore.ReadEvent) error { return nil }, func(context.Context) error { return nil })
	result := make(chan error, 1)
	d.wake() // Simulate receipt of the initial NATS ping.
	go func() { result <- d.run(ctx) }()
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Fatal("initial read did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not stop waiting")
	}
}
