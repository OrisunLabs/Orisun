//go:build !orisun_embedded

package orisun

import (
	"context"
	"fmt"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
)

// subscriptionDrain owns delivery progress. Only its reader goroutine may call
// drain or run; notification receivers may only call wake.
type subscriptionDrain struct {
	retriever   EventsRetriever
	boundary    string
	query       *Query
	handler     coreeventstore.EventHandler
	check       func(context.Context) error
	cursor      *Position
	initialized bool
	pending     chan struct{}
}

func newSubscriptionDrain(retriever EventsRetriever, boundary string, query *Query, after *Position, handler coreeventstore.EventHandler, check func(context.Context) error) *subscriptionDrain {
	d := &subscriptionDrain{retriever: retriever, boundary: boundary, query: query, handler: handler, check: check, pending: make(chan struct{}, 1)}
	if after != nil {
		cursor := *after
		d.cursor = &cursor
		d.initialized = true
	}
	return d
}

func (d *subscriptionDrain) wake() {
	select {
	case d.pending <- struct{}{}:
	default:
	}
}

// run reads only after a received notification records a wake-up.
func (d *subscriptionDrain) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.pending:
		}
		// Consume the wake-up before reading. A hint arriving during a read
		// stays pending, even if that read returns an empty stable view.
		if err := d.drain(ctx); err != nil {
			return err
		}
	}
}

func (d *subscriptionDrain) drain(ctx context.Context) error {
	const batchSize = 100 // Inclusive forward reads must request at least two.
	for {
		if err := d.checkActive(ctx); err != nil {
			return err
		}
		req := &GetEventsRequest{Boundary: d.boundary, Query: d.query, Direction: Direction_ASC, Count: batchSize}
		initial := !d.initialized
		if initial {
			req.Direction = Direction_DESC
			req.Count = 1
		} else if d.cursor != nil {
			cursor := *d.cursor
			req.FromPosition = &cursor
		}
		batch, err := d.retriever.GetBatch(ctx, req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return statuscode.Errorf(statuscode.Internal, "failed to get subscription events: %v", err)
		}
		if err := d.validate(batch, req.Count); err != nil {
			return statuscode.Errorf(statuscode.Internal, "invalid subscription batch: %v", err)
		}
		for i := range batch {
			event := batch[i]
			if d.cursor != nil && event.CommitPosition == d.cursor.CommitPosition && event.PreparePosition == d.cursor.PreparePosition {
				continue // Only the inclusive first row may equal the cursor.
			}
			if err := d.checkActive(ctx); err != nil {
				return err
			}
			if err := d.handler(ctx, neutralSubscriptionReadEvent(event)); err != nil {
				return statuscode.Errorf(statuscode.Internal, "subscription event handler failed: %v", err)
			}
			d.cursor = &Position{CommitPosition: event.CommitPosition, PreparePosition: event.PreparePosition}
		}
		d.initialized = true
		if len(batch) == 0 || (!initial && len(batch) < batchSize) {
			return nil
		}
	}
}

func (d *subscriptionDrain) checkActive(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.check(ctx)
}

// Validate the entire bounded batch before delivering any prefix. Criteria
// evaluation belongs entirely to the backend; only positions are inspected here.
func (d *subscriptionDrain) validate(batch ReadEventBatch, count uint32) error {
	if len(batch) > int(count) {
		return fmt.Errorf("backend returned %d events for limit %d", len(batch), count)
	}
	previous := d.cursor
	for i := range batch {
		event := &batch[i]
		if previous != nil && !positionValuesAfter(event.CommitPosition, event.PreparePosition, previous.CommitPosition, previous.PreparePosition) {
			if i != 0 || event.CommitPosition != previous.CommitPosition || event.PreparePosition != previous.PreparePosition {
				return fmt.Errorf("non-advancing position at batch index %d", i)
			}
		}
		previous = &Position{CommitPosition: event.CommitPosition, PreparePosition: event.PreparePosition}
	}
	return nil
}
