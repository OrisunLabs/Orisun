//go:build foundationdb

package foundationdb

import (
	"bytes"
	"context"
	"strconv"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Native position predicates use the commit-ordered event key itself. They
// never require an asynchronously maintained copy of the committed position.
// A commit or write ID is the required leading key; prepare position alone
// cannot select a bounded slice of this ordered keyspace.
func (b *Backend) nativeCriterionRange(boundary string, criterion map[string]any) (fdb.KeyRange, bool) {
	prefix := prefixRange(b.eventPrefix(boundary))
	empty := fdb.KeyRange{Begin: prefix.Begin, End: prefix.Begin}
	var tx, first, last int64
	last = 1<<32 - 1
	if id, ok := criterionEquality(criterion, "__writeId"); ok {
		pos, err := eventstore.ValidateWriteContextRequest(&eventstore.GetWriteContextRequest{Boundary: boundary, WriteId: id})
		if err != nil || pos.PreparePosition > last {
			return empty, true
		}
		tx, first, last = pos.CommitPosition, pos.PreparePosition & ^int64(65535), pos.PreparePosition
	} else if value, ok := criterionEquality(criterion, "__commitPosition"); ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 0 || strconv.FormatInt(parsed, 10) != value {
			return empty, true
		}
		tx = parsed
	} else if predicates, ok := criterion["__commitPosition"]; ok {
		lower, upper, possible := commitBounds(predicates)
		if !possible {
			return empty, true
		}
		begin := b.eventKeyForPosition(boundary, &eventstore.Position{CommitPosition: lower})
		end := keyAfter(b.eventKeyForPosition(boundary, &eventstore.Position{CommitPosition: upper, PreparePosition: last}))
		return fdb.KeyRange{Begin: begin, End: end}, true
	} else if _, ok := criterion["__writeId"]; ok {
		// Write IDs are strings; their lexicographic order differs from native
		// commit order. The native range must cover all candidate positions.
		return prefix, true
	} else {
		return fdb.KeyRange{}, false
	}
	if value, ok := criterionEquality(criterion, "__preparePosition"); ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < first || parsed > last || strconv.FormatInt(parsed, 10) != value {
			return empty, true
		}
		first, last = parsed, parsed
	}
	begin := b.eventKeyForPosition(boundary, &eventstore.Position{CommitPosition: tx, PreparePosition: first})
	end := keyAfter(b.eventKeyForPosition(boundary, &eventstore.Position{CommitPosition: tx, PreparePosition: last}))
	return fdb.KeyRange{Begin: begin, End: end}, true
}

func (b *Backend) scanNativeCriterion(ctx context.Context, rt fdb.ReadTransaction, boundary string, criterion map[string]any, native fdb.KeyRange, from *eventstore.Position, direction eventstore.Direction, count int) (eventstore.ReadEventBatch, error) {
	begin, end := b.eventRangeForCursor(boundary, from, direction)
	if bytes.Compare(begin.FDBKey(), native.Begin.FDBKey()) < 0 {
		begin = native.Begin
	}
	if bytes.Compare(end.FDBKey(), native.End.FDBKey()) > 0 {
		end = native.End
	}
	return scanEventRange(ctx, rt, fdb.KeyRange{Begin: begin, End: end}, direction, count, criterion)
}

// scanEventRange applies predicates before counting results. Unfiltered reads
// can also bound the database range request by the requested count.
func scanEventRange(ctx context.Context, rt fdb.ReadTransaction, keyRange fdb.KeyRange, direction eventstore.Direction, count int, criterion map[string]any) (eventstore.ReadEventBatch, error) {
	if bytes.Compare(keyRange.Begin.FDBKey(), keyRange.End.FDBKey()) >= 0 {
		return nil, nil
	}
	if count <= 0 {
		count = int(eventstore.DefaultReadBatchSize)
	}
	// No row limit before filtering: a bounded prefix must be exhausted to
	// establish absence, especially inside a CCC transaction.
	options := fdb.RangeOptions{Mode: fdb.StreamingModeIterator, Reverse: direction == eventstore.Direction_DESC}
	if len(criterion) == 0 {
		options.Limit = count
		options.Mode = fdb.StreamingModeWantAll
	}
	iter := rt.GetRange(keyRange, options).Iterator()
	var events eventstore.ReadEventBatch
	for iter.Advance() {
		if err := contextStatusErr(ctx); err != nil {
			return nil, err
		}
		kv, err := iter.Get()
		if err != nil {
			return nil, err
		}
		tx, gid, err := eventPositionFromKey(kv.Key)
		if err != nil {
			return nil, err
		}
		event, err := readEventFromRecord(kv.Value, tx, gid)
		if err != nil {
			return nil, err
		}
		if len(criterion) > 0 {
			data, err := eventdata.EnvelopeFields(event.Data, eventdata.Envelope{EventID: event.EventId, EventType: event.EventType, WriteID: event.WriteId, Metadata: event.Metadata, DateCreated: event.DateCreated, CommitPosition: tx, PreparePosition: gid})
			if err != nil {
				return nil, err
			}
			if !eventMatchesCriterion(data, criterion) {
				continue
			}
		}
		events = append(events, event)
		if len(events) == count {
			break
		}
	}
	return events, nil
}

func (b *Backend) scanCriterion(ctx context.Context, rt fdb.ReadTransaction, boundary string, indexes []indexDefinition, criterion map[string]any, from *eventstore.Position, direction eventstore.Direction, count int) (eventstore.ReadEventBatch, error) {
	if native, ok := b.nativeCriterionRange(boundary, criterion); ok {
		return b.scanNativeCriterion(ctx, rt, boundary, criterion, native, from, direction, count)
	}
	idx, ok := chooseCoveringIndex(indexes, criterion)
	if !ok {
		return nil, b.unindexedQueryErr(boundary, criterion)
	}
	return b.scanIndexCandidates(ctx, rt, boundary, idx, criterion, from, direction, count)
}
