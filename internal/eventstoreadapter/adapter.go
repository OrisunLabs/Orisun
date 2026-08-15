// Package eventstoreadapter confines conversions between Orisun's legacy
// event-store model and the transport-neutral eventstore model.
package eventstoreadapter

import (
	"context"
	"fmt"
	"strconv"

	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"github.com/OrisunLabs/Orisun/orisun"
)

type SubscribeFunc func(
	ctx context.Context,
	request coreeventstore.SubscribeRequest,
	handler coreeventstore.EventHandler,
) error

// Adapter implements the narrow neutral ports declared by boundary slices.
type Adapter struct {
	saver     orisun.EventsSaver
	retriever orisun.EventsRetriever
	subscribe SubscribeFunc
}

func New(saver orisun.EventsSaver, retriever orisun.EventsRetriever, subscribe SubscribeFunc) *Adapter {
	return &Adapter{saver: saver, retriever: retriever, subscribe: subscribe}
}

func (a *Adapter) Append(ctx context.Context, request coreeventstore.AppendRequest) (coreeventstore.AppendResult, error) {
	if a == nil || a.saver == nil {
		return coreeventstore.AppendResult{}, fmt.Errorf("event-store append adapter is not configured")
	}
	input := make([]orisun.EventWithMapTags, len(request.Events))
	for index, event := range request.Events {
		input[index] = orisun.EventWithMapTags{
			EventId:   event.EventID,
			EventType: event.EventType,
			Data:      event.Data,
			Metadata:  event.Metadata,
		}
	}
	prepared, err := orisun.PrepareEventsForSave(input)
	if err != nil {
		return coreeventstore.AppendResult{}, err
	}
	transactionID, globalID, err := a.saver.SavePrepared(
		ctx,
		prepared,
		request.Boundary,
		legacyPosition(request.ExpectedPosition),
		legacyQuery(request.Subset),
	)
	if err != nil {
		return coreeventstore.AppendResult{}, err
	}
	commitPosition, err := strconv.ParseInt(transactionID, 10, 64)
	if err != nil {
		return coreeventstore.AppendResult{}, fmt.Errorf("parse transaction id %q: %w", transactionID, err)
	}
	return coreeventstore.AppendResult{Position: coreeventstore.Position{
		CommitPosition:  commitPosition,
		PreparePosition: globalID,
	}}, nil
}

func (a *Adapter) Read(ctx context.Context, request coreeventstore.ReadRequest) (coreeventstore.ReadEventBatch, error) {
	if a == nil || a.retriever == nil {
		return nil, fmt.Errorf("event-store read adapter is not configured")
	}
	batch, err := a.retriever.GetBatch(ctx, &orisun.GetEventsRequest{
		Boundary:     request.Boundary,
		FromPosition: legacyPosition(request.FromPosition),
		Count:        request.Count,
		Direction:    legacyDirection(request.Direction),
		Query:        legacyQuery(request.Query),
	})
	if err != nil {
		return nil, err
	}
	result := make(coreeventstore.ReadEventBatch, len(batch))
	for index, event := range batch {
		result[index] = neutralReadEvent(event)
	}
	return result, nil
}

func (a *Adapter) LatestByCriteria(
	ctx context.Context,
	request coreeventstore.LatestByCriteriaRequest,
) (coreeventstore.LatestByCriteriaResult, error) {
	if a == nil || a.retriever == nil {
		return coreeventstore.LatestByCriteriaResult{}, fmt.Errorf("event-store latest-by-criteria adapter is not configured")
	}
	criteria := make([]orisun.ReadCriterion, len(request.Criteria))
	for index, criterion := range request.Criteria {
		criteria[index] = orisun.ReadCriterion{Tags: make([]orisun.ReadTag, len(criterion.Tags))}
		for tagIndex, tag := range criterion.Tags {
			criteria[index].Tags[tagIndex] = orisun.ReadTag{Key: tag.Key, Value: tag.Value}
		}
	}
	batch, err := a.retriever.GetLatestByCriteria(ctx, orisun.LatestByCriteriaQuery{
		Boundary: request.Boundary,
		Criteria: criteria,
	})
	if err != nil {
		return coreeventstore.LatestByCriteriaResult{}, err
	}
	result := coreeventstore.LatestByCriteriaResult{
		Matches: make([]coreeventstore.LatestCriterionMatch, len(batch.Matches)),
		ContextPosition: coreeventstore.Position{
			CommitPosition:  batch.ContextCommitPosition,
			PreparePosition: batch.ContextPreparePosition,
		},
	}
	for index, match := range batch.Matches {
		result.Matches[index] = coreeventstore.LatestCriterionMatch{
			Event: neutralReadEvent(match.Event),
			Found: match.Found,
		}
	}
	return result, nil
}

func (a *Adapter) Subscribe(
	ctx context.Context,
	request coreeventstore.SubscribeRequest,
	handle coreeventstore.EventHandler,
) error {
	if a == nil || a.subscribe == nil || handle == nil {
		return fmt.Errorf("event-store subscription adapter is not configured")
	}
	return a.subscribe(ctx, request, handle)
}

func legacyPosition(position *coreeventstore.Position) *orisun.Position {
	if position == nil {
		return nil
	}
	return &orisun.Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition,
	}
}

func legacyQuery(query coreeventstore.Query) *orisun.Query {
	if len(query.Criteria) == 0 {
		return nil
	}
	result := &orisun.Query{Criteria: make([]*orisun.Criterion, len(query.Criteria))}
	for index, criterion := range query.Criteria {
		tags := make([]*orisun.Tag, len(criterion.Tags))
		for tagIndex, tag := range criterion.Tags {
			tags[tagIndex] = &orisun.Tag{Key: tag.Key, Value: tag.Value}
		}
		result.Criteria[index] = &orisun.Criterion{Tags: tags}
	}
	return result
}

func legacyDirection(direction coreeventstore.Direction) orisun.Direction {
	if direction == coreeventstore.DirectionDescending {
		return orisun.Direction_DESC
	}
	return orisun.Direction_ASC
}

func neutralReadEvent(event orisun.ReadEvent) coreeventstore.ReadEvent {
	return coreeventstore.ReadEvent{
		EventID:   event.EventId,
		EventType: event.EventType,
		Data:      eventdata.WithoutStorageEventType(event.Data),
		Metadata:  event.Metadata,
		Position: coreeventstore.Position{
			CommitPosition:  event.CommitPosition,
			PreparePosition: event.PreparePosition,
		},
		DateCreated: event.DateCreated,
	}
}
