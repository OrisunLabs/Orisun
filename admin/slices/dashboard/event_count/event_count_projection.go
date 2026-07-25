package event_count

import (
	"context"
	admin_common "github.com/OrisunLabs/Orisun/admin/slices/common"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

const (
	projectorName             = "Event_Count_Projection"
	EventCountPubSubscription = "events-count"
)

func projectorNameForBoundary(boundary string) string {
	return projectorName + "__" + boundary
}

type EventCountReadModel struct {
	Count    int
	Boundary string
}

type GetEventCount = func(ctx context.Context, boundary string) (EventCountReadModel, error)
type SaveEventCount = func(ctx context.Context, count int, boundary string) error

type SubscribeToEventCount = func(consumerName string, boundary string, ctx context.Context, stream *orisun.MessageHandler[EventCountReadModel]) error

type EventCountEventHandler struct {
	boundary                  string
	getProjectorLastPosition  admin_common.GetProjectorLastPositionType
	publishEventCountToPubSub admin_common.PublishToPubSubType
	getEventsCount            GetEventCount
	saveEventCount            SaveEventCount
	subscribeToEventStore     admin_common.SubscribeToEventStoreType
	updateProjectorPosition   admin_common.UpdateProjectorPositionType
	logger                    l.Logger
}

func NewEventCountProjection(
	boundary string,
	getProjectorLastPosition admin_common.GetProjectorLastPositionType,
	publishEventCountToPubSub admin_common.PublishToPubSubType,
	getEventsCount GetEventCount,
	saveEventCount SaveEventCount,
	subscribeToEventStore admin_common.SubscribeToEventStoreType,
	updateProjectorPosition admin_common.UpdateProjectorPositionType,
	logger l.Logger,
) *EventCountEventHandler {
	return &EventCountEventHandler{
		boundary:                  boundary,
		publishEventCountToPubSub: publishEventCountToPubSub,
		getEventsCount:            getEventsCount,
		subscribeToEventStore:     subscribeToEventStore,
		getProjectorLastPosition:  getProjectorLastPosition,
		updateProjectorPosition:   updateProjectorPosition,
		logger:                    logger,
		saveEventCount:            saveEventCount,
	}
}

func (p *EventCountEventHandler) Start(ctx context.Context) error {
	projectorName := projectorNameForBoundary(p.boundary)

	// Get last checkpoint
	pos, err := p.getProjectorLastPosition(ctx, projectorName)
	if err != nil {
		return err
	}

	var afterPosition *coreeventstore.Position
	if pos != nil {
		afterPosition = &coreeventstore.Position{
			CommitPosition:  pos.CommitPosition,
			PreparePosition: pos.PreparePosition,
		}
	}

	return p.subscribeToEventStore(
		ctx,
		coreeventstore.SubscribeRequest{
			Boundary:       p.boundary,
			SubscriberName: projectorName,
			AfterPosition:  afterPosition,
		},
		func(ctx context.Context, event coreeventstore.ReadEvent) error {
			for {
				if p.logger.IsDebugEnabled() {
					p.logger.Debugf("Receiving events for: %s", "events_count_projection")
				}

				if err := p.Project(ctx, event); err != nil {
					p.logger.Error("Error handling event: %v", err)

					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(5 * time.Second):
					}
					continue
				}

				position := orisun.Position{
					CommitPosition:  event.Position.CommitPosition,
					PreparePosition: event.Position.PreparePosition,
				}

				err := p.updateProjectorPosition(
					ctx,
					projectorName,
					&position,
				)

				if err != nil {
					p.logger.Error("Error updating checkpoint: %v", err)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(5 * time.Second):
					}
					continue
				}
				return nil
			}
		},
	)
}

func (p *EventCountEventHandler) Project(ctx context.Context, event coreeventstore.ReadEvent) error {
	// For any event type, we increment the event count
	// p.logger.Infof("Projecting event: %v", event)
	// Get current event count
	currentCount, err := p.getEventsCount(ctx, p.boundary)
	if err != nil {
		return err
	}

	newCount := currentCount.Count + 1
	// Increment the count
	updatedCount := &EventCountReadModel{
		Count:    newCount,
		Boundary: p.boundary,
	}
	marshaled, err := json.Marshal(updatedCount)
	if err != nil {
		return err
	}
	p.saveEventCount(ctx, newCount, p.boundary)
	eventId, err := uuid.NewV7()
	if err != nil {
		return err
	}
	p.publishEventCountToPubSub(ctx, &admin_common.PublishRequest{
		Id:      eventId.String(),
		Subject: EventCountPubSubscription,
		Data:    marshaled,
	})

	return nil
}
