package user_count

import (
	"context"
	ev "github.com/OrisunLabs/Orisun/admin/events"
	admin_common "github.com/OrisunLabs/Orisun/admin/slices/common"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

const (
	projectorName            = "User_Count_Projection"
	UserCountPubSubscription = "users-count"
)

type UserCountReadModel struct {
	Count uint32
}

type GetUserCount = func(ctx context.Context) (UserCountReadModel, error)
type SaveUserCount = func(ctx context.Context, count uint32) error

type SubscribeToUserCount = func(consumerName string, ctx context.Context, stream *orisun.MessageHandler[UserCountReadModel]) error

type UserCountEventHandler struct {
	boundary                 string
	getProjectorLastPosition admin_common.GetProjectorLastPositionType
	publishUserCountToPubSub admin_common.PublishToPubSubType
	getUsersCount            GetUserCount
	saveUserCount            SaveUserCount
	subscribeToEventStore    admin_common.SubscribeToEventStoreType
	updateProjectorPosition  admin_common.UpdateProjectorPositionType
	logger                   l.Logger
}

func NewUserCountProjection(
	boundary string,
	getProjectorLastPosition admin_common.GetProjectorLastPositionType,
	publishUsersCountToPubSub admin_common.PublishToPubSubType,
	getUsersCount GetUserCount,
	saveUserCount SaveUserCount,
	subscribeToEventStore admin_common.SubscribeToEventStoreType,
	updateProjectorPosition admin_common.UpdateProjectorPositionType,
	logger l.Logger,
) *UserCountEventHandler {
	return &UserCountEventHandler{
		boundary:                 boundary,
		publishUserCountToPubSub: publishUsersCountToPubSub,
		getUsersCount:            getUsersCount,
		subscribeToEventStore:    subscribeToEventStore,
		getProjectorLastPosition: getProjectorLastPosition,
		updateProjectorPosition:  updateProjectorPosition,
		logger:                   logger,
		saveUserCount:            saveUserCount,
	}
}

func (p *UserCountEventHandler) Start(ctx context.Context) error {
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
					p.logger.Debugf("Receiving events for: %s", "users_count_projection")
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

func (p *UserCountEventHandler) Project(ctx context.Context, event coreeventstore.ReadEvent) error {
	switch event.EventType {
	case ev.EventTypeUserCreated:
		{
			// p.logger.Infof("Projecting event: %v", event)
			// Get current user count
			currentCount, err := p.getUsersCount(ctx)
			if err != nil {
				return err
			}

			newCount := currentCount.Count + 1
			// Increment the count
			updatedCount := &UserCountReadModel{
				Count: newCount,
			}
			marshaled, err := json.Marshal(updatedCount)
			if err != nil {
				return err
			}
			p.saveUserCount(ctx, newCount)
			userId, err := uuid.NewV7()
			if err != nil {
				return err
			}
			p.publishUserCountToPubSub(ctx, &admin_common.PublishRequest{
				Id:      userId.String(),
				Subject: UserCountPubSubscription,
				Data:    marshaled,
			})
		}
	case ev.EventTypeUserDeleted:
		{
			// Get current user count
			currentCount, err := p.getUsersCount(ctx)
			if err != nil {
				return err
			}

			// Increment the count
			newCount := currentCount.Count - 1

			updatedCount := UserCountReadModel{
				Count: newCount,
			}

			marshaled, err := json.Marshal(updatedCount)
			if err != nil {
				return err
			}

			p.saveUserCount(ctx, newCount)
			userId, err := uuid.NewV7()
			if err != nil {
				return err
			}
			p.publishUserCountToPubSub(ctx, &admin_common.PublishRequest{
				Id:      userId.String(),
				Subject: UserCountPubSubscription,
				Data:    marshaled,
			})
		}
	}
	return nil
}
