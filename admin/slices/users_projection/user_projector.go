package users_projection

import (
	"context"
	ev "github.com/OrisunLabs/Orisun/admin/events"
	common "github.com/OrisunLabs/Orisun/admin/slices/common"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"time"
)

type CreateNewUserType = func(ctx context.Context, user orisun.User) error
type DeleteUserType = func(ctx context.Context, id string) error
type CountUsersType = func() error
type GetUserById = func(ctx context.Context, userId string) (orisun.User, error)

type UserProjector struct {
	getProjectorLastPosition common.GetProjectorLastPositionType
	updateProjectorPosition  common.UpdateProjectorPositionType
	createNewUser            CreateNewUserType
	deleteUser               DeleteUserType
	getUserById              GetUserById
	logger                   l.Logger
	boundary                 string
	saveEvents               common.SaveEventsType
	getEvents                common.GetEventsType
	subscribeToEvents        common.SubscribeToEventStoreType
}

func NewUserProjector(
	getProjectorLastPosition common.GetProjectorLastPositionType,
	updateProjectorPosition common.UpdateProjectorPositionType,
	createNewUser CreateNewUserType,
	deleteUser DeleteUserType,
	getUserById GetUserById,
	saveEvents common.SaveEventsType,
	getEvents common.GetEventsType,
	logger l.Logger,
	boundary string,
	subscribeToEvents common.SubscribeToEventStoreType,
) *UserProjector {

	return &UserProjector{
		getProjectorLastPosition: getProjectorLastPosition,
		updateProjectorPosition:  updateProjectorPosition,
		createNewUser:            createNewUser,
		deleteUser:               deleteUser,
		logger:                   logger,
		boundary:                 boundary,
		saveEvents:               saveEvents,
		getEvents:                getEvents,
		subscribeToEvents:        subscribeToEvents,
		getUserById:              getUserById,
	}
}

func (p *UserProjector) Start(ctx context.Context) error {
	p.logger.Info("Starting user projector")
	var projectorName = "user-projector"
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

	return p.subscribeToEvents(
		ctx,
		coreeventstore.SubscribeRequest{
			Boundary:       p.boundary,
			SubscriberName: projectorName,
			AfterPosition:  afterPosition,
		},
		func(ctx context.Context, event coreeventstore.ReadEvent) error {
			if p.logger.IsDebugEnabled() {
				p.logger.Debugf("Receiving events for: %s", projectorName)
			}

			for {
				if err := p.handleEvent(ctx, event); err != nil {
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

func (p *UserProjector) handleEvent(ctx context.Context, event coreeventstore.ReadEvent) error {
	if p.logger.IsDebugEnabled() {
		p.logger.Debugf("Handling event %v", event)
	}

	switch event.EventType {
	case ev.EventTypeUserCreated:
		var userEvent ev.UserCreated
		if err := json.Unmarshal([]byte(event.Data), &userEvent); err != nil {
			return err
		}

		err := p.createNewUser(
			ctx,
			orisun.User{
				Id:             userEvent.UserId,
				Username:       userEvent.Username,
				HashedPassword: userEvent.PasswordHash,
				Name:           userEvent.Name,
				Roles:          userEvent.Roles,
			},
		)

		if err != nil {
			return err
		}

	case ev.EventTypeUserDeleted:
		var userEvent ev.UserDeleted
		if err := json.Unmarshal([]byte(event.Data), &userEvent); err != nil {
			return err
		}

		err := p.deleteUser(ctx, userEvent.UserId)
		if err != nil {
			return err
		}

	case ev.EventTypeUserPasswordChanged:
		var userEvent ev.UserPasswordChanged
		if err := json.Unmarshal([]byte(event.Data), &userEvent); err != nil {
			return err
		}
		user, err := p.getUserById(ctx, userEvent.UserId)

		if err != nil {
			return err
		}
		user.HashedPassword = userEvent.PasswordHash
		err = p.createNewUser(ctx, user)
		if err != nil {
			return err
		}
	}
	return nil
}
