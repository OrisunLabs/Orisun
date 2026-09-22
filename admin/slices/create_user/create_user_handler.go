package create_user

import (
	"context"
	"fmt"
	ev "github.com/OrisunLabs/Orisun/admin/events"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"strings"

	"github.com/goccy/go-json"

	admincommon "github.com/OrisunLabs/Orisun/admin/slices/common"

	"github.com/google/uuid"
)

type CreateUserHandler struct {
	logger     l.Logger
	boundary   string
	saveEvents admincommon.SaveEventsType
	getEvents  admincommon.GetEventsType
}

func NewCreateUserHandler(
	logger l.Logger,
	boundary string,
	saveEvents admincommon.SaveEventsType,
	getEvents admincommon.GetEventsType) *CreateUserHandler {
	return &CreateUserHandler{
		logger:     logger,
		boundary:   boundary,
		getEvents:  getEvents,
		saveEvents: saveEvents,
	}
}

type AddNewUserRequest struct {
	Name            string
	Username        string
	Password        string
	ConfirmPassword string
	Role            string
}

func CreateUser(
	ctx context.Context,
	name, username, password string,
	roles []orisun.Role,
	boundary string,
	saveEvents admincommon.SaveEventsType,
	getEvents admincommon.GetEventsType,
	logger l.Logger,
	currentUserId *string,
) (*ev.UserCreated, error) {
	username = strings.TrimSpace(username)

	userCreatedEvent, err := getEvents(
		ctx,
		&orisun.GetEventsRequest{
			Boundary:  boundary,
			Count:     1,
			Direction: orisun.Direction_DESC,
			Query: &orisun.Query{
				Criteria: []*orisun.Criterion{
					{
						Tags: []*orisun.Tag{
							{Key: "username", Value: username},
							{Key: "__eventType", Value: ev.EventTypeUserCreated},
						},
					},
				},
			},
		},
	)

	if err != nil {
		return nil, err
	}

	logger.Infof("userCreatedEvent: %v", userCreatedEvent)

	var events []*ev.Event
	for _, event := range userCreatedEvent.Events {
		var userCreatedEvent = ev.UserCreated{}
		err := json.Unmarshal([]byte(event.Data), &userCreatedEvent)
		if err != nil {
			return nil, err
		}
		events = append(events, &ev.Event{
			EventType: event.EventType,
			Data:      userCreatedEvent,
		})
	}

	if len(userCreatedEvent.Events) > 0 {
		userCreatedEvent := events[0].Data.(ev.UserCreated)
		UserDeletedEvent, err := getEvents(
			ctx,
			&orisun.GetEventsRequest{
				Boundary:  boundary,
				Direction: orisun.Direction_DESC,
				Count:     1,
				Query: &orisun.Query{
					Criteria: []*orisun.Criterion{
						{
							Tags: []*orisun.Tag{
								{Key: "__eventType", Value: ev.EventTypeUserDeleted},
								{Key: "userId", Value: userCreatedEvent.UserId},
							},
						},
					},
				},
			},
		)

		if err != nil {
			return nil, err
		}

		for _, event := range UserDeletedEvent.Events {
			var userDeletedEvent = ev.UserDeleted{}
			err := json.Unmarshal([]byte(event.Data), &userDeletedEvent)
			if err != nil {
				return nil, err
			}
			events = append(events, &ev.Event{
				EventType: event.EventType,
				Data:      userDeletedEvent,
			})
		}
	}

	userId, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}

	newEvents, err := createUserCommandHandler(userId.String(), username, password, name, roles, events)

	if err != nil {
		return nil, err
	}

	var eventsToSave []*orisun.EventToSave

	for _, event := range newEvents {
		eventData, err := json.Marshal(event.Data)
		if err != nil {
			return nil, err
		}
		eventId, err := uuid.NewV7()
		if err != nil {
			return nil, err
		}
		currentUserIdVerified := "SYSTEM"
		if currentUserId != nil {
			currentUserIdVerified = *currentUserId
		}
		eventsToSave = append(eventsToSave, &orisun.EventToSave{
			EventId:   eventId.String(),
			EventType: event.EventType,
			Data:      string(eventData),
			Metadata:  "{\"schema\":\"" + boundary + "\",\"createdBy\":\"" + currentUserIdVerified + "\"}",
		})
	}

	position := orisun.NotExistsPosition()
	if _, err := saveEvents(ctx, &orisun.SaveEventsRequest{
		Boundary: boundary,
		Events:   eventsToSave,
		Query: &orisun.SaveQuery{
			ExpectedPosition: &position,
			SubsetQuery: &orisun.Query{
				Criteria: []*orisun.Criterion{
					{
						Tags: []*orisun.Tag{
							{Key: "username", Value: username},
							{Key: "__eventType", Value: ev.EventTypeUserCreated},
						},
					},
					{
						Tags: []*orisun.Tag{
							{Key: "__eventType", Value: ev.EventTypeUserDeleted},
							{Key: "userId", Value: userId.String()},
						},
					},
				},
			},
		},
	}); err != nil {
		return nil, err
	}

	userCreated, ok := newEvents[0].Data.(ev.UserCreated)
	if !ok {
		return nil, fmt.Errorf("unexpected event data type")
	}
	return &userCreated, nil
}

func createUserCommandHandler(userId string, username, password string, name string, roles []orisun.Role, events []*ev.Event) ([]*ev.Event, error) {
	//check if events contains any user created event and not user deleted event
	if len(events) > 0 {
		hasUserCreated := false
		hasUserDeleted := false
		for _, event := range events {
			switch event.EventType {
			case ev.EventTypeUserCreated:
				hasUserCreated = true
			case ev.EventTypeUserDeleted:
				hasUserDeleted = true
			}
		}
		if hasUserCreated && !hasUserDeleted {
			return nil, UserExistsError{
				username: username,
			}
		}
	}

	hash, err := admincommon.HashPassword(password)
	if err != nil {
		return nil, err
	}

	// Create user created event
	event := ev.Event{
		EventType: ev.EventTypeUserCreated,
		Data: ev.UserCreated{
			Name:         name,
			Username:     username,
			Roles:        roles,
			PasswordHash: string(hash),
			UserId:       userId,
		},
	}
	return []*ev.Event{&event}, nil
}

type UserExistsError struct {
	username string
}

func (e UserExistsError) Error() string {
	return fmt.Sprintf("username %s already exists", e.username)
}
