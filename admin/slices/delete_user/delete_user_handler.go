package delete_user

import (
	"context"
	"fmt"
	admin_common "github.com/OrisunLabs/Orisun/admin/slices/common"
	l "github.com/OrisunLabs/Orisun/logging"
	"strings"

	"github.com/goccy/go-json"

	"github.com/OrisunLabs/Orisun/admin/events"
	eventstore "github.com/OrisunLabs/Orisun/orisun"

	"github.com/google/uuid"
)

type DeleteUserHandler struct {
	logger     l.Logger
	saveEvents admin_common.SaveEventsType
	getEvents  admin_common.GetEventsType
	boundary   string
}

func NewDeleteUserHandler(logger l.Logger, saveEvents admin_common.SaveEventsType, getEvents admin_common.GetEventsType, boundary string) *DeleteUserHandler {
	return &DeleteUserHandler{
		logger:     logger,
		saveEvents: saveEvents,
		getEvents:  getEvents,
		boundary:   boundary,
	}
}

func DeleteUser(
	ctx context.Context,
	userId string,
	currentUserId string,
	boundary string,
	saveEvents admin_common.SaveEventsType,
	getEvents admin_common.GetEventsType,
	logger l.Logger,
) error {
	userId = strings.TrimSpace(userId)
	currentUserId = strings.TrimSpace(currentUserId)
	if logger.IsDebugEnabled() {
		logger.Debug("Current Userrrr: " + currentUserId)
	}

	if userId == currentUserId {
		return fmt.Errorf("you cannot delete your own account")
	}
	logger.Infof("Deleting user: %s", userId)
	evts, err := getEvents(
		ctx,
		&eventstore.GetEventsRequest{
			Boundary:  boundary,
			Count:     2,
			Direction: eventstore.Direction_DESC,
			Query: &eventstore.Query{
				Criteria: []*eventstore.Criterion{
					{
						Tags: []*eventstore.Tag{
							{Key: "user_id", Value: userId},
							{Key: "__eventType", Value: events.EventTypeUserCreated},
						},
					},
					{
						Tags: []*eventstore.Tag{
							{Key: "user_id", Value: userId},
							{Key: "__eventType", Value: events.EventTypeUserDeleted},
						},
					},
				},
			},
		},
	)
	if err != nil {
		return err
	}

	if len(evts.Events) > 0 {
		for i := 0; i < len(evts.Events); i++ {
			if evts.Events[i].EventType == events.EventTypeUserDeleted {
				return fmt.Errorf("error: user already deleted")
			}
		}

		event := events.UserDeleted{
			UserId: userId,
		}

		eventData, err := json.Marshal(event)
		if err != nil {
			return err
		}

		// Store event
		id, err := uuid.NewV7()
		if err != nil {
			return err
		}
		lastExpectedVersion := evts.Events[len(evts.Events)-1].Position

		_, err = saveEvents(ctx, &eventstore.SaveEventsV2Request{
			Boundary: boundary, Consistency: []*eventstore.ConsistencyObservation{{Position: lastExpectedVersion, Query: &eventstore.Query{
				Criteria: []*eventstore.Criterion{
					{
						Tags: []*eventstore.Tag{
							{Key: "user_id", Value: userId},
							{Key: "__eventType", Value: events.EventTypeUserCreated},
						},
					},
					{
						Tags: []*eventstore.Tag{
							{Key: "user_id", Value: userId},
							{Key: "__eventType", Value: events.EventTypeUserDeleted},
						},
					},
				},
			}}}, Events: []*eventstore.EventToSave{{
				EventId:   id.String(),
				EventType: events.EventTypeUserDeleted,
				Data:      string(eventData),
				Metadata:  "{\"schema\":\"" + boundary + "\",\"createdBy\":\"" + id.String() + "\"}",
			}},
		})

		if err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("error: user not found")
}
