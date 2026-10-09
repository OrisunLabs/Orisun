package changepassword

import (
	"context"
	"errors"

	admin_events "github.com/OrisunLabs/Orisun/admin/events"
	admin_common "github.com/OrisunLabs/Orisun/admin/slices/common"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

var ErrInvalidCurrentPassword = errors.New("invalid current password")

type ChangePasswordHandler struct {
	logger     l.Logger
	boundary   string
	saveEvents admin_common.SaveEventsType
	getEvents  admin_common.GetEventsType
}

func NewChangePasswordHandler(
	logger l.Logger,
	boundary string,
	saveEvents admin_common.SaveEventsType,
	getEvents admin_common.GetEventsType) *ChangePasswordHandler {
	return &ChangePasswordHandler{
		logger:     logger,
		boundary:   boundary,
		getEvents:  getEvents,
		saveEvents: saveEvents,
	}
}

type ChangePasswordRequest struct {
	CurrentPassword string `json:"currentPassword"`
	NewPassword     string `json:"newPassword"`
	ConfirmPassword string `json:"confirmPassword"`
}

func ChangePassword(
	ctx context.Context,
	currentPassword, newPassword string,
	boundary string,
	saveEvents admin_common.SaveEventsType,
	getEvents admin_common.GetEventsType,
	logger l.Logger,
	currentUserId string,
) error {
	// Use goroutines to fetch events concurrently with errgroup for cancellation
	var userCreated, userDeleted, passwordChanged *orisun.GetEventsResponse

	newUserEventRequest := func(eventType string) *orisun.GetEventsRequest {
		return &orisun.GetEventsRequest{
			Boundary:  boundary,
			Count:     1,
			Direction: orisun.Direction_DESC,
			Query: &orisun.Query{
				Criteria: []*orisun.Criterion{
					{
						Tags: []*orisun.Tag{
							{Key: "user_id", Value: currentUserId},
							{Key: "__eventType", Value: eventType},
						},
					},
				},
			},
		}
	}

	// Fetch UserCreated event
	var err error
	userCreated, err = getEvents(ctx, newUserEventRequest(admin_events.EventTypeUserCreated))
	if err != nil {
		return err
	}

	// Concurrently fetch UserDeleted and PasswordChanged using errgroup
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		// UserDeleted
		resp, err := getEvents(gctx, newUserEventRequest(admin_events.EventTypeUserDeleted))
		if err != nil {
			return err
		}
		userDeleted = resp
		return nil
	})

	g.Go(func() error {
		// PasswordChanged
		resp, err := getEvents(gctx, newUserEventRequest(admin_events.EventTypeUserPasswordChanged))
		if err != nil {
			return err
		}
		passwordChanged = resp
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// Validate user existence and not deleted
	if userCreated == nil || len(userCreated.Events) == 0 {
		return errors.New("user not found")
	}
	if userDeleted != nil && len(userDeleted.Events) > 0 {
		return errors.New("user not found")
	}

	// Determine current password hash
	var currentPasswordHash string
	if passwordChanged != nil && len(passwordChanged.Events) > 0 {
		var passwordChangedEventData admin_events.UserPasswordChanged
		if err := json.Unmarshal([]byte(passwordChanged.Events[0].Data), &passwordChangedEventData); err != nil {
			return err
		}
		currentPasswordHash = passwordChangedEventData.PasswordHash
	} else {
		var userCreatedEventData admin_events.UserCreated
		if err := json.Unmarshal([]byte(userCreated.Events[0].Data), &userCreatedEventData); err != nil {
			return err
		}
		currentPasswordHash = userCreatedEventData.PasswordHash
	}

	// Verify current password
	if err := admin_common.ComparePassword(currentPasswordHash, currentPassword); err != nil {
		return ErrInvalidCurrentPassword
	}

	// Save password change event
	hash, err := admin_common.HashPassword(newPassword)
	if err != nil {
		return err
	}
	passwordChangedEvent := admin_events.UserPasswordChanged{
		UserId:       currentUserId,
		PasswordHash: hash,
	}

	payload, err := json.Marshal(passwordChangedEvent)
	if err != nil {
		return err
	}

	tags := []*orisun.Tag{
		{Key: "__eventType", Value: admin_events.EventTypeUserPasswordChanged},
		{Key: "user_id", Value: currentUserId},
	}

	var expectedPosition *orisun.Position = nil
	if passwordChanged != nil && len(passwordChanged.Events) > 0 {
		expectedPosition = passwordChanged.Events[0].Position
	}

	if expectedPosition == nil {
		expectedPosition = userCreated.Events[0].Position
	}
	_, err = saveEvents(ctx, &orisun.SaveEventsV2Request{
		Boundary: boundary, Consistency: []*orisun.ConsistencyObservation{{Position: expectedPosition, Query: &orisun.Query{Criteria: []*orisun.Criterion{{Tags: tags}}}}}, Events: []*orisun.EventToSave{{
			EventId:   uuid.NewString(),
			EventType: admin_events.EventTypeUserPasswordChanged,
			Data:      string(payload),
			Metadata:  "",
		}},
	})
	if err != nil {
		return err
	}

	return nil
}
