package admin

import (
	"context"
	"fmt"
	admin_events "github.com/OrisunLabs/Orisun/admin/events"
	admin_common "github.com/OrisunLabs/Orisun/admin/slices/common"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/orisun"
	"sync"
	"time"

	logger "github.com/OrisunLabs/Orisun/logging"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

type Authenticator struct {
	boundary          string
	getEvents         admin_common.GetEventsType
	logger            logger.Logger
	getUserByUsername func(ctx context.Context, username string) (orisun.User, error)
}

func NewAuthenticator(
	getEvents admin_common.GetEventsType,
	logger logger.Logger,
	boundary string,
	getUserByUsername func(ctx context.Context, username string) (orisun.User, error),
) *Authenticator {
	return &Authenticator{
		boundary:          boundary,
		getEvents:         getEvents,
		logger:            logger,
		getUserByUsername: getUserByUsername,
	}
}

var userByIdCache = map[string]*orisun.User{}
var userByUsernameCache = map[string]*orisun.User{}
var userTokenCache = map[string]*orisun.User{}
var cacheMutex sync.RWMutex

func (a *Authenticator) ValidateToken(ctx context.Context, token string) (*orisun.User, error) {
	// Check if the user is in the cache
	user, ok := userTokenCache[token]

	if ok && user != nil {
		if a.logger.IsDebugEnabled() {
			a.logger.Debugf("Fetched user from cache by token")
		}
		return user, nil
	}
	return nil, fmt.Errorf("invalid credentials")
}

func (a *Authenticator) ValidateCredentials(ctx context.Context, username string, password string) (orisun.User, string, error) {
	// Check if the user is in the cache
	// cacheMutex.Lock()
	// defer cacheMutex.Unlock()
	// user, ok := userByUsernameCache[username]

	// if ok && user.Username == username {
	// 	// Compare the provided password with the stored hash
	// 	if err := admin_common.ComparePassword(user.HashedPassword, password); err != nil {
	// 		return orisun.User{}, "", fmt.Errorf("invalid credentials")
	// 	}
	// 	a.logger.Debugf("Fetched user from cache")
	// 	return *user, userByIdCache[], nil
	// }

	userr, errr := a.getUserByUsername(ctx, username)
	if errr != nil {
		a.logger.Errorf("Could not retrieve user %v", errr)
		return orisun.User{}, "", fmt.Errorf("invalid credentials")
	}

	// Compare the provided password with the stored hash
	if err := admin_common.ComparePassword(userr.HashedPassword, password); err != nil {
		return orisun.User{}, "", fmt.Errorf("invalid password")
	}

	// generate a session token for the user
	generatredToken := uuid.New().String()
	if a.logger.IsDebugEnabled() {
		a.logger.Debugf("Generated session token %s for user %s", generatredToken, userr.Username)
	}

	// Cache the user
	// go func() {
	// userByIdCache[userr.Id] = &userr
	// userByUsernameCache[userr.Username] = &userr
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	userTokenCache[generatredToken] = &userr
	// }()

	return userr, generatredToken, nil
}

type AuthUserProjector struct {
	boundary          string
	logger            logger.Logger
	subscribeToEvents admin_common.SubscribeToEventStoreType
}

func NewAuthUserProjector(
	logger logger.Logger,
	subscribeToEvents admin_common.SubscribeToEventStoreType,
	boundary string,
) *AuthUserProjector {
	return &AuthUserProjector{
		boundary:          boundary,
		logger:            logger,
		subscribeToEvents: subscribeToEvents,
	}
}

func (p *AuthUserProjector) Start(ctx context.Context) error {
	var projectorName = "auth-user-projector-" + uuid.New().String()
	p.logger.Info("Starting auth user projector %s", projectorName)

	return p.subscribeToEvents(
		ctx,
		coreeventstore.SubscribeRequest{
			Boundary:       p.boundary,
			SubscriberName: projectorName,
		},
		func(ctx context.Context, event coreeventstore.ReadEvent) error {
			for {
				if err := p.handleEvent(event); err != nil {
					p.logger.Error("Error handling event: %v", err)

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

func (p *AuthUserProjector) handleEvent(event coreeventstore.ReadEvent) error {
	if p.logger.IsDebugEnabled() {
		p.logger.Debugf("Handling event %v", event)
	}

	switch event.EventType {
	case admin_events.EventTypeUserPasswordChanged:
		var userEvent admin_events.UserPasswordChanged
		if err := json.Unmarshal([]byte(event.Data), &userEvent); err != nil {
			return err
		}

		if userByIdCache[userEvent.UserId] != nil {
			if p.logger.IsDebugEnabled() {
				p.logger.Debugf("Updating password hash for user %s", userEvent.UserId)
			}
			cacheMutex.Lock()
			defer cacheMutex.Unlock()
			userByIdCache[userEvent.UserId].HashedPassword = userEvent.PasswordHash
			userByUsernameCache[userByIdCache[userEvent.UserId].Username].HashedPassword = userEvent.PasswordHash
		}

	case admin_events.EventTypeUserDeleted:
		var userEvent admin_events.UserDeleted
		if err := json.Unmarshal([]byte(event.Data), &userEvent); err != nil {
			return err
		}

		cacheMutex.Lock()
		defer cacheMutex.Unlock()
		if userByIdCache[userEvent.UserId] != nil {
			p.logger.Infof("Deleting user %s from cache", userEvent.UserId)
			delete(userByUsernameCache, userByIdCache[userEvent.UserId].Username)
			delete(userByIdCache, userEvent.UserId)
		}
	}
	return nil
}
