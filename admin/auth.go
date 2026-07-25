package admin

import (
	"context"
	"fmt"
	"sync"
	"time"

	admin_events "github.com/OrisunLabs/Orisun/admin/events"
	admin_common "github.com/OrisunLabs/Orisun/admin/slices/common"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	logger "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

const (
	DefaultSessionTTL         = 24 * time.Hour
	DefaultMaxSessionsPerUser = 16
)

type authenticatedSession struct {
	user      orisun.User
	expiresAt time.Time
}

type Authenticator struct {
	boundary          string
	getEvents         admin_common.GetEventsType
	logger            logger.Logger
	getUserByUsername func(ctx context.Context, username string) (orisun.User, error)
	sessionTTL        time.Duration
	maxUserSessions   int
	now               func() time.Time
	sessionMutex      sync.Mutex
	sessions          map[string]authenticatedSession
}

func NewAuthenticator(
	getEvents admin_common.GetEventsType,
	logger logger.Logger,
	boundary string,
	getUserByUsername func(ctx context.Context, username string) (orisun.User, error),
	sessionTTL ...time.Duration,
) *Authenticator {
	ttl := DefaultSessionTTL
	if len(sessionTTL) > 0 && sessionTTL[0] > 0 {
		ttl = sessionTTL[0]
	}
	return &Authenticator{
		boundary:          boundary,
		getEvents:         getEvents,
		logger:            logger,
		getUserByUsername: getUserByUsername,
		sessionTTL:        ttl,
		maxUserSessions:   DefaultMaxSessionsPerUser,
		now:               time.Now,
		sessions:          make(map[string]authenticatedSession),
	}
}

func (a *Authenticator) ValidateToken(ctx context.Context, token string) (*orisun.User, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := a.now()
	a.sessionMutex.Lock()
	session, ok := a.sessions[token]
	if !ok || !now.Before(session.expiresAt) {
		delete(a.sessions, token)
		a.sessionMutex.Unlock()
		return nil, fmt.Errorf("invalid or expired credentials")
	}
	session.expiresAt = now.Add(a.sessionTTL)
	a.sessions[token] = session
	a.sessionMutex.Unlock()

	if a.logger.IsDebugEnabled() {
		a.logger.Debugf("Validated session token for user %s", session.user.Username)
	}
	user := session.user
	return &user, nil
}

func (a *Authenticator) ValidateCredentials(ctx context.Context, username string, password string) (orisun.User, string, error) {
	user, err := a.VerifyCredentials(ctx, username, password)
	if err != nil {
		return orisun.User{}, "", err
	}

	token := uuid.New().String()
	if a.logger.IsDebugEnabled() {
		a.logger.Debugf("Generated session token for user %s", user.Username)
	}

	now := a.now()
	a.sessionMutex.Lock()
	a.deleteExpiredSessionsLocked(now)
	a.enforceUserSessionLimitLocked(user.Id)
	a.sessions[token] = authenticatedSession{
		user:      user,
		expiresAt: now.Add(a.sessionTTL),
	}
	a.sessionMutex.Unlock()

	return user, token, nil
}

// VerifyCredentials validates a username and password without issuing a
// session. Administrative credential checks use this path so their result does
// not leave an unreachable live token behind.
func (a *Authenticator) VerifyCredentials(ctx context.Context, username string, password string) (orisun.User, error) {
	user, err := a.getUserByUsername(ctx, username)
	if err != nil {
		a.logger.Errorf("Could not retrieve user %v", err)
		return orisun.User{}, fmt.Errorf("invalid credentials")
	}

	// Compare the provided password with the stored hash
	if err := admin_common.ComparePassword(user.HashedPassword, password); err != nil {
		return orisun.User{}, fmt.Errorf("invalid password")
	}

	return user, nil
}

// RevokeUserSessions invalidates every session issued for userID and returns
// the number of sessions removed.
func (a *Authenticator) RevokeUserSessions(userID string) int {
	a.sessionMutex.Lock()
	defer a.sessionMutex.Unlock()

	revoked := 0
	for token, session := range a.sessions {
		if session.user.Id == userID {
			delete(a.sessions, token)
			revoked++
		}
	}
	return revoked
}

func (a *Authenticator) deleteExpiredSessionsLocked(now time.Time) {
	for token, session := range a.sessions {
		if !now.Before(session.expiresAt) {
			delete(a.sessions, token)
		}
	}
}

func (a *Authenticator) enforceUserSessionLimitLocked(userID string) {
	if a.maxUserSessions <= 0 {
		return
	}
	for {
		count := 0
		oldestToken := ""
		var oldestExpiry time.Time
		for token, session := range a.sessions {
			if session.user.Id != userID {
				continue
			}
			count++
			if oldestToken == "" || session.expiresAt.Before(oldestExpiry) {
				oldestToken = token
				oldestExpiry = session.expiresAt
			}
		}
		if count < a.maxUserSessions {
			return
		}
		delete(a.sessions, oldestToken)
	}
}

type AuthUserProjector struct {
	boundary          string
	logger            logger.Logger
	subscribeToEvents admin_common.SubscribeToEventStoreType
	revokeSessions    func(string) int
}

func NewAuthUserProjector(
	logger logger.Logger,
	subscribeToEvents admin_common.SubscribeToEventStoreType,
	boundary string,
	revokeSessions ...func(string) int,
) *AuthUserProjector {
	projector := &AuthUserProjector{
		boundary:          boundary,
		logger:            logger,
		subscribeToEvents: subscribeToEvents,
	}
	if len(revokeSessions) > 0 {
		projector.revokeSessions = revokeSessions[0]
	}
	return projector
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
		p.revokeUserSessions(userEvent.UserId)

	case admin_events.EventTypeUserDeleted:
		var userEvent admin_events.UserDeleted
		if err := json.Unmarshal([]byte(event.Data), &userEvent); err != nil {
			return err
		}
		p.revokeUserSessions(userEvent.UserId)

	// No supported role-mutation command emits this event yet. Keep the
	// revocation behavior ready for that event contract when one is added.
	case admin_events.EventTypeRolesChanged:
		var userEvent struct {
			UserId string `json:"user_id"`
		}
		if err := json.Unmarshal([]byte(event.Data), &userEvent); err != nil {
			return err
		}
		p.revokeUserSessions(userEvent.UserId)
	}
	return nil
}

func (p *AuthUserProjector) revokeUserSessions(userID string) {
	if p.revokeSessions == nil || userID == "" {
		return
	}
	revoked := p.revokeSessions(userID)
	if revoked > 0 && p.logger.IsDebugEnabled() {
		p.logger.Debugf("Revoked %d sessions for user %s", revoked, userID)
	}
}
