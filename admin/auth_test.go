package admin

import (
	"context"
	"errors"
	"testing"
	"time"

	adminevents "github.com/OrisunLabs/Orisun/admin/events"
	admincommon "github.com/OrisunLabs/Orisun/admin/slices/common"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/orisun"

	"github.com/goccy/go-json"
)

func TestAuthenticatorSessionExpiresAfterInactivity(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)
	auth := newTestAuthenticator(t, 10*time.Minute, map[string]orisun.User{
		"admin": testAuthUser(t, "user-1", "admin", "changeit"),
	})
	auth.now = func() time.Time { return now }

	_, token, err := auth.ValidateCredentials(t.Context(), "admin", "changeit")
	if err != nil {
		t.Fatalf("ValidateCredentials returned an error: %v", err)
	}

	now = now.Add(10 * time.Minute)
	if _, err := auth.ValidateToken(t.Context(), token); err == nil {
		t.Fatal("expired token was accepted")
	}
}

func TestAuthenticatorSessionTTLSlidesOnUse(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)
	now := start
	auth := newTestAuthenticator(t, 10*time.Minute, map[string]orisun.User{
		"admin": testAuthUser(t, "user-1", "admin", "changeit"),
	})
	auth.now = func() time.Time { return now }

	_, token, err := auth.ValidateCredentials(t.Context(), "admin", "changeit")
	if err != nil {
		t.Fatalf("ValidateCredentials returned an error: %v", err)
	}

	now = start.Add(9 * time.Minute)
	if _, err := auth.ValidateToken(t.Context(), token); err != nil {
		t.Fatalf("active token was rejected: %v", err)
	}
	now = start.Add(18 * time.Minute)
	if _, err := auth.ValidateToken(t.Context(), token); err != nil {
		t.Fatalf("renewed token was rejected: %v", err)
	}
	now = start.Add(29 * time.Minute)
	if _, err := auth.ValidateToken(t.Context(), token); err == nil {
		t.Fatal("inactive token was accepted after its renewed deadline")
	}
}

func TestAuthenticatorRevokesAllSessionsForUser(t *testing.T) {
	t.Parallel()

	users := map[string]orisun.User{
		"admin": testAuthUser(t, "user-1", "admin", "changeit"),
		"ops":   testAuthUser(t, "user-2", "ops", "changeit"),
	}
	auth := newTestAuthenticator(t, time.Hour, users)

	_, firstAdminToken, err := auth.ValidateCredentials(t.Context(), "admin", "changeit")
	if err != nil {
		t.Fatal(err)
	}
	_, secondAdminToken, err := auth.ValidateCredentials(t.Context(), "admin", "changeit")
	if err != nil {
		t.Fatal(err)
	}
	_, opsToken, err := auth.ValidateCredentials(t.Context(), "ops", "changeit")
	if err != nil {
		t.Fatal(err)
	}

	if revoked := auth.RevokeUserSessions("user-1"); revoked != 2 {
		t.Fatalf("revoked sessions = %d, want 2", revoked)
	}
	for _, token := range []string{firstAdminToken, secondAdminToken} {
		if _, err := auth.ValidateToken(t.Context(), token); err == nil {
			t.Fatal("revoked token was accepted")
		}
	}
	if _, err := auth.ValidateToken(t.Context(), opsToken); err != nil {
		t.Fatalf("unrelated user's token was revoked: %v", err)
	}
}

func TestAuthenticatorSessionsAreInstanceLocal(t *testing.T) {
	t.Parallel()

	users := map[string]orisun.User{
		"admin": testAuthUser(t, "user-1", "admin", "changeit"),
	}
	first := newTestAuthenticator(t, time.Hour, users)
	second := newTestAuthenticator(t, time.Hour, users)

	_, token, err := first.ValidateCredentials(t.Context(), "admin", "changeit")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.ValidateToken(t.Context(), token); err == nil {
		t.Fatal("token issued by another authenticator instance was accepted")
	}
}

func TestAuthUserProjectorRevokesSessionsForSecurityEvents(t *testing.T) {
	t.Parallel()

	var revoked []string
	projector := NewAuthUserProjector(authTestLogger{}, nil, "orisun_admin", func(userID string) int {
		revoked = append(revoked, userID)
		return 1
	})

	for _, eventType := range []string{
		adminevents.EventTypeUserPasswordChanged,
		adminevents.EventTypeUserDeleted,
		adminevents.EventTypeRolesChanged,
	} {
		data, err := json.Marshal(map[string]any{"user_id": "user-1"})
		if err != nil {
			t.Fatal(err)
		}
		if err := projector.handleEvent(coreeventstore.ReadEvent{
			EventType: eventType,
			Data:      string(data),
		}); err != nil {
			t.Fatalf("handleEvent(%s) returned an error: %v", eventType, err)
		}
	}

	if len(revoked) != 3 {
		t.Fatalf("revocation calls = %d, want 3", len(revoked))
	}
	for _, userID := range revoked {
		if userID != "user-1" {
			t.Fatalf("revoked user = %q, want user-1", userID)
		}
	}
}

func newTestAuthenticator(t *testing.T, ttl time.Duration, users map[string]orisun.User) *Authenticator {
	t.Helper()
	return NewAuthenticator(
		nil,
		authTestLogger{},
		"orisun_admin",
		func(_ context.Context, username string) (orisun.User, error) {
			user, ok := users[username]
			if !ok {
				return orisun.User{}, errors.New("user not found")
			}
			return user, nil
		},
		ttl,
	)
}

func testAuthUser(t *testing.T, id, username, password string) orisun.User {
	t.Helper()
	hash, err := admincommon.HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword returned an error: %v", err)
	}
	return orisun.User{
		Id:             id,
		Username:       username,
		HashedPassword: hash,
		Roles:          []orisun.Role{orisun.RoleAdmin},
	}
}

type authTestLogger struct{}

func (authTestLogger) IsDebugEnabled() bool  { return false }
func (authTestLogger) Debug(...any)          {}
func (authTestLogger) Debugf(string, ...any) {}
func (authTestLogger) Info(...any)           {}
func (authTestLogger) Infof(string, ...any)  {}
func (authTestLogger) Warn(...any)           {}
func (authTestLogger) Warnf(string, ...any)  {}
func (authTestLogger) Error(...any)          {}
func (authTestLogger) Errorf(string, ...any) {}
func (authTestLogger) Fatal(...any)          {}
func (authTestLogger) Fatalf(string, ...any) {}
