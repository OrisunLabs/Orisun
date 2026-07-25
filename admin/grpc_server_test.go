package admin

import (
	"context"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminServiceUsesCountDependencies(t *testing.T) {
	t.Parallel()

	var countedBoundary string
	server := NewGRPCAdminServerWithDependencies(nil, "orisun_admin", GRPCAdminDependencies{
		GetUserCount: func(ctx context.Context) (uint32, error) {
			return 7, nil
		},
		GetEventCount: func(ctx context.Context, boundary string) (int, error) {
			countedBoundary = boundary
			return 42, nil
		},
	})

	users, err := server.GetUserCount(adminTestContext(), &grpcapi.GetUserCountRequest{})
	if err != nil {
		t.Fatalf("GetUserCount returned an error: %v", err)
	}
	if users.Count != 7 {
		t.Fatalf("GetUserCount count = %d, want 7", users.Count)
	}

	events, err := server.GetEventCount(adminTestContext(), &grpcapi.GetEventCountRequest{
		Boundary: "orders",
	})
	if err != nil {
		t.Fatalf("GetEventCount returned an error: %v", err)
	}
	if events.Count != 42 {
		t.Fatalf("GetEventCount count = %d, want 42", events.Count)
	}
	if countedBoundary != "orders" {
		t.Fatalf("GetEventCount boundary = %q, want orders", countedBoundary)
	}
}

func TestAdminServiceUsesAuthenticatorAsSessionRevoker(t *testing.T) {
	t.Parallel()

	auth := &revokingCredentialsValidator{}
	server := NewGRPCAdminServerWithDependencies(nil, "orisun_admin", GRPCAdminDependencies{
		CredentialsValidator: auth,
	})

	server.revokeUserSessions("user-1")

	if len(auth.revokedUserIDs) != 1 || auth.revokedUserIDs[0] != "user-1" {
		t.Fatalf("revoked user IDs = %v, want [user-1]", auth.revokedUserIDs)
	}
}

func TestChangePasswordRequiresAuthenticatedUserContext(t *testing.T) {
	t.Parallel()

	server := NewGRPCAdminServerWithDependencies(nil, "orisun_admin", GRPCAdminDependencies{})
	_, err := server.ChangePassword(context.Background(), &grpcapi.ChangePasswordRequest{
		UserId:          "user-1",
		CurrentPassword: "old-password",
		NewPassword:     "new-password",
	})
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("ChangePassword error code = %s, want %s", got, codes.Unauthenticated)
	}
}

func TestCurrentUserIDFromContext(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), orisun.UserContextKey, orisun.User{Id: "user-1"})
	userID, err := getCurrentUserIDFromContext(ctx)
	if err != nil {
		t.Fatalf("getCurrentUserIDFromContext returned an error: %v", err)
	}
	if userID != "user-1" {
		t.Fatalf("user ID = %q, want user-1", userID)
	}
}

func TestAuthorizeAdminRPC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		ctx   context.Context
		roles []orisun.Role
		want  codes.Code
	}{
		{
			name:  "missing user",
			ctx:   context.Background(),
			roles: []orisun.Role{orisun.RoleAdmin},
			want:  codes.Unauthenticated,
		},
		{
			name: "empty user identity",
			ctx: context.WithValue(context.Background(), orisun.UserContextKey, orisun.User{
				Roles: []orisun.Role{orisun.RoleAdmin},
			}),
			roles: []orisun.Role{orisun.RoleAdmin},
			want:  codes.Unauthenticated,
		},
		{
			name:  "admin allowed",
			ctx:   adminTestContext(),
			roles: []orisun.Role{orisun.RoleAdmin},
			want:  codes.OK,
		},
		{
			name:  "operations denied admin mutation",
			ctx:   operationsTestContext(),
			roles: []orisun.Role{orisun.RoleAdmin},
			want:  codes.PermissionDenied,
		},
		{
			name:  "operations allowed inventory",
			ctx:   operationsTestContext(),
			roles: []orisun.Role{orisun.RoleAdmin, orisun.RoleOperations},
			want:  codes.OK,
		},
		{
			name: "authenticated self service",
			ctx:  operationsTestContext(),
			want: codes.OK,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := status.Code(authorizeAdminRPC(test.ctx, test.roles...)); got != test.want {
				t.Fatalf("authorizeAdminRPC() code = %s, want %s", got, test.want)
			}
		})
	}
}

func TestAdminRPCsEnforceRoles(t *testing.T) {
	t.Parallel()

	server := NewGRPCAdminServerWithDependencies(nil, "orisun_admin", GRPCAdminDependencies{})
	tests := []struct {
		name   string
		invoke func(context.Context) error
	}{
		{
			name: "create boundary",
			invoke: func(ctx context.Context) error {
				_, err := server.CreateBoundary(ctx, &grpcapi.CreateBoundaryRequest{})
				return err
			},
		},
		{
			name: "create user",
			invoke: func(ctx context.Context) error {
				_, err := server.CreateUser(ctx, &grpcapi.CreateUserRequest{})
				return err
			},
		},
		{
			name: "delete user",
			invoke: func(ctx context.Context) error {
				_, err := server.DeleteUser(ctx, &grpcapi.DeleteUserRequest{})
				return err
			},
		},
		{
			name: "list users",
			invoke: func(ctx context.Context) error {
				_, err := server.ListUsers(ctx, &grpcapi.ListUsersRequest{})
				return err
			},
		},
		{
			name: "validate credentials",
			invoke: func(ctx context.Context) error {
				_, err := server.ValidateCredentials(ctx, &grpcapi.ValidateCredentialsRequest{})
				return err
			},
		},
		{
			name: "get user count",
			invoke: func(ctx context.Context) error {
				_, err := server.GetUserCount(ctx, &grpcapi.GetUserCountRequest{})
				return err
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := status.Code(test.invoke(operationsTestContext())); got != codes.PermissionDenied {
				t.Fatalf("RPC code = %s, want %s", got, codes.PermissionDenied)
			}
		})
	}
}

func TestValidateCreateUserRequestRejectsUnknownRoles(t *testing.T) {
	t.Parallel()

	server := NewGRPCAdminServerWithDependencies(nil, "orisun_admin", GRPCAdminDependencies{})
	base := &grpcapi.CreateUserRequest{
		Name:     "Application",
		Username: "application",
		Password: "password",
	}

	for _, roles := range [][]string{{"admin"}, {"READER"}, {"OPERATIONS", "WRITER"}} {
		req := *base
		req.Roles = roles
		if err := server.validateCreateUserRequest(&req); err == nil {
			t.Fatalf("validateCreateUserRequest() accepted roles %v", roles)
		}
	}

	for _, roles := range [][]string{{"ADMIN"}, {"OPERATIONS"}, {"ADMIN", "OPERATIONS"}} {
		req := *base
		req.Roles = roles
		if err := server.validateCreateUserRequest(&req); err != nil {
			t.Fatalf("validateCreateUserRequest() rejected roles %v: %v", roles, err)
		}
	}
}

func TestValidateCredentialsUsesVerificationWithoutIssuingSession(t *testing.T) {
	t.Parallel()

	auth := newTestAuthenticator(t, time.Hour, map[string]orisun.User{
		"admin": testAuthUser(t, "user-1", "admin", "changeit"),
	})
	server := NewGRPCAdminServerWithDependencies(nil, "orisun_admin", GRPCAdminDependencies{
		CredentialsValidator: auth,
	})

	response, err := server.ValidateCredentials(adminTestContext(), &grpcapi.ValidateCredentialsRequest{
		Username: "admin",
		Password: "changeit",
	})
	if err != nil {
		t.Fatalf("ValidateCredentials() error = %v", err)
	}
	if !response.Success || response.User.GetUserId() != "user-1" {
		t.Fatalf("ValidateCredentials() response = %#v", response)
	}
	if got := len(auth.sessions); got != 0 {
		t.Fatalf("ValidateCredentials() issued %d sessions, want 0", got)
	}
}

func TestGetUserByIDReturnsNotFound(t *testing.T) {
	t.Parallel()

	server := NewGRPCAdminServerWithDependencies(nil, "orisun_admin", GRPCAdminDependencies{
		ListAdminUsers: func(ctx context.Context) ([]*orisun.User, error) {
			return []*orisun.User{{Id: "user-1"}}, nil
		},
	})
	_, err := server.getUserByID(context.Background(), "missing")
	if err != ErrUserNotFound {
		t.Fatalf("getUserByID error = %v, want %v", err, ErrUserNotFound)
	}
}

func adminTestContext() context.Context {
	return context.WithValue(context.Background(), orisun.UserContextKey, orisun.User{
		Id:    "admin-1",
		Roles: []orisun.Role{orisun.RoleAdmin},
	})
}

func operationsTestContext() context.Context {
	return context.WithValue(context.Background(), orisun.UserContextKey, orisun.User{
		Id:    "operations-1",
		Roles: []orisun.Role{orisun.RoleOperations},
	})
}

type revokingCredentialsValidator struct {
	revokedUserIDs []string
}

func (*revokingCredentialsValidator) ValidateCredentials(context.Context, string, string) (orisun.User, string, error) {
	return orisun.User{}, "", nil
}

func (v *revokingCredentialsValidator) RevokeUserSessions(userID string) int {
	v.revokedUserIDs = append(v.revokedUserIDs, userID)
	return 1
}
