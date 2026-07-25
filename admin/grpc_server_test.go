package admin

import (
	"context"
	"testing"

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

	users, err := server.GetUserCount(context.Background(), &grpcapi.GetUserCountRequest{})
	if err != nil {
		t.Fatalf("GetUserCount returned an error: %v", err)
	}
	if users.Count != 7 {
		t.Fatalf("GetUserCount count = %d, want 7", users.Count)
	}

	events, err := server.GetEventCount(context.Background(), &grpcapi.GetEventCountRequest{
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
