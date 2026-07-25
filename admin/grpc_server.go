package admin

import (
	"context"
	"errors"
	"fmt"

	boundarycatalog "github.com/OrisunLabs/Orisun/admin/slices/boundary_catalog"
	changepassword "github.com/OrisunLabs/Orisun/admin/slices/change_password"
	createboundary "github.com/OrisunLabs/Orisun/admin/slices/create_boundary"
	createuser "github.com/OrisunLabs/Orisun/admin/slices/create_user"
	deleteuser "github.com/OrisunLabs/Orisun/admin/slices/delete_user"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/eventstoreadapter"
	"github.com/OrisunLabs/Orisun/internal/grpcstatus"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/orisun/grpcapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrUserNotFound     = errors.New("user not found")
	ErrCannotDeleteSelf = errors.New("cannot delete your own account")
)

// AdminServiceServer implements the Admin gRPC service
type AdminServiceServer struct {
	grpcapi.UnimplementedAdminServer
	logger         l.Logger
	boundary       string
	getEvents      GetEventsFunc
	saveEvents     SaveEventsFunc
	listAdminUser  ListAdminUsersFunc
	getUserCount   GetUserCountFunc
	getEventCount  GetEventCountFunc
	authenticator  CredentialsValidator
	sessionRevoker SessionRevoker
	boundaryEvents *eventstoreadapter.Adapter
}

var _ grpcapi.AdminServer = (*AdminServiceServer)(nil)

// GetEventsFunc is the function signature for getting events
type GetEventsFunc func(ctx context.Context, req *orisun.GetEventsRequest) (*orisun.GetEventsResponse, error)

// SaveEventsFunc is the function signature for saving events
type SaveEventsFunc func(ctx context.Context, req *orisun.SaveEventsRequest) (*orisun.WriteResult, error)

// ListAdminUsersFunc is the function signature for listing admin users
type ListAdminUsersFunc func(ctx context.Context) ([]*orisun.User, error)

type GetUserCountFunc func(ctx context.Context) (uint32, error)

type GetEventCountFunc func(ctx context.Context, boundary string) (int, error)

type CredentialsValidator interface {
	ValidateCredentials(context.Context, string, string) (orisun.User, string, error)
}

type CredentialsVerifier interface {
	VerifyCredentials(context.Context, string, string) (orisun.User, error)
}

type SessionRevoker interface {
	RevokeUserSessions(userID string) int
}

type GRPCAdminDependencies struct {
	GetEvents            GetEventsFunc
	SaveEvents           SaveEventsFunc
	ListAdminUsers       ListAdminUsersFunc
	GetUserCount         GetUserCountFunc
	GetEventCount        GetEventCountFunc
	CredentialsValidator CredentialsValidator
	BoundarySaver        orisun.EventsSaver
	BoundaryReader       orisun.EventsRetriever
}

// NewGRPCAdminServer creates a new AdminServiceServer
func NewGRPCAdminServer(
	logger l.Logger,
	boundary string,
	getEvents GetEventsFunc,
	saveEvents SaveEventsFunc,
	listAdminUsers ListAdminUsersFunc,
	authenticator CredentialsValidator,
) *AdminServiceServer {
	return NewGRPCAdminServerWithDependencies(logger, boundary, GRPCAdminDependencies{
		GetEvents:            getEvents,
		SaveEvents:           saveEvents,
		ListAdminUsers:       listAdminUsers,
		CredentialsValidator: authenticator,
	})
}

func NewGRPCAdminServerWithBoundaryCommands(
	logger l.Logger,
	boundary string,
	getEvents GetEventsFunc,
	saveEvents SaveEventsFunc,
	listAdminUsers ListAdminUsersFunc,
	authenticator CredentialsValidator,
	boundarySaver orisun.EventsSaver,
	boundaryReader orisun.EventsRetriever,
) *AdminServiceServer {
	return NewGRPCAdminServerWithDependencies(logger, boundary, GRPCAdminDependencies{
		GetEvents:            getEvents,
		SaveEvents:           saveEvents,
		ListAdminUsers:       listAdminUsers,
		CredentialsValidator: authenticator,
		BoundarySaver:        boundarySaver,
		BoundaryReader:       boundaryReader,
	})
}

func NewGRPCAdminServerWithDependencies(
	logger l.Logger,
	boundary string,
	dependencies GRPCAdminDependencies,
) *AdminServiceServer {
	server := &AdminServiceServer{
		logger:         logger,
		boundary:       boundary,
		getEvents:      dependencies.GetEvents,
		saveEvents:     dependencies.SaveEvents,
		listAdminUser:  dependencies.ListAdminUsers,
		getUserCount:   dependencies.GetUserCount,
		getEventCount:  dependencies.GetEventCount,
		authenticator:  dependencies.CredentialsValidator,
		boundaryEvents: eventstoreadapter.New(dependencies.BoundarySaver, dependencies.BoundaryReader, nil),
	}
	if revoker, ok := dependencies.CredentialsValidator.(SessionRevoker); ok {
		server.sessionRevoker = revoker
	}
	return server
}

// CreateBoundary emits the definition event. Physical provisioning is handled
// asynchronously by the boundary_provisioning slice subscriber.
func (s *AdminServiceServer) CreateBoundary(ctx context.Context, req *grpcapi.CreateBoundaryRequest) (*grpcapi.CreateBoundaryResponse, error) {
	if err := authorizeAdminRPC(ctx, orisun.RoleAdmin); err != nil {
		return nil, err
	}
	if req == nil || req.Placement == nil {
		return nil, status.Error(codes.InvalidArgument, "boundary placement is required")
	}
	result, err := createboundary.CreateBoundaryCommandHandler(
		ctx,
		createboundary.CreateBoundaryCommand{
			Name:        req.Name,
			Description: req.Description,
			Placement: boundarymodel.Placement{
				Backend:   req.Placement.Backend,
				Namespace: req.Placement.Namespace,
			},
			ExistedBeforeCatalog: req.ExistedBeforeCatalog,
			Metadata:             createboundary.CommandMetadata{"source": "grpc", "operation": "create_boundary"},
		},
		s.boundary,
		s.boundaryEvents,
		s.boundaryEvents,
	)
	if err != nil {
		return nil, grpcstatus.FromError(err)
	}
	return &grpcapi.CreateBoundaryResponse{Boundary: boundaryInfo(result.Boundary)}, nil
}

func (s *AdminServiceServer) ListBoundaries(ctx context.Context, _ *grpcapi.ListBoundariesRequest) (*grpcapi.ListBoundariesResponse, error) {
	if err := authorizeAdminRPC(ctx, orisun.RoleAdmin, orisun.RoleOperations); err != nil {
		return nil, err
	}
	boundaries, err := boundarycatalog.ListBoundariesQueryHandler(
		ctx,
		boundarycatalog.ListBoundariesQuery{},
		s.boundary,
		s.boundaryEvents,
	)
	if err != nil {
		return nil, grpcstatus.FromError(err)
	}
	response := &grpcapi.ListBoundariesResponse{Boundaries: make([]*grpcapi.BoundaryInfo, len(boundaries))}
	for i, boundary := range boundaries {
		response.Boundaries[i] = boundaryInfo(boundary)
	}
	return response, nil
}

func (s *AdminServiceServer) GetBoundary(ctx context.Context, req *grpcapi.GetBoundaryRequest) (*grpcapi.GetBoundaryResponse, error) {
	if err := authorizeAdminRPC(ctx, orisun.RoleAdmin, orisun.RoleOperations); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "boundary name is required")
	}
	boundary, err := boundarycatalog.GetBoundaryQueryHandler(
		ctx,
		boundarycatalog.GetBoundaryQuery{Name: req.Name},
		s.boundary,
		s.boundaryEvents,
	)
	if err != nil {
		return nil, grpcstatus.FromError(err)
	}
	return &grpcapi.GetBoundaryResponse{Boundary: boundaryInfo(boundary)}, nil
}

// CreateUser creates a new user with the given details
func (s *AdminServiceServer) CreateUser(ctx context.Context, req *grpcapi.CreateUserRequest) (*grpcapi.CreateUserResponse, error) {
	if err := authorizeAdminRPC(ctx, orisun.RoleAdmin); err != nil {
		return nil, err
	}
	// Validate request
	if err := s.validateCreateUserRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
	}

	// Convert string roles to Role types
	roles := make([]orisun.Role, len(req.Roles))
	for i, roleStr := range req.Roles {
		roles[i] = orisun.Role(roleStr)
	}

	// Create the user (this will use the existing create_user logic)
	user, err := s.createUser(ctx, req.Name, req.Username, req.Password, roles)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create user: %v", err)
	}

	s.logger.Infof("Created user: %s (ID: %s)", user.Username, user.Id)
	return &grpcapi.CreateUserResponse{User: convertToProtoUser(user)}, nil
}

// DeleteUser deletes a user by ID
func (s *AdminServiceServer) DeleteUser(ctx context.Context, req *grpcapi.DeleteUserRequest) (*grpcapi.DeleteUserResponse, error) {
	if err := authorizeAdminRPC(ctx, orisun.RoleAdmin); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.UserId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "user_id is required")
	}

	// Get current user from context (set by auth interceptor)
	currentUserId, err := getCurrentUserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if currentUserId == req.UserId {
		return nil, status.Errorf(codes.FailedPrecondition, "%s", ErrCannotDeleteSelf)
	}

	// Check if user exists
	user, err := s.getUserByID(ctx, req.UserId)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return nil, status.Errorf(codes.NotFound, "user not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to get user: %v", err)
	}

	// Check if user is already deleted
	if isUserDeleted(ctx, s.getEvents, s.boundary, req.UserId) {
		return nil, status.Errorf(codes.FailedPrecondition, "user already deleted")
	}

	// Delete the user
	if err := s.deleteUser(ctx, req.UserId, currentUserId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete user: %v", err)
	}
	s.revokeUserSessions(req.UserId)

	s.logger.Infof("Deleted user: %s (ID: %s)", user.Username, req.UserId)
	return &grpcapi.DeleteUserResponse{Success: true}, nil
}

// ChangePassword changes a user's password
func (s *AdminServiceServer) ChangePassword(ctx context.Context, req *grpcapi.ChangePasswordRequest) (*grpcapi.ChangePasswordResponse, error) {
	if err := authorizeAdminRPC(ctx); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.UserId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "user_id is required")
	}
	if req.CurrentPassword == "" {
		return nil, status.Errorf(codes.InvalidArgument, "current_password is required")
	}
	if req.NewPassword == "" {
		return nil, status.Errorf(codes.InvalidArgument, "new_password is required")
	}

	// Get current user from context
	currentUserId, err := getCurrentUserIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if currentUserId != req.UserId {
		return nil, status.Errorf(codes.PermissionDenied, "can only change your own password")
	}

	// Change password using existing logic
	if err := s.changeUserPassword(ctx, req); err != nil {
		if errors.Is(err, changepassword.ErrInvalidCurrentPassword) {
			return nil, status.Errorf(codes.Unauthenticated, "%s", err)
		}
		return nil, status.Errorf(codes.Internal, "failed to change password: %v", err)
	}
	s.revokeUserSessions(req.UserId)

	s.logger.Infof("Changed password for user: %s", req.UserId)
	return &grpcapi.ChangePasswordResponse{Success: true}, nil
}

// ListUsers returns all users
func (s *AdminServiceServer) ListUsers(ctx context.Context, _ *grpcapi.ListUsersRequest) (*grpcapi.ListUsersResponse, error) {
	if err := authorizeAdminRPC(ctx, orisun.RoleAdmin); err != nil {
		return nil, err
	}
	if s.listAdminUser == nil {
		return nil, status.Error(codes.Internal, "admin user store is not configured")
	}
	users, err := s.listAdminUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list users: %v", err)
	}

	// Convert to proto AdminUser format
	protoUsers := make([]*grpcapi.AdminUser, 0, len(users))
	for _, user := range users {
		// Skip deleted users
		if isUserDeleted(ctx, s.getEvents, s.boundary, user.Id) {
			continue
		}
		protoUsers = append(protoUsers, convertToProtoUser(user))
	}

	return &grpcapi.ListUsersResponse{Users: protoUsers}, nil
}

// ValidateCredentials validates username and password
func (s *AdminServiceServer) ValidateCredentials(ctx context.Context, req *grpcapi.ValidateCredentialsRequest) (*grpcapi.ValidateCredentialsResponse, error) {
	if err := authorizeAdminRPC(ctx, orisun.RoleAdmin); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.Username == "" || req.Password == "" {
		return nil, status.Errorf(codes.InvalidArgument, "username and password are required")
	}
	if s.authenticator == nil {
		return nil, status.Error(codes.Internal, "credentials validator is not configured")
	}

	// Credential checks do not need a session. Use the verification-only path
	// when available, while retaining compatibility with custom validators.
	var user orisun.User
	var err error
	if verifier, ok := s.authenticator.(CredentialsVerifier); ok {
		user, err = verifier.VerifyCredentials(ctx, req.Username, req.Password)
	} else {
		user, _, err = s.authenticator.ValidateCredentials(ctx, req.Username, req.Password)
	}
	if err != nil {
		// Return success: false instead of an error for invalid credentials
		return &grpcapi.ValidateCredentialsResponse{Success: false}, nil
	}

	// Convert to proto user
	protoUser := convertToProtoUser(&user)

	return &grpcapi.ValidateCredentialsResponse{
		Success: true,
		User:    protoUser,
	}, nil
}

// GetUserCount returns the total number of users
func (s *AdminServiceServer) GetUserCount(ctx context.Context, _ *grpcapi.GetUserCountRequest) (*grpcapi.GetUserCountResponse, error) {
	if err := authorizeAdminRPC(ctx, orisun.RoleAdmin); err != nil {
		return nil, err
	}
	if s.getUserCount != nil {
		count, err := s.getUserCount(ctx)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to count users: %v", err)
		}
		return &grpcapi.GetUserCountResponse{Count: int64(count)}, nil
	}
	if s.listAdminUser == nil {
		return nil, status.Error(codes.Internal, "admin user store is not configured")
	}
	users, err := s.listAdminUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to count users: %v", err)
	}

	return &grpcapi.GetUserCountResponse{Count: int64(len(users))}, nil
}

// GetEventCount returns the number of events in a boundary
func (s *AdminServiceServer) GetEventCount(ctx context.Context, req *grpcapi.GetEventCountRequest) (*grpcapi.GetEventCountResponse, error) {
	if err := authorizeAdminRPC(ctx, orisun.RoleAdmin, orisun.RoleOperations); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.Boundary == "" {
		return nil, status.Errorf(codes.InvalidArgument, "boundary is required")
	}
	if s.getEventCount == nil {
		return nil, status.Error(codes.Internal, "event count store is not configured")
	}
	count, err := s.getEventCount(ctx, req.Boundary)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to count events: %v", err)
	}
	return &grpcapi.GetEventCountResponse{Count: int64(count)}, nil
}

// Helper methods

func (s *AdminServiceServer) validateCreateUserRequest(req *grpcapi.CreateUserRequest) error {
	if req == nil {
		return fmt.Errorf("request is required")
	}
	if req.Name == "" {
		return fmt.Errorf("name is required")
	}
	if req.Username == "" {
		return fmt.Errorf("username is required")
	}
	if len(req.Username) < 3 {
		return fmt.Errorf("username must be at least 3 characters")
	}
	if req.Password == "" {
		return fmt.Errorf("password is required")
	}
	if len(req.Password) < 6 {
		return fmt.Errorf("password must be at least 6 characters")
	}
	if len(req.Roles) == 0 {
		return fmt.Errorf("at least one role is required")
	}
	for _, role := range req.Roles {
		switch orisun.Role(role) {
		case orisun.RoleAdmin, orisun.RoleOperations:
		default:
			return fmt.Errorf("unsupported role %q; valid roles are %s and %s", role, orisun.RoleAdmin, orisun.RoleOperations)
		}
	}
	return nil
}

func (s *AdminServiceServer) getUserByID(ctx context.Context, userID string) (*orisun.User, error) {
	if s.listAdminUser == nil {
		return nil, fmt.Errorf("admin user store is not configured")
	}
	users, err := s.listAdminUser(ctx)
	if err != nil {
		return nil, err
	}

	for _, user := range users {
		if user.Id == userID {
			return user, nil
		}
	}

	return nil, ErrUserNotFound
}

func (s *AdminServiceServer) createUser(ctx context.Context, name, username, password string, roles []orisun.Role) (*orisun.User, error) {
	// Call the exported CreateUser function from createuser package
	userCreated, err := createuser.CreateUser(
		ctx,
		name,
		username,
		password,
		roles,
		s.boundary,
		s.saveEvents,
		s.getEvents,
		s.logger,
		nil, // currentUserId is nil for system-created users
	)
	if err != nil {
		return nil, err
	}

	return &orisun.User{
		Id:       userCreated.UserId,
		Name:     userCreated.Name,
		Username: userCreated.Username,
		Roles:    userCreated.Roles,
	}, nil
}

func (s *AdminServiceServer) deleteUser(ctx context.Context, userID, currentUserID string) error {
	return deleteuser.DeleteUser(
		ctx,
		userID,
		currentUserID,
		s.boundary,
		s.saveEvents,
		s.getEvents,
		s.logger,
	)
}

func (s *AdminServiceServer) changeUserPassword(ctx context.Context, req *grpcapi.ChangePasswordRequest) error {
	return changepassword.ChangePassword(
		ctx,
		req.CurrentPassword,
		req.NewPassword,
		s.boundary,
		s.saveEvents,
		s.getEvents,
		s.logger,
		req.UserId,
	)
}

func (s *AdminServiceServer) revokeUserSessions(userID string) {
	if s.sessionRevoker == nil {
		return
	}
	s.sessionRevoker.RevokeUserSessions(userID)
}

func isUserDeleted(ctx context.Context, getEvents GetEventsFunc, boundary, userId string) bool {
	resp, err := getEvents(ctx, &orisun.GetEventsRequest{
		Boundary:  boundary,
		Count:     1,
		Direction: orisun.Direction_DESC,
		Query: &orisun.Query{
			Criteria: []*orisun.Criterion{
				{
					Tags: []*orisun.Tag{
						{Key: "user_id", Value: userId},
						{Key: "eventType", Value: "UserDeleted"},
					},
				},
			},
		},
	})

	return err == nil && len(resp.Events) > 0
}

func getCurrentUserIDFromContext(ctx context.Context) (string, error) {
	user, err := currentUserFromContext(ctx)
	if err != nil {
		return "", err
	}
	if user.Id == "" {
		return "", status.Error(codes.Unauthenticated, "authenticated user is missing from context")
	}
	return user.Id, nil
}

func authorizeAdminRPC(ctx context.Context, allowedRoles ...orisun.Role) error {
	user, err := currentUserFromContext(ctx)
	if err != nil {
		return err
	}
	if len(allowedRoles) == 0 {
		return nil
	}
	for _, userRole := range user.Roles {
		for _, allowedRole := range allowedRoles {
			if userRole == allowedRole {
				return nil
			}
		}
	}
	return status.Error(codes.PermissionDenied, "user does not have a role required for this admin operation")
}

func currentUserFromContext(ctx context.Context) (orisun.User, error) {
	switch user := ctx.Value(orisun.UserContextKey).(type) {
	case orisun.User:
		if user.Id != "" {
			return user, nil
		}
	case *orisun.User:
		if user != nil && user.Id != "" {
			return *user, nil
		}
	}
	return orisun.User{}, status.Error(codes.Unauthenticated, "authenticated user is missing from context")
}

func convertToProtoUser(user *orisun.User) *grpcapi.AdminUser {
	if user == nil {
		return nil
	}

	// Convert []Role to []string
	roles := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roles[i] = string(role)
	}

	return &grpcapi.AdminUser{
		UserId:    user.Id,
		Name:      user.Name,
		Username:  user.Username,
		Roles:     roles,
		CreatedAt: timestamppb.Now(),
	}
}

func boundaryInfo(boundary boundarymodel.Boundary) *grpcapi.BoundaryInfo {
	status := grpcapi.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_UNSPECIFIED
	switch boundary.Status {
	case boundarymodel.StatusProvisioning:
		status = grpcapi.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_PROVISIONING
	case boundarymodel.StatusActive:
		status = grpcapi.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_ACTIVE
	case boundarymodel.StatusFailed:
		status = grpcapi.BoundaryLifecycleStatus_BOUNDARY_LIFECYCLE_STATUS_FAILED
	}
	return &grpcapi.BoundaryInfo{
		Name:        boundary.Name,
		Description: boundary.Description,
		Placement: &grpcapi.BoundaryPlacementInput{
			Backend:   boundary.Placement.Backend,
			Namespace: boundary.Placement.Namespace,
		},
		Status:               status,
		ExistedBeforeCatalog: boundary.ExistedBeforeCatalog,
		LastError:            boundary.LastError,
		DefinitionPosition:   grpcPosition(boundary.DefinitionPosition),
		StatusPosition:       grpcPosition(boundary.StatusPosition),
	}
}

func grpcPosition(position *coreeventstore.Position) *grpcapi.Position {
	if position == nil {
		return nil
	}
	return &grpcapi.Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition,
	}
}
