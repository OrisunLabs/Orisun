package admin_common

import (
	"context"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/orisun"
	"golang.org/x/crypto/bcrypt"
	"net/http"
)

type IndexField = orisun.BoundaryIndexField
type IndexCondition = orisun.BoundaryIndexCondition

const CombinatorAND = orisun.IndexCombinatorAND
const CombinatorOR = orisun.IndexCombinatorOR

type DB interface {
	ListAdminUsers(ctx context.Context) ([]*orisun.User, error)
	GetProjectorLastPosition(ctx context.Context, projectorName string) (*orisun.Position, error)
	UpdateProjectorPosition(ctx context.Context, name string, position *orisun.Position) error
	UpsertUser(ctx context.Context, user orisun.User) error
	DeleteUser(ctx context.Context, id string) error
	GetUserByUsername(ctx context.Context, username string) (orisun.User, error)
	GetUserById(ctx context.Context, username string) (orisun.User, error)
	GetUsersCount(ctx context.Context) (uint32, error)
	SaveUsersCount(ctx context.Context, count uint32) error
	GetEventsCount(ctx context.Context, boundary string) (int, error)
	SaveEventCount(ctx context.Context, count int, boundary string) error
	orisun.BoundaryIndexManager
}

type SaveEventsType = func(ctx context.Context, in *orisun.SaveEventsRequest) (resp *orisun.WriteResult, err error)
type GetEventsType = func(ctx context.Context, in *orisun.GetEventsRequest) (*orisun.GetEventsResponse, error)
type GetProjectorLastPositionType = func(ctx context.Context, projectorName string) (*orisun.Position, error)
type UpdateProjectorPositionType = func(ctx context.Context, projectorName string, position *orisun.Position) error
type SubscribeToEventStoreType = func(
	ctx context.Context,
	request coreeventstore.SubscribeRequest,
	handler coreeventstore.EventHandler,
) error

type PublishRequest struct {
	Id      string `json:"id"`
	Subject string `json:"subject"`
	Data    []byte `json:"data"`
}

type PublishToPubSubType = func(ctx context.Context, req *PublishRequest) error

func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func GetCurrentUser(r *http.Request) *orisun.User {
	currentUser := r.Context().Value(orisun.UserContextKey).(orisun.User)
	if currentUser.Id != "" {
		return &currentUser
	}
	return nil
}

func ComparePassword(hashedPassword string, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
}
