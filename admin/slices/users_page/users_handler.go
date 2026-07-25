package users_page

import (
	"context"
	"github.com/OrisunLabs/Orisun/admin/slices/common"
	l "github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
)

type UsersPageHandler struct {
	logger                l.Logger
	boundary              string
	ListAdminUsers        func(ctx context.Context) ([]*orisun.User, error)
	subscribeToEventstore admin_common.SubscribeToEventStoreType
}

func NewUsersPageHandler(logger l.Logger, boundary string,
	listAdminUsers func(ctx context.Context) ([]*orisun.User, error),
	subscribeToEventstore admin_common.SubscribeToEventStoreType) *UsersPageHandler {
	return &UsersPageHandler{
		logger:                logger,
		boundary:              boundary,
		ListAdminUsers:        listAdminUsers,
		subscribeToEventstore: subscribeToEventstore,
	}
}
