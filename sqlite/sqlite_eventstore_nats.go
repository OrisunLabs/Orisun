//go:build !orisun_embedded

package sqlite

import (
	"context"
	"fmt"

	config "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/nats-io/nats.go/jetstream"
)

func InitializeSqliteDatabaseRuntime(
	ctx context.Context,
	sqliteCfg config.SqliteConfig,
	adminCfg config.AdminConfig,
	js jetstream.JetStream,
	logger logging.Logger,
) (*DatabaseRuntime, error) {
	lockProvider, err := eventstore.NewJetStreamLockProvider(ctx, js, logger)
	if err != nil {
		return nil, fmt.Errorf("init lock provider: %w", err)
	}
	return InitializeSqliteDatabaseRuntimeWithLockProvider(
		ctx, sqliteCfg, adminCfg, lockProvider, logger,
	)
}
