//go:build !foundationdb

package foundationdb

import (
	"context"
	"fmt"

	config "github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
)

func InitializeFoundationDBRuntime(
	ctx context.Context,
	fdbCfg config.FoundationDBConfig,
	adminCfg config.AdminConfig,
	logger logging.Logger,
) (*DatabaseRuntime, error) {
	return nil, fmt.Errorf("foundationdb backend requires building with -tags foundationdb and installed FoundationDB client libraries")
}
