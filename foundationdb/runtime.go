package foundationdb

import (
	"context"

	common "github.com/OrisunLabs/Orisun/admin/slices/common"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	"github.com/OrisunLabs/Orisun/orisun"
)

type DatabaseRuntime struct {
	SaveEvents        orisun.EventsSaver
	GetEvents         orisun.EventsRetriever
	LockProvider      orisun.LockProvider
	AdminDB           common.DB
	EventPublishing   orisun.EventPublishingTracker
	SignalProvider    func(string) orisun.EventSignal
	ProvisionBoundary func(context.Context, boundarymodel.Definition) error
	InstallBoundary   func(context.Context, boundarymodel.Definition) error
	Close             func(context.Context)
}
