package boundary_provisioning

import (
	"context"

	boundarycatalog "github.com/OrisunLabs/Orisun/admin/slices/boundary_catalog"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
)

// InstallBoundary installs an already-provisioned boundary into one process's
// local backend registry, wake-up listener, publisher, and projectors.
type InstallBoundary func(ctx context.Context, definition boundarymodel.Definition) error

// ActivateBoundary exposes a locally installed boundary to public requests in
// one process.
type ActivateBoundary func(ctx context.Context, boundary string) error

type BoundaryRuntimeEventHandler struct {
	adminBoundary string
	retriever     DefinitionEventsRetriever
	install       InstallBoundary
	activate      ActivateBoundary
}

func NewBoundaryRuntimeEventHandler(
	adminBoundary string,
	retriever DefinitionEventsRetriever,
	install InstallBoundary,
	activate ActivateBoundary,
) *BoundaryRuntimeEventHandler {
	return &BoundaryRuntimeEventHandler{
		adminBoundary: adminBoundary,
		retriever:     retriever,
		install:       install,
		activate:      activate,
	}
}

func (h *BoundaryRuntimeEventHandler) Handle(ctx context.Context, event coreeventstore.ReadEvent) error {
	if h == nil || h.adminBoundary == "" || h.retriever == nil || h.install == nil || h.activate == nil {
		return statuscode.New(statuscode.Internal, "boundary runtime event handler is not configured")
	}
	if event.EventType != boundaryevents.EventTypeBoundaryActivated {
		return statuscode.Errorf(statuscode.InvalidArgument, "unsupported boundary runtime event %q", event.EventType)
	}
	var activated boundaryevents.BoundaryActivated
	if err := json.Unmarshal([]byte(event.Data), &activated); err != nil {
		return statuscode.Errorf(statuscode.Internal, "decode %s: %v", event.EventType, err)
	}
	boundary, err := boundarycatalog.GetBoundaryQueryHandler(
		ctx,
		boundarycatalog.GetBoundaryQuery{Name: activated.Boundary},
		h.adminBoundary,
		h.retriever,
	)
	if err != nil {
		if statuscode.CodeOf(err) == statuscode.NotFound {
			// An activation with no current BoundaryCreated definition is an
			// orphan from a removed definition path. Ignoring it is fail-closed
			// and avoids an impossible retry loop.
			return nil
		}
		return err
	}
	if boundary.DefinitionPosition != nil && !event.Position.After(*boundary.DefinitionPosition) {
		// This activation predates the current BoundaryCreated definition. It
		// belongs to a removed pre-catalog definition path and must not install
		// or expose the replacement boundary.
		return nil
	}
	if boundary.Status != boundarymodel.StatusActive {
		return statuscode.Errorf(statuscode.FailedPrecondition, "boundary %q is not active", boundary.Name)
	}
	definition := boundarymodel.Definition{
		Name:        boundary.Name,
		Description: boundary.Description,
		Placement:   boundary.Placement,
	}
	if err := h.install(ctx, definition); err != nil {
		return err
	}
	return h.activate(ctx, boundary.Name)
}
