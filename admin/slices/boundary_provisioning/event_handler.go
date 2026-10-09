package boundary_provisioning

import (
	"context"
	"time"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
)

type BoundaryProvisioningEventHandler struct {
	adminBoundary string
	appender      EventAppender
	retriever     LatestByCriteriaRetriever
	provision     ProvisionBoundary
	ensureGlobal  EnsureGlobalBoundary
}

// EnsureGlobalBoundary reconciles shared ephemeral resources, such as the
// boundary's JetStream stream, without repeating durable backend provisioning.
type EnsureGlobalBoundary func(ctx context.Context, boundary string) error

func NewBoundaryProvisioningEventHandler(
	adminBoundary string,
	appender EventAppender,
	retriever LatestByCriteriaRetriever,
	provision ProvisionBoundary,
	ensureGlobal EnsureGlobalBoundary,
) *BoundaryProvisioningEventHandler {
	return &BoundaryProvisioningEventHandler{
		adminBoundary: adminBoundary,
		appender:      appender,
		retriever:     retriever,
		provision:     provision,
		ensureGlobal:  ensureGlobal,
	}
}

func (h *BoundaryProvisioningEventHandler) Handle(ctx context.Context, event coreeventstore.ReadEvent) error {
	if h == nil || h.provision == nil || h.ensureGlobal == nil {
		return statuscode.New(statuscode.Internal, "boundary provisioning event handler is not configured")
	}
	definition, err := definitionFromEvent(event)
	if err != nil {
		return err
	}
	_, err = ProvisionBoundaryCommandHandler(ctx, ProvisionBoundaryCommand{
		Definition: definition,
		Metadata: map[string]any{
			"source":     "event_handler",
			"handler":    "boundary_provisioning",
			"capturedAt": time.Now().UTC().Format(time.RFC3339Nano),
			"reactedTo": map[string]any{
				"eventId":   event.EventID,
				"eventType": event.EventType,
				"position": map[string]int64{
					"commit":  event.Position.CommitPosition,
					"prepare": event.Position.PreparePosition,
				},
			},
		},
	}, h.adminBoundary, h.appender, h.retriever, func(provisionCtx context.Context, definition boundarymodel.Definition) error {
		if provisionErr := h.provision(provisionCtx, definition); provisionErr != nil {
			return provisionErr
		}
		return h.ensureGlobal(provisionCtx, definition.Name)
	})
	if statuscode.CodeOf(err) == statuscode.AlreadyExists {
		// Controller failover must not repeat physical DDL for ACTIVE
		// definitions, but it must restore shared ephemeral resources such as a
		// lost in-memory JetStream stream.
		return h.ensureGlobal(ctx, definition.Name)
	}
	return err
}

func definitionFromEvent(event coreeventstore.ReadEvent) (boundarymodel.Definition, error) {
	if event.EventType != boundaryevents.EventTypeBoundaryCreated {
		return boundarymodel.Definition{}, statuscode.Errorf(statuscode.InvalidArgument, "unsupported boundary provisioning event %q", event.EventType)
	}
	var data boundaryevents.BoundaryCreated
	if err := json.Unmarshal([]byte(event.Data), &data); err != nil {
		return boundarymodel.Definition{}, statuscode.Errorf(statuscode.Internal, "decode %s: %v", event.EventType, err)
	}
	return boundarymodel.Definition{
		Name:        data.Boundary,
		Description: data.Description,
		Placement:   data.Placement,
	}, nil
}
