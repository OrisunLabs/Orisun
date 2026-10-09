package boundary_provisioning

import (
	"context"
	"fmt"
	"sort"

	boundarycatalog "github.com/OrisunLabs/Orisun/admin/slices/boundary_catalog"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
)

type ProvisionBoundaryCommand struct {
	Definition boundarymodel.Definition
	Metadata   map[string]any
}

type ProvisionBoundaryResult struct {
	Boundary boundarymodel.Boundary
}

// ProvisionBoundary is the single side-effecting port required by this slice.
// A backend provisioner method can be passed directly as this function value.
type ProvisionBoundary func(ctx context.Context, definition boundarymodel.Definition) error

type LatestByCriteriaRetriever interface {
	LatestByCriteria(ctx context.Context, request coreeventstore.LatestByCriteriaRequest) (coreeventstore.LatestByCriteriaResult, error)
}

type EventAppender interface {
	Append(ctx context.Context, request coreeventstore.AppendRequest) (coreeventstore.AppendResult, error)
}

func ProvisionBoundaryCommandHandler(
	ctx context.Context,
	command ProvisionBoundaryCommand,
	adminBoundary string,
	appender EventAppender,
	retriever LatestByCriteriaRetriever,
	provision ProvisionBoundary,
) (ProvisionBoundaryResult, error) {
	if err := validateDefinition(command.Definition); err != nil {
		return ProvisionBoundaryResult{}, err
	}
	if adminBoundary == "" || appender == nil || retriever == nil || provision == nil {
		return ProvisionBoundaryResult{}, statuscode.New(statuscode.Internal, "provision boundary handler is not configured")
	}

	model, err := loadContext(ctx, adminBoundary, command.Definition.Name, retriever)
	if err != nil {
		return ProvisionBoundaryResult{}, err
	}
	if model.boundary.Status == boundarymodel.StatusActive {
		return ProvisionBoundaryResult{}, statuscode.Errorf(statuscode.AlreadyExists, "boundary %q is already active", command.Definition.Name)
	}
	provisionErr := provision(ctx, command.Definition)
	if provisionErr == nil {
		for {
			boundary, appendErr := appendOutcome(
				ctx,
				adminBoundary,
				appender,
				command.Metadata,
				model,
				boundaryevents.EventTypeBoundaryActivated,
				boundaryevents.BoundaryActivated{Boundary: command.Definition.Name},
			)
			if statuscode.CodeOf(appendErr) != statuscode.AlreadyExists {
				if appendErr != nil {
					return ProvisionBoundaryResult{}, appendErr
				}
				return ProvisionBoundaryResult{Boundary: boundary}, nil
			}

			// A concurrent outcome changed the context. Activation remains
			// monotonic: retry from FAILED, or report the already-active fact.
			model, err = loadContext(ctx, adminBoundary, command.Definition.Name, retriever)
			if err != nil {
				return ProvisionBoundaryResult{}, err
			}
			if model.boundary.Status == boundarymodel.StatusActive {
				return ProvisionBoundaryResult{}, statuscode.Errorf(statuscode.AlreadyExists, "boundary %q is already active", command.Definition.Name)
			}
		}
	}

	// Avoid an unbounded run of identical failure events while an event handler
	// retries the same definition. A later successful attempt can still append
	// BoundaryActivated from the FAILED context.
	if model.boundary.Status != boundarymodel.StatusFailed {
		if _, appendErr := appendOutcome(
			ctx,
			adminBoundary,
			appender,
			command.Metadata,
			model,
			boundaryevents.EventTypeBoundaryFailed,
			boundaryevents.BoundaryProvisioningFailed{Boundary: command.Definition.Name, Error: provisionErr.Error()},
		); appendErr != nil && statuscode.CodeOf(appendErr) != statuscode.AlreadyExists {
			return ProvisionBoundaryResult{}, fmt.Errorf("provision boundary %q: %v; record failure: %w", command.Definition.Name, provisionErr, appendErr)
		}
	}
	return ProvisionBoundaryResult{}, provisionErr
}

type commandContext struct {
	boundary *boundarymodel.Boundary
	position *coreeventstore.Position
	query    coreeventstore.Query
}

func loadContext(ctx context.Context, adminBoundary, name string, retriever LatestByCriteriaRetriever) (*commandContext, error) {
	criteria := lifecycleCriteria(name)
	latest, err := retriever.LatestByCriteria(ctx, coreeventstore.LatestByCriteriaRequest{Boundary: adminBoundary, Criteria: criteria})
	if err != nil {
		return nil, err
	}
	if len(latest.Matches) != len(criteria) {
		return nil, statuscode.Errorf(statuscode.Internal, "boundary lifecycle query returned %d matches, expected %d", len(latest.Matches), len(criteria))
	}

	var definition *coreeventstore.ReadEvent
	for _, match := range latest.Matches {
		if match.Found && match.Event.EventType == boundaryevents.EventTypeBoundaryCreated {
			event := match.Event
			definition = &event
			break
		}
	}
	if definition == nil {
		return nil, statuscode.Errorf(statuscode.FailedPrecondition, "boundary %q has no definition event", name)
	}

	events := []coreeventstore.ReadEvent{*definition}
	for _, match := range latest.Matches {
		if !match.Found || match.Event.EventType == boundaryevents.EventTypeBoundaryCreated {
			continue
		}
		// Outcomes written before the current definition belong to a removed
		// pre-catalog definition path and must not suppress provisioning of the
		// replacement BoundaryCreated event.
		if match.Event.Position.After(definition.Position) {
			events = append(events, match.Event)
		}
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Position.CommitPosition != events[j].Position.CommitPosition {
			return events[i].Position.CommitPosition < events[j].Position.CommitPosition
		}
		return events[i].Position.PreparePosition < events[j].Position.PreparePosition
	})

	catalog := boundarycatalog.NewCatalog()
	for _, event := range events {
		applied, applyErr := catalog.Apply(event)
		if applyErr != nil {
			return nil, statuscode.Errorf(statuscode.Internal, "rebuild boundary %q lifecycle: %v", name, applyErr)
		}
		if !applied {
			return nil, statuscode.Errorf(statuscode.Internal, "unexpected event %q in boundary lifecycle", event.EventType)
		}
	}
	boundary, ok := catalog.Get(name)
	if !ok {
		return nil, statuscode.Errorf(statuscode.Internal, "boundary definition event does not describe %q", name)
	}
	return &commandContext{
		boundary: &boundary,
		position: &latest.ContextPosition,
		query:    subsetQuery(criteria),
	}, nil
}

func appendOutcome(
	ctx context.Context,
	adminBoundary string,
	appender EventAppender,
	metadata map[string]any,
	model *commandContext,
	eventType string,
	data any,
) (boundarymodel.Boundary, error) {
	eventID, err := uuid.NewV7()
	if err != nil {
		return boundarymodel.Boundary{}, statuscode.Errorf(statuscode.Internal, "create %s event id: %v", eventType, err)
	}
	dataJSON, err := eventData(data)
	if err != nil {
		return boundarymodel.Boundary{}, statuscode.Errorf(statuscode.Internal, "prepare %s data: %v", eventType, err)
	}
	metadataJSON, err := json.Marshal(metadataWithQuery(metadata, model.query))
	if err != nil {
		return boundarymodel.Boundary{}, statuscode.Errorf(statuscode.Internal, "prepare %s metadata: %v", eventType, err)
	}
	appendResult, err := appender.Append(ctx, coreeventstore.AppendRequest{
		Boundary: adminBoundary,
		Events: []coreeventstore.EventToAppend{{
			EventID:   eventID.String(),
			EventType: eventType,
			Data:      dataJSON,
			Metadata:  string(metadataJSON),
		}},
		Consistency: []coreeventstore.ConsistencyObservation{{Query: model.query, Position: *model.position}},
	})
	if err != nil {
		return boundarymodel.Boundary{}, err
	}
	position := appendResult.Position
	boundary := *model.boundary
	boundary.StatusPosition = &position
	if eventType == boundaryevents.EventTypeBoundaryActivated {
		boundary.Status = boundarymodel.StatusActive
		boundary.LastError = ""
	} else {
		boundary.Status = boundarymodel.StatusFailed
		if failed, ok := data.(boundaryevents.BoundaryProvisioningFailed); ok {
			boundary.LastError = failed.Error
		}
	}
	return boundary, nil
}

func validateDefinition(definition boundarymodel.Definition) error {
	if err := boundarymodel.ValidateName(definition.Name); err != nil {
		return statuscode.Errorf(statuscode.InvalidArgument, "invalid boundary %q: %v", definition.Name, err)
	}
	if definition.Placement.Backend == "" || definition.Placement.Namespace == "" {
		return statuscode.New(statuscode.InvalidArgument, "boundary placement backend and namespace are required")
	}
	return nil
}

func lifecycleCriteria(name string) []coreeventstore.Criterion {
	eventTypes := []string{
		boundaryevents.EventTypeBoundaryCreated,
		boundaryevents.EventTypeBoundaryActivated,
		boundaryevents.EventTypeBoundaryFailed,
	}
	criteria := make([]coreeventstore.Criterion, len(eventTypes))
	for i, eventType := range eventTypes {
		criteria[i] = coreeventstore.Criterion{Tags: []coreeventstore.Tag{{Key: "boundary", Value: name}, {Key: "__eventType", Value: eventType}}}
	}
	return criteria
}

func subsetQuery(criteria []coreeventstore.Criterion) coreeventstore.Query {
	return coreeventstore.Query{Criteria: criteria}
}

func metadataWithQuery(metadata map[string]any, query coreeventstore.Query) map[string]any {
	merged := make(map[string]any, len(metadata)+1)
	for key, value := range metadata {
		merged[key] = value
	}
	if encoded, err := json.Marshal(query); err == nil {
		merged["query"] = string(encoded)
	}
	return merged
}

func eventData(value any) (string, error) {
	encoded, err := json.Marshal(value)
	return string(encoded), err
}
