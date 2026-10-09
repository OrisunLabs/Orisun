package boundary_catalog

import (
	"context"
	"strings"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
)

const catalogReadBatchSize uint32 = 1_000

type EventsRetriever interface {
	Read(ctx context.Context, request coreeventstore.ReadRequest) (coreeventstore.ReadEventBatch, error)
}

type ListBoundariesQuery struct{}

type GetBoundaryQuery struct {
	Name string
}

func ListBoundariesQueryHandler(
	ctx context.Context,
	_ ListBoundariesQuery,
	adminBoundary string,
	retriever EventsRetriever,
) ([]boundarymodel.Boundary, error) {
	catalog, err := replayCatalog(ctx, adminBoundary, "", retriever)
	if err != nil {
		return nil, err
	}
	return catalog.List(), nil
}

func GetBoundaryQueryHandler(
	ctx context.Context,
	query GetBoundaryQuery,
	adminBoundary string,
	retriever EventsRetriever,
) (boundarymodel.Boundary, error) {
	name := strings.TrimSpace(query.Name)
	if err := boundarymodel.ValidateName(name); err != nil {
		return boundarymodel.Boundary{}, statuscode.Errorf(statuscode.InvalidArgument, "invalid boundary %q: %v", name, err)
	}
	catalog, err := replayCatalog(ctx, adminBoundary, name, retriever)
	if err != nil {
		return boundarymodel.Boundary{}, err
	}
	boundary, found := catalog.Get(name)
	if !found {
		return boundarymodel.Boundary{}, statuscode.Errorf(statuscode.NotFound, "boundary %q was not found", name)
	}
	return boundary, nil
}

func replayCatalog(ctx context.Context, adminBoundary, name string, retriever EventsRetriever) (*Catalog, error) {
	if adminBoundary == "" || retriever == nil {
		return nil, statuscode.New(statuscode.Internal, "boundary catalog query handler is not configured")
	}
	catalog := NewCatalog()
	cursor := coreeventstore.NotExistsPosition()
	for {
		batch, err := retriever.Read(ctx, coreeventstore.ReadRequest{
			Boundary:     adminBoundary,
			Direction:    coreeventstore.DirectionAscending,
			Count:        catalogReadBatchSize,
			FromPosition: &cursor,
			Query:        lifecycleQuery(name),
		})
		if err != nil {
			return nil, err
		}
		advanced := false
		for _, event := range batch {
			if event.Position == cursor {
				continue
			}
			applied, applyErr := catalog.Apply(event)
			if applyErr != nil {
				return nil, statuscode.Errorf(statuscode.Internal, "rebuild boundary catalog at %d/%d: %v", event.Position.CommitPosition, event.Position.PreparePosition, applyErr)
			}
			if !applied {
				return nil, statuscode.Errorf(statuscode.Internal, "boundary catalog query returned unrelated event %q", event.EventType)
			}
			cursor = event.Position
			advanced = true
		}
		if len(batch) < int(catalogReadBatchSize) || !advanced {
			return catalog, nil
		}
	}
}

func lifecycleQuery(name string) coreeventstore.Query {
	eventTypes := []string{
		boundaryevents.EventTypeBoundaryCreated,
		boundaryevents.EventTypeBoundaryActivated,
		boundaryevents.EventTypeBoundaryFailed,
	}
	query := coreeventstore.Query{Criteria: make([]coreeventstore.Criterion, len(eventTypes))}
	for i, eventType := range eventTypes {
		tags := []coreeventstore.Tag{{Key: "__eventType", Value: eventType}}
		if name != "" {
			tags = append(tags, coreeventstore.Tag{Key: "boundary", Value: name})
		}
		query.Criteria[i] = coreeventstore.Criterion{Tags: tags}
	}
	return query
}
