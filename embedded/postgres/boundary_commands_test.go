package postgres

import (
	"context"
	"testing"

	adminevents "github.com/OrisunLabs/Orisun/admin/events"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	"github.com/OrisunLabs/Orisun/internal/eventstoreadapter"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
)

func TestEmbeddedBoundaryCommandsEmitEvents(t *testing.T) {
	for _, test := range []struct {
		name                 string
		existedBeforeCatalog bool
	}{
		{name: "new storage"},
		{name: "existing storage", existedBeforeCatalog: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			saver := &embeddedBoundarySaver{}
			store := &Store{
				adminBoundary: "orisun_admin",
				boundaryEvents: eventstoreadapter.New(
					saver,
					embeddedBoundaryRetriever{},
					nil,
				),
			}
			boundary, err := store.CreateBoundary(t.Context(), boundarymodel.Definition{
				Name:                 "sales",
				Description:          "Sales domain",
				Placement:            boundarymodel.Placement{Backend: "postgres", Namespace: "tenant_data"},
				ExistedBeforeCatalog: test.existedBeforeCatalog,
			})
			if err != nil {
				t.Fatalf("command error = %v", err)
			}
			if len(saver.events) != 1 || saver.events[0].EventType != adminevents.EventTypeBoundaryCreated {
				t.Fatalf("saved events = %#v, want one %s", saver.events, adminevents.EventTypeBoundaryCreated)
			}
			if boundary.Name != "sales" || boundary.Status != boundarymodel.StatusProvisioning ||
				boundary.ExistedBeforeCatalog != test.existedBeforeCatalog {
				t.Fatalf("boundary = %#v", boundary)
			}
		})
	}
}

func TestEmbeddedBoundaryCatalogQueries(t *testing.T) {
	created, err := json.Marshal(adminevents.BoundaryCreated{
		Boundary:  "sales",
		Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "tenant_data"},
	})
	if err != nil {
		t.Fatal(err)
	}
	activated, err := json.Marshal(adminevents.BoundaryActivated{Boundary: "sales"})
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{
		adminBoundary: "orisun_admin",
		boundaryEvents: eventstoreadapter.New(nil, embeddedCatalogRetriever{events: orisun.ReadEventBatch{
			{EventType: adminevents.EventTypeBoundaryCreated, Data: string(created), CommitPosition: 1, PreparePosition: 1},
			{EventType: adminevents.EventTypeBoundaryActivated, Data: string(activated), CommitPosition: 2, PreparePosition: 2},
		}}, nil),
	}

	boundaries, err := store.ListBoundaries(t.Context())
	if err != nil || len(boundaries) != 1 || boundaries[0].Status != boundarymodel.StatusActive {
		t.Fatalf("ListBoundaries() = %#v, %v", boundaries, err)
	}
	boundary, err := store.GetBoundary(t.Context(), "sales")
	if err != nil || boundary.Name != "sales" || boundary.Status != boundarymodel.StatusActive {
		t.Fatalf("GetBoundary() = %#v, %v", boundary, err)
	}
}

type embeddedBoundarySaver struct {
	events orisun.PreparedEventBatch
}

func (s *embeddedBoundarySaver) SavePrepared(
	_ context.Context,
	events orisun.PreparedEventBatch,
	_ string,
	_ []orisun.ConsistencyCheck,
) (string, int64, error) {
	s.events = append(orisun.PreparedEventBatch(nil), events...)
	return "1", 2, nil
}

type embeddedBoundaryRetriever struct{}

func (embeddedBoundaryRetriever) GetBatch(context.Context, *orisun.GetEventsRequest) (orisun.ReadEventBatch, error) {
	return nil, nil
}

type embeddedCatalogRetriever struct {
	events orisun.ReadEventBatch
}

func (r embeddedCatalogRetriever) GetBatch(context.Context, *orisun.GetEventsRequest) (orisun.ReadEventBatch, error) {
	return r.events, nil
}

func (r embeddedCatalogRetriever) GetLatestByCriteria(context.Context, orisun.LatestByCriteriaQuery) (orisun.LatestByCriteriaBatch, error) {
	return orisun.LatestByCriteriaBatch{}, nil
}

func (embeddedBoundaryRetriever) GetLatestByCriteria(_ context.Context, query orisun.LatestByCriteriaQuery) (orisun.LatestByCriteriaBatch, error) {
	return orisun.LatestByCriteriaBatch{
		Matches:                make([]orisun.LatestCriterionMatch, len(query.Criteria)),
		ContextCommitPosition:  -1,
		ContextPreparePosition: -1,
	}, nil
}
