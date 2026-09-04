package foundationdb

import (
	"context"
	"testing"

	adminevents "github.com/OrisunLabs/Orisun/admin/events"
	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	"github.com/OrisunLabs/Orisun/internal/eventstoreadapter"
	"github.com/OrisunLabs/Orisun/orisun"
)

func TestEmbeddedFoundationDBBoundaryCommandsEmitEvents(t *testing.T) {
	for _, test := range []struct {
		name                 string
		existedBeforeCatalog bool
	}{
		{name: "new storage"},
		{name: "existing storage", existedBeforeCatalog: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			saver := &foundationDBBoundarySaver{}
			store := &Store{
				adminBoundary:  "orisun_admin",
				boundaryEvents: eventstoreadapter.New(saver, foundationDBBoundaryRetriever{}, nil),
			}
			boundary, err := store.CreateBoundary(t.Context(), boundarymodel.Definition{
				Name:                 "sales",
				Placement:            boundarymodel.Placement{Backend: "foundationdb", Namespace: "orisun"},
				ExistedBeforeCatalog: test.existedBeforeCatalog,
			})
			if err != nil {
				t.Fatalf("command error = %v", err)
			}
			if len(saver.events) != 1 || saver.events[0].EventType != adminevents.EventTypeBoundaryCreated {
				t.Fatalf("events = %#v", saver.events)
			}
			if boundary.Status != boundarymodel.StatusProvisioning ||
				boundary.ExistedBeforeCatalog != test.existedBeforeCatalog {
				t.Fatalf("boundary = %#v", boundary)
			}
		})
	}
}

type foundationDBBoundarySaver struct {
	events orisun.PreparedEventBatch
}

func (s *foundationDBBoundarySaver) SavePrepared(
	_ context.Context,
	events orisun.PreparedEventBatch,
	_ string,
	_ []orisun.ConsistencyCheck,
) (string, int64, error) {
	s.events = append(orisun.PreparedEventBatch(nil), events...)
	return "1", 2, nil
}

type foundationDBBoundaryRetriever struct{}

func (foundationDBBoundaryRetriever) GetBatch(context.Context, *orisun.GetEventsRequest) (orisun.ReadEventBatch, error) {
	return nil, nil
}

func (foundationDBBoundaryRetriever) GetLatestByCriteria(_ context.Context, query orisun.LatestByCriteriaQuery) (orisun.LatestByCriteriaBatch, error) {
	return orisun.LatestByCriteriaBatch{
		Matches:               make([]orisun.LatestCriterionMatch, len(query.Criteria)),
		ContextCommitPosition: -1, ContextPreparePosition: -1,
	}, nil
}
