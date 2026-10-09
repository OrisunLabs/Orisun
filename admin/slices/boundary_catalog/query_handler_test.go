package boundary_catalog

import (
	"context"
	"fmt"
	"sort"
	"testing"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
)

func TestListBoundariesQueryHandlerReplaysMultiplePages(t *testing.T) {
	events := make(coreeventstore.ReadEventBatch, 0, catalogReadBatchSize+2)
	for i := 0; i < int(catalogReadBatchSize/2)+1; i++ {
		name := boundaryName(i)
		events = append(events,
			catalogReadEvent(t, boundaryevents.EventTypeBoundaryCreated, boundaryevents.BoundaryCreated{
				Boundary:  name,
				Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "public"},
			}, int64(len(events)+1)),
			catalogReadEvent(t, boundaryevents.EventTypeBoundaryActivated, boundaryevents.BoundaryActivated{Boundary: name}, int64(len(events)+2)),
		)
	}
	retriever := &catalogQueryRetriever{events: events}

	boundaries, err := ListBoundariesQueryHandler(t.Context(), ListBoundariesQuery{}, "orisun_admin", retriever)
	if err != nil {
		t.Fatalf("ListBoundariesQueryHandler() error = %v", err)
	}
	if len(boundaries) != int(catalogReadBatchSize/2)+1 {
		t.Fatalf("boundaries = %d", len(boundaries))
	}
	if retriever.calls < 2 {
		t.Fatalf("Read() calls = %d, want multiple pages", retriever.calls)
	}
	for _, boundary := range boundaries {
		if boundary.Status != boundarymodel.StatusActive {
			t.Fatalf("boundary %s status = %s", boundary.Name, boundary.Status)
		}
	}
}

func TestGetBoundaryQueryHandlerFiltersAndReturnsBoundary(t *testing.T) {
	retriever := &catalogQueryRetriever{events: coreeventstore.ReadEventBatch{
		catalogReadEvent(t, boundaryevents.EventTypeBoundaryCreated, boundaryevents.BoundaryCreated{
			Boundary:  "sales",
			Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "legacy"},
		}, 1),
		catalogReadEvent(t, boundaryevents.EventTypeBoundaryFailed, boundaryevents.BoundaryProvisioningFailed{
			Boundary: "sales",
			Error:    "unavailable",
		}, 2),
	}}

	boundary, err := GetBoundaryQueryHandler(t.Context(), GetBoundaryQuery{Name: "sales"}, "orisun_admin", retriever)
	if err != nil {
		t.Fatalf("GetBoundaryQueryHandler() error = %v", err)
	}
	if boundary.Status != boundarymodel.StatusFailed || boundary.LastError != "unavailable" {
		t.Fatalf("boundary = %#v", boundary)
	}
	if got := retriever.queries[0].Criteria[0].Tags; len(got) != 2 || got[1].Value != "sales" {
		t.Fatalf("get query tags = %#v", got)
	}
}

func TestGetBoundaryQueryHandlerReturnsNotFound(t *testing.T) {
	_, err := GetBoundaryQueryHandler(t.Context(), GetBoundaryQuery{Name: "missing"}, "orisun_admin", &catalogQueryRetriever{})
	if statuscode.CodeOf(err) != statuscode.NotFound {
		t.Fatalf("error = %v, code = %v", err, statuscode.CodeOf(err))
	}
}

type catalogQueryRetriever struct {
	events  coreeventstore.ReadEventBatch
	calls   int
	queries []coreeventstore.Query
}

func (r *catalogQueryRetriever) Read(_ context.Context, req coreeventstore.ReadRequest) (coreeventstore.ReadEventBatch, error) {
	r.calls++
	r.queries = append(r.queries, req.Query)
	events := append(coreeventstore.ReadEventBatch(nil), r.events...)
	sort.Slice(events, func(i, j int) bool {
		return events[i].Position.Before(events[j].Position)
	})
	result := make(coreeventstore.ReadEventBatch, 0, req.Count)
	for _, event := range events {
		if req.FromPosition != nil && event.Position.Before(*req.FromPosition) {
			continue
		}
		result = append(result, event)
		if len(result) == int(req.Count) {
			break
		}
	}
	return result, nil
}

func boundaryName(index int) string {
	return fmt.Sprintf("boundary_%04d", index)
}

func catalogReadEvent(t *testing.T, eventType string, data any, position int64) coreeventstore.ReadEvent {
	t.Helper()
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return coreeventstore.ReadEvent{
		EventID:   fmt.Sprintf("event-%d", position),
		EventType: eventType,
		Data:      string(encoded),
		Metadata:  `{}`,
		Position: coreeventstore.Position{
			CommitPosition:  position,
			PreparePosition: position,
		},
	}
}
