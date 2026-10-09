package create_boundary

import (
	"context"
	"testing"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/goccy/go-json"
)

func TestEnsureBootstrapBoundaryCreatesMissingDefinition(t *testing.T) {
	retriever := &bootstrapRetriever{}
	saver := &bootstrapSaver{retriever: retriever}
	created, err := EnsureBootstrapBoundary(
		t.Context(),
		boundarymodel.Definition{
			Name:      "orisun_admin",
			Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "admin"},
		},
		"orisun_admin",
		saver,
		retriever,
	)
	if err != nil {
		t.Fatalf("EnsureBootstrapBoundary() error = %v", err)
	}
	if !created || len(saver.events) != 1 {
		t.Fatalf("created = %v, events = %#v", created, saver.events)
	}
}

func TestEnsureBootstrapBoundaryAcceptsMatchingDefinition(t *testing.T) {
	retriever := &bootstrapRetriever{existing: map[string]boundarymodel.Definition{
		"orisun_admin": {
			Name:      "orisun_admin",
			Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "admin"},
		},
	}}
	created, err := EnsureBootstrapBoundary(
		t.Context(),
		retriever.existing["orisun_admin"],
		"orisun_admin",
		&bootstrapSaver{retriever: retriever},
		retriever,
	)
	if err != nil {
		t.Fatalf("EnsureBootstrapBoundary() error = %v", err)
	}
	if created {
		t.Fatal("EnsureBootstrapBoundary() created existing definition")
	}
}

func TestEnsureBootstrapBoundaryRejectsPlacementConflict(t *testing.T) {
	retriever := &bootstrapRetriever{existing: map[string]boundarymodel.Definition{
		"orisun_admin": {
			Name:      "orisun_admin",
			Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "catalog_admin"},
		},
	}}
	_, err := EnsureBootstrapBoundary(
		t.Context(),
		boundarymodel.Definition{
			Name:      "orisun_admin",
			Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "configured_admin"},
		},
		"orisun_admin",
		&bootstrapSaver{retriever: retriever},
		retriever,
	)
	if err == nil {
		t.Fatal("EnsureBootstrapBoundary() error = nil")
	}
}

type bootstrapRetriever struct {
	existing map[string]boundarymodel.Definition
}

func (r *bootstrapRetriever) LatestByCriteria(_ context.Context, query coreeventstore.LatestByCriteriaRequest) (coreeventstore.LatestByCriteriaResult, error) {
	matches := make([]coreeventstore.LatestCriterionMatch, len(query.Criteria))
	name := query.Criteria[0].Tags[0].Value
	if definition, ok := r.existing[name]; ok {
		data, err := json.Marshal(boundaryevents.BoundaryCreated{
			Boundary:    definition.Name,
			Description: definition.Description,
			Placement:   definition.Placement,
		})
		if err != nil {
			return coreeventstore.LatestByCriteriaResult{}, err
		}
		matches[0] = coreeventstore.LatestCriterionMatch{
			Found: true,
			Event: coreeventstore.ReadEvent{EventType: boundaryevents.EventTypeBoundaryCreated, Data: string(data)},
		}
	}
	return coreeventstore.LatestByCriteriaResult{
		Matches:         matches,
		ContextPosition: coreeventstore.NotExistsPosition(),
	}, nil
}

type bootstrapSaver struct {
	retriever *bootstrapRetriever
	events    []coreeventstore.EventToAppend
}

func (s *bootstrapSaver) Append(_ context.Context, request coreeventstore.AppendRequest) (coreeventstore.AppendResult, error) {
	s.events = append(s.events, request.Events...)
	if s.retriever != nil {
		if s.retriever.existing == nil {
			s.retriever.existing = make(map[string]boundarymodel.Definition)
		}
		var data boundaryevents.BoundaryCreated
		if err := json.Unmarshal([]byte(request.Events[0].Data), &data); err == nil {
			s.retriever.existing[data.Boundary] = boundarymodel.Definition{
				Name:        data.Boundary,
				Description: data.Description,
				Placement:   data.Placement,
			}
		}
	}
	return coreeventstore.AppendResult{Position: coreeventstore.Position{
		CommitPosition:  1,
		PreparePosition: int64(len(s.events)),
	}}, nil
}
