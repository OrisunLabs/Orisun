package create_boundary

import (
	"context"
	"testing"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestCreateBoundaryCommandHandlerEmitsBoundaryCreatedWithCCCContext(t *testing.T) {
	retriever := &fakeRetriever{batch: emptyBatch()}
	saver := &fakeSaver{position: coreeventstore.Position{CommitPosition: 17, PreparePosition: 9}}
	result, err := CreateBoundaryCommandHandler(context.Background(), CreateBoundaryCommand{
		Name:        " orders ",
		Description: " Orders context ",
		Placement:   boundarymodel.Placement{Backend: " postgres ", Namespace: " sales "},
		Metadata:    CommandMetadata{"actor": "admin-1"},
	}, "orisun_admin", saver, retriever)
	require.NoError(t, err)
	require.Equal(t, "orders", result.Boundary.Name)
	require.Equal(t, &coreeventstore.Position{CommitPosition: 17, PreparePosition: 9}, result.Boundary.DefinitionPosition)
	require.Equal(t, &coreeventstore.Position{CommitPosition: -1, PreparePosition: -1}, saver.expected)
	require.Equal(t, subsetQuery(definitionCriteria("orders")), saver.query)
	require.Len(t, saver.events, 1)
	require.Equal(t, boundaryevents.EventTypeBoundaryCreated, saver.events[0].EventType)

	var data map[string]any
	require.NoError(t, json.Unmarshal([]byte(saver.events[0].Data), &data))
	require.Equal(t, "orders", data["boundary"])
	require.NotContains(t, data, "__eventType")
	require.NotContains(t, data, "eventType")
	var metadata map[string]any
	require.NoError(t, json.Unmarshal([]byte(saver.events[0].Metadata), &metadata))
	require.Equal(t, "admin-1", metadata["actor"])
	require.NotEmpty(t, metadata["query"])
}

func TestCreateBoundaryCommandHandlerRejectsExistingBoundaryWithoutEvent(t *testing.T) {
	existing := readEvent(t, boundaryevents.EventTypeBoundaryCreated, boundaryevents.BoundaryCreated{
		Boundary:  "orders",
		Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "public"},
	}, 4, 5)
	batch := emptyBatch()
	batch.Matches[0] = coreeventstore.LatestCriterionMatch{Event: existing, Found: true}
	batch.ContextPosition = coreeventstore.Position{CommitPosition: 4, PreparePosition: 5}
	saver := &fakeSaver{}

	_, err := CreateBoundaryCommandHandler(context.Background(), CreateBoundaryCommand{
		Name:      "orders",
		Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "public"},
	}, "orisun_admin", saver, &fakeRetriever{batch: batch})
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
	require.Empty(t, saver.events)
}

type fakeSaver struct {
	events   []coreeventstore.EventToAppend
	expected *coreeventstore.Position
	query    coreeventstore.Query
	position coreeventstore.Position
}

func (s *fakeSaver) Append(_ context.Context, request coreeventstore.AppendRequest) (coreeventstore.AppendResult, error) {
	s.events, s.expected, s.query = request.Events, &request.Consistency[0].Position, request.Consistency[0].Query
	return coreeventstore.AppendResult{Position: s.position}, nil
}

type fakeRetriever struct {
	batch coreeventstore.LatestByCriteriaResult
	err   error
}

func (r *fakeRetriever) LatestByCriteria(context.Context, coreeventstore.LatestByCriteriaRequest) (coreeventstore.LatestByCriteriaResult, error) {
	return r.batch, r.err
}

func emptyBatch() coreeventstore.LatestByCriteriaResult {
	return coreeventstore.LatestByCriteriaResult{
		Matches:         make([]coreeventstore.LatestCriterionMatch, 1),
		ContextPosition: coreeventstore.NotExistsPosition(),
	}
}

func readEvent(t *testing.T, eventType string, data any, commit, prepare int64) coreeventstore.ReadEvent {
	t.Helper()
	raw, err := json.Marshal(data)
	require.NoError(t, err)
	return coreeventstore.ReadEvent{
		EventType: eventType,
		Data:      string(raw),
		Position:  coreeventstore.Position{CommitPosition: commit, PreparePosition: prepare},
	}
}
