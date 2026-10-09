package boundary_provisioning

import (
	"context"
	"errors"
	"testing"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestEventHandlerProvisionsAndRecordsActivation(t *testing.T) {
	definition := createdBoundaryEvent(t, "orders", 10, 2)
	retriever := &scriptedBoundaryRetriever{batches: []coreeventstore.LatestByCriteriaResult{
		lifecycleBatch(definition),
	}}
	saver := &scriptedOutcomeSaver{}
	provisioner := &captureProvisioner{}
	global := &captureActivator{}
	handler := NewBoundaryProvisioningEventHandler("orisun_admin", saver, retriever, provisioner.EnsureBoundary, global.ActivateBoundary)

	err := handler.Handle(context.Background(), definition)
	require.NoError(t, err)
	require.Equal(t, boundarymodel.Definition{
		Name:        "orders",
		Description: "Orders context",
		Placement:   boundarymodel.Placement{Backend: "postgres", Namespace: "sales"},
	}, provisioner.definitions[0])
	require.Len(t, saver.calls, 1)
	require.Equal(t, boundaryevents.EventTypeBoundaryActivated, saver.calls[0].events[0].EventType)
	require.Equal(t, &coreeventstore.Position{CommitPosition: 10, PreparePosition: 2}, saver.calls[0].expectedPosition)
	require.Equal(t, subsetQuery(lifecycleCriteria("orders")), saver.calls[0].query)
	require.Equal(t, []string{"orders"}, global.boundaries)
}

func TestEventHandlerRecordsProvisioningFailure(t *testing.T) {
	definition := createdBoundaryEvent(t, "orders", 10, 2)
	retriever := &scriptedBoundaryRetriever{batches: []coreeventstore.LatestByCriteriaResult{
		lifecycleBatch(definition),
	}}
	saver := &scriptedOutcomeSaver{}
	provisionErr := errors.New("create schema: permission denied")
	provisioner := &captureProvisioner{err: provisionErr}
	handler := NewBoundaryProvisioningEventHandler("orisun_admin", saver, retriever, provisioner.EnsureBoundary, (&captureActivator{}).ActivateBoundary)

	err := handler.Handle(context.Background(), definition)
	require.ErrorIs(t, err, provisionErr)
	require.Len(t, saver.calls, 1)
	require.Equal(t, boundaryevents.EventTypeBoundaryFailed, saver.calls[0].events[0].EventType)

	var data boundaryevents.BoundaryProvisioningFailed
	require.NoError(t, json.Unmarshal([]byte(saver.calls[0].events[0].Data), &data))
	require.Equal(t, "orders", data.Boundary)
	require.Equal(t, provisionErr.Error(), data.Error)
}

func TestEventHandlerReturnsActivationAppendFailure(t *testing.T) {
	definition := createdBoundaryEvent(t, "orders", 10, 2)
	retriever := &scriptedBoundaryRetriever{batches: []coreeventstore.LatestByCriteriaResult{
		lifecycleBatch(definition),
	}}
	appendErr := errors.New("activation append unavailable")
	saver := &scriptedOutcomeSaver{errors: []error{appendErr}}
	provisioner := &captureProvisioner{}
	handler := NewBoundaryProvisioningEventHandler(
		"orisun_admin",
		saver,
		retriever,
		provisioner.EnsureBoundary,
		(&captureActivator{}).ActivateBoundary,
	)

	err := handler.Handle(context.Background(), definition)
	require.ErrorIs(t, err, appendErr)
}

func TestEventHandlerDoesNotReprovisionAlreadyActiveBoundary(t *testing.T) {
	definition := createdBoundaryEvent(t, "orders", 10, 2)
	active := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryActivated, boundaryevents.BoundaryActivated{
		Boundary: "orders",
	}, 11, 3)
	retriever := &scriptedBoundaryRetriever{batches: []coreeventstore.LatestByCriteriaResult{
		lifecycleBatch(definition, active),
	}}
	saver := &scriptedOutcomeSaver{}
	provisioner := &captureProvisioner{}
	global := &captureActivator{}
	handler := NewBoundaryProvisioningEventHandler("orisun_admin", saver, retriever, provisioner.EnsureBoundary, global.ActivateBoundary)

	err := handler.Handle(context.Background(), definition)
	require.NoError(t, err)
	require.Empty(t, provisioner.definitions)
	require.Empty(t, saver.calls)
	require.Equal(t, []string{"orders"}, global.boundaries)
}

func TestEventHandlerIgnoresOutcomeBeforeDefinition(t *testing.T) {
	staleActive := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryActivated, boundaryevents.BoundaryActivated{
		Boundary: "orders",
	}, 9, 1)
	definition := createdBoundaryEvent(t, "orders", 10, 2)
	retriever := &scriptedBoundaryRetriever{batches: []coreeventstore.LatestByCriteriaResult{
		lifecycleBatch(definition, staleActive),
	}}
	saver := &scriptedOutcomeSaver{}
	provisioner := &captureProvisioner{}
	global := &captureActivator{}
	handler := NewBoundaryProvisioningEventHandler("orisun_admin", saver, retriever, provisioner.EnsureBoundary, global.ActivateBoundary)

	err := handler.Handle(t.Context(), definition)
	require.NoError(t, err)
	require.Len(t, provisioner.definitions, 1)
	require.Len(t, saver.calls, 1)
	require.Equal(t, boundaryevents.EventTypeBoundaryActivated, saver.calls[0].events[0].EventType)
	require.Equal(t, &coreeventstore.Position{CommitPosition: 10, PreparePosition: 2}, saver.calls[0].expectedPosition)
	require.Equal(t, []string{"orders"}, global.boundaries)
}

func TestEventHandlerPromotesFailedBoundaryAfterSuccessfulRetry(t *testing.T) {
	definition := createdBoundaryEvent(t, "orders", 10, 2)
	failed := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryFailed, boundaryevents.BoundaryProvisioningFailed{
		Boundary: "orders",
		Error:    "temporary failure",
	}, 12, 4)
	retriever := &scriptedBoundaryRetriever{batches: []coreeventstore.LatestByCriteriaResult{
		lifecycleBatch(definition, failed),
	}}
	saver := &scriptedOutcomeSaver{}
	provisioner := &captureProvisioner{}
	handler := NewBoundaryProvisioningEventHandler("orisun_admin", saver, retriever, provisioner.EnsureBoundary, (&captureActivator{}).ActivateBoundary)

	err := handler.Handle(context.Background(), definition)
	require.NoError(t, err)
	require.Len(t, saver.calls, 1)
	require.Equal(t, boundaryevents.EventTypeBoundaryActivated, saver.calls[0].events[0].EventType)
	require.Equal(t, &coreeventstore.Position{CommitPosition: 12, PreparePosition: 4}, saver.calls[0].expectedPosition)
}

func TestEventHandlerReevaluatesAfterConcurrentOutcome(t *testing.T) {
	definition := createdBoundaryEvent(t, "orders", 10, 2)
	failed := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryFailed, boundaryevents.BoundaryProvisioningFailed{
		Boundary: "orders",
		Error:    "other node failed",
	}, 11, 3)
	retriever := &scriptedBoundaryRetriever{batches: []coreeventstore.LatestByCriteriaResult{
		lifecycleBatch(definition),
		lifecycleBatch(definition, failed),
	}}
	saver := &scriptedOutcomeSaver{errors: []error{
		statuscode.New(statuscode.AlreadyExists, "lifecycle context changed"),
		nil,
	}}
	provisioner := &captureProvisioner{}
	handler := NewBoundaryProvisioningEventHandler("orisun_admin", saver, retriever, provisioner.EnsureBoundary, (&captureActivator{}).ActivateBoundary)

	err := handler.Handle(context.Background(), definition)
	require.NoError(t, err)
	require.Len(t, saver.calls, 2)
	require.Equal(t, &coreeventstore.Position{CommitPosition: 10, PreparePosition: 2}, saver.calls[0].expectedPosition)
	require.Equal(t, &coreeventstore.Position{CommitPosition: 11, PreparePosition: 3}, saver.calls[1].expectedPosition)
}

type captureProvisioner struct {
	definitions []boundarymodel.Definition
	err         error
}

func (p *captureProvisioner) EnsureBoundary(_ context.Context, definition boundarymodel.Definition) error {
	p.definitions = append(p.definitions, definition)
	return p.err
}

type captureActivator struct {
	boundaries []string
	err        error
}

func (a *captureActivator) ActivateBoundary(_ context.Context, boundary string) error {
	a.boundaries = append(a.boundaries, boundary)
	return a.err
}

type scriptedBoundaryRetriever struct {
	batches []coreeventstore.LatestByCriteriaResult
}

func (r *scriptedBoundaryRetriever) LatestByCriteria(context.Context, coreeventstore.LatestByCriteriaRequest) (coreeventstore.LatestByCriteriaResult, error) {
	if len(r.batches) == 0 {
		return coreeventstore.LatestByCriteriaResult{}, errors.New("no scripted lifecycle batch")
	}
	batch := r.batches[0]
	r.batches = r.batches[1:]
	return batch, nil
}

type outcomeSaveCall struct {
	events           []coreeventstore.EventToAppend
	boundary         string
	expectedPosition *coreeventstore.Position
	query            coreeventstore.Query
}

type scriptedOutcomeSaver struct {
	calls  []outcomeSaveCall
	errors []error
}

func (s *scriptedOutcomeSaver) Append(
	_ context.Context,
	request coreeventstore.AppendRequest,
) (coreeventstore.AppendResult, error) {
	s.calls = append(s.calls, outcomeSaveCall{
		events:   request.Events,
		boundary: request.Boundary,
		expectedPosition: &coreeventstore.Position{
			CommitPosition:  request.Consistency[0].Position.CommitPosition,
			PreparePosition: request.Consistency[0].Position.PreparePosition,
		},
		query: request.Consistency[0].Query,
	})
	if len(s.errors) == 0 {
		return coreeventstore.AppendResult{}, nil
	}
	err := s.errors[0]
	s.errors = s.errors[1:]
	return coreeventstore.AppendResult{}, err
}

func lifecycleBatch(events ...coreeventstore.ReadEvent) coreeventstore.LatestByCriteriaResult {
	batch := coreeventstore.LatestByCriteriaResult{
		Matches:         make([]coreeventstore.LatestCriterionMatch, 3),
		ContextPosition: coreeventstore.NotExistsPosition(),
	}
	for _, event := range events {
		index := lifecycleEventIndex(event.EventType)
		batch.Matches[index] = coreeventstore.LatestCriterionMatch{Event: event, Found: true}
		if event.Position.After(batch.ContextPosition) {
			batch.ContextPosition = event.Position
		}
	}
	return batch
}

func lifecycleEventIndex(eventType string) int {
	switch eventType {
	case boundaryevents.EventTypeBoundaryCreated:
		return 0
	case boundaryevents.EventTypeBoundaryActivated:
		return 1
	case boundaryevents.EventTypeBoundaryFailed:
		return 2
	default:
		panic("unexpected lifecycle event type: " + eventType)
	}
}

func createdBoundaryEvent(t *testing.T, name string, commit, prepare int64) coreeventstore.ReadEvent {
	t.Helper()
	return lifecycleEventRead(t, boundaryevents.EventTypeBoundaryCreated, boundaryevents.BoundaryCreated{
		Boundary:    name,
		Description: "Orders context",
		Placement:   boundarymodel.Placement{Backend: "postgres", Namespace: "sales"},
	}, commit, prepare)
}

func lifecycleEventRead(t *testing.T, eventType string, data any, commit, prepare int64) coreeventstore.ReadEvent {
	t.Helper()
	raw, err := json.Marshal(data)
	require.NoError(t, err)
	return coreeventstore.ReadEvent{
		EventType: eventType,
		Data:      string(raw),
		Position: coreeventstore.Position{
			CommitPosition:  commit,
			PreparePosition: prepare,
		},
	}
}
