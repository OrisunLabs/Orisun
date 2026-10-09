package boundary_provisioning

import (
	"context"
	"errors"
	"testing"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/stretchr/testify/require"
)

func TestRuntimeEventHandlerInstallsThenActivatesLocally(t *testing.T) {
	definitionEvent := createdBoundaryEvent(t, "orders", 10, 2)
	activationEvent := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryActivated, boundaryevents.BoundaryActivated{
		Boundary: "orders",
	}, 11, 3)
	installer := &captureInstaller{}
	activator := &captureActivator{}
	handler := NewBoundaryRuntimeEventHandler(
		"orisun_admin",
		runtimeCatalogRetriever{events: coreeventstore.ReadEventBatch{definitionEvent, activationEvent}},
		installer.InstallBoundary,
		activator.ActivateBoundary,
	)

	require.NoError(t, handler.Handle(t.Context(), activationEvent))
	require.Equal(t, []boundarymodel.Definition{{
		Name:        "orders",
		Description: "Orders context",
		Placement:   boundarymodel.Placement{Backend: "postgres", Namespace: "sales"},
	}}, installer.definitions)
	require.Equal(t, []string{"orders"}, activator.boundaries)
}

func TestRuntimeEventHandlerKeepsGateClosedWhenLocalInstallFails(t *testing.T) {
	definitionEvent := createdBoundaryEvent(t, "orders", 10, 2)
	activationEvent := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryActivated, boundaryevents.BoundaryActivated{
		Boundary: "orders",
	}, 11, 3)
	installErr := errors.New("register LISTEN: unavailable")
	installer := &captureInstaller{err: installErr}
	activator := &captureActivator{}
	handler := NewBoundaryRuntimeEventHandler(
		"orisun_admin",
		runtimeCatalogRetriever{events: coreeventstore.ReadEventBatch{definitionEvent, activationEvent}},
		installer.InstallBoundary,
		activator.ActivateBoundary,
	)

	require.ErrorIs(t, handler.Handle(t.Context(), activationEvent), installErr)
	require.Empty(t, activator.boundaries)
}

func TestRuntimeEventHandlerIgnoresActivationBeforeCurrentDefinition(t *testing.T) {
	staleActivation := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryActivated, boundaryevents.BoundaryActivated{
		Boundary: "orders",
	}, 9, 1)
	definitionEvent := createdBoundaryEvent(t, "orders", 10, 2)
	installer := &captureInstaller{}
	activator := &captureActivator{}
	handler := NewBoundaryRuntimeEventHandler(
		"orisun_admin",
		runtimeCatalogRetriever{events: coreeventstore.ReadEventBatch{staleActivation, definitionEvent}},
		installer.InstallBoundary,
		activator.ActivateBoundary,
	)

	require.NoError(t, handler.Handle(t.Context(), staleActivation))
	require.Empty(t, installer.definitions)
	require.Empty(t, activator.boundaries)
}

func TestRuntimeEventHandlerIgnoresOrphanActivation(t *testing.T) {
	orphanActivation := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryActivated, boundaryevents.BoundaryActivated{
		Boundary: "removed_boundary",
	}, 9, 1)
	installer := &captureInstaller{}
	activator := &captureActivator{}
	handler := NewBoundaryRuntimeEventHandler(
		"orisun_admin",
		runtimeCatalogRetriever{},
		installer.InstallBoundary,
		activator.ActivateBoundary,
	)

	require.NoError(t, handler.Handle(t.Context(), orphanActivation))
	require.Empty(t, installer.definitions)
	require.Empty(t, activator.boundaries)
}

func TestRuntimeEventHandlerPreservesExistingStorageFlag(t *testing.T) {
	definitionEvent := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryCreated, boundaryevents.BoundaryCreated{
		Boundary:    "orders",
		Description: "Orders context",
		Placement:   boundarymodel.Placement{Backend: "postgres", Namespace: "sales"},
	}, 10, 2)
	activationEvent := lifecycleEventRead(t, boundaryevents.EventTypeBoundaryActivated, boundaryevents.BoundaryActivated{
		Boundary: "orders",
	}, 11, 3)
	installer := &captureInstaller{}
	handler := NewBoundaryRuntimeEventHandler(
		"orisun_admin",
		runtimeCatalogRetriever{events: coreeventstore.ReadEventBatch{definitionEvent, activationEvent}},
		installer.InstallBoundary,
		(&captureActivator{}).ActivateBoundary,
	)

	require.NoError(t, handler.Handle(t.Context(), activationEvent))
	require.Len(t, installer.definitions, 1)
}

type runtimeCatalogRetriever struct {
	events coreeventstore.ReadEventBatch
}

func (r runtimeCatalogRetriever) Read(context.Context, coreeventstore.ReadRequest) (coreeventstore.ReadEventBatch, error) {
	return r.events, nil
}

type captureInstaller struct {
	definitions []boundarymodel.Definition
	err         error
}

func (i *captureInstaller) InstallBoundary(_ context.Context, definition boundarymodel.Definition) error {
	i.definitions = append(i.definitions, definition)
	return i.err
}
