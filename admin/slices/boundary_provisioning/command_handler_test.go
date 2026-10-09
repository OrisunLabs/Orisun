package boundary_provisioning

import (
	"context"
	"testing"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	boundaryevents "github.com/OrisunLabs/Orisun/boundary/events"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	"github.com/stretchr/testify/require"
)

func TestProvisionBoundaryCommandHandlerInvokesFunctionAndEmitsActivation(t *testing.T) {
	definitionEvent := createdBoundaryEvent(t, "orders", 10, 2)
	retriever := &scriptedBoundaryRetriever{batches: []coreeventstore.LatestByCriteriaResult{lifecycleBatch(definitionEvent)}}
	saver := &scriptedOutcomeSaver{}
	called := false
	provision := func(_ context.Context, definition boundarymodel.Definition) error {
		called = true
		require.Equal(t, "orders", definition.Name)
		return nil
	}

	result, err := ProvisionBoundaryCommandHandler(context.Background(), ProvisionBoundaryCommand{
		Definition: boundarymodel.Definition{
			Name:        "orders",
			Description: "Orders context",
			Placement:   boundarymodel.Placement{Backend: "postgres", Namespace: "sales"},
		},
		Metadata: map[string]any{"source": "test"},
	}, "orisun_admin", saver, retriever, provision)
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, boundarymodel.StatusActive, result.Boundary.Status)
	require.Len(t, saver.calls, 1)
	require.Equal(t, boundaryevents.EventTypeBoundaryActivated, saver.calls[0].events[0].EventType)
}

func TestProvisionBoundaryCommandHandlerDoesNotProvisionWithoutDefinitionEvent(t *testing.T) {
	retriever := &scriptedBoundaryRetriever{batches: []coreeventstore.LatestByCriteriaResult{{
		Matches:         make([]coreeventstore.LatestCriterionMatch, 4),
		ContextPosition: coreeventstore.NotExistsPosition(),
	}}}
	called := false
	_, err := ProvisionBoundaryCommandHandler(context.Background(), ProvisionBoundaryCommand{
		Definition: boundarymodel.Definition{
			Name:      "orders",
			Placement: boundarymodel.Placement{Backend: "postgres", Namespace: "sales"},
		},
	}, "orisun_admin", &scriptedOutcomeSaver{}, retriever, func(context.Context, boundarymodel.Definition) error {
		called = true
		return nil
	})
	require.Error(t, err)
	require.False(t, called)
}
