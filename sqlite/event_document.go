package sqlite

import (
	"github.com/OrisunLabs/Orisun/internal/eventdata"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// prepareStoredEvents is owned by the writer lifecycle. Backend callers supply
// ordinary envelopes; only the batch used for persistence contains storage keys.
func prepareStoredEvents(events eventstore.PreparedEventBatch) (eventstore.PreparedEventBatch, error) {
	stored := make(eventstore.PreparedEventBatch, len(events))
	for i, event := range events {
		data, err := eventdata.WithEnvelope(event.DataJSON, event.EventId, event.EventType)
		if err != nil {
			return nil, err
		}
		stored[i] = event
		stored[i].DataJSON = data
	}
	return stored, nil
}
