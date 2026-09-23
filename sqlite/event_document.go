package sqlite

import (
	"time"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// prepareStoredEvents is owned by the writer lifecycle. Backend callers supply
// ordinary envelopes; only the batch used for persistence contains storage keys.
func prepareStoredEvents(events eventstore.PreparedEventBatch) (eventstore.PreparedEventBatch, error) {
	stored := make(eventstore.PreparedEventBatch, len(events))
	for i, event := range events {
		data, err := eventdata.WithFields(event.DataJSON, map[string]any{"__eventId": event.EventId, "__eventType": event.EventType, "__metadata": eventdata.MetadataValue(event.MetadataJSON)})
		if err != nil {
			return nil, err
		}
		stored[i] = event
		stored[i].DataJSON = data
	}
	return stored, nil
}

// The insertion boundary owns fields assigned after consistency checks and ID allocation.
func positionedDocument(event eventstore.PreparedEvent, tx, gid int64, created time.Time) (string, error) {
	return eventdata.WithFields(event.DataJSON, map[string]any{
		"__eventId": event.EventId, "__eventType": event.EventType,
		"__metadata": eventdata.MetadataValue(event.MetadataJSON), "__dateCreated": created.UTC().Format(time.RFC3339Nano),
		"__commitPosition": tx, "__preparePosition": gid, "__writeId": eventstore.WriteID(tx, tx),
	})
}
