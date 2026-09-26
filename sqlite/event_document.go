package sqlite

import (
	"time"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// The insertion boundary owns fields assigned after consistency checks and ID allocation.
func positionedDocument(event eventstore.PreparedEvent, tx, gid int64, created time.Time) (string, error) {
	return eventdata.WithFields(event.DataJSON, map[string]any{
		"__eventId": event.EventId, "__eventType": event.EventType,
		"__metadata": eventdata.MetadataValue(event.MetadataJSON), "__dateCreated": created.UTC().Format(time.RFC3339Nano),
		"__commitPosition": tx, "__preparePosition": gid, "__writeId": eventstore.WriteID(tx, tx),
	})
}
