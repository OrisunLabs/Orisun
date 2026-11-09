package sqlite

import (
	"bytes"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
)

// preparedEventRow holds one event's column values. Everything here is computed
// before writer ownership; the writer only assigns positions and binds.
type preparedEventRow struct {
	eventID   string
	eventType string
	metadata  string
	data      string
	err       error
}

func prepareEventRow(event eventstore.PreparedEvent) preparedEventRow {
	row := preparedEventRow{eventID: event.EventId, eventType: event.EventType, data: event.DataJSON}
	row.metadata, row.err = compactMetadata(event.MetadataJSON)
	if row.err == nil {
		row.err = eventdata.ValidateObject(event.DataJSON)
	}
	return row
}

func prepareEventRows(events eventstore.PreparedEventBatch) []preparedEventRow {
	prepared := make([]preparedEventRow, len(events))
	for i, event := range events {
		prepared[i] = prepareEventRow(event)
	}
	return prepared
}

// compactMetadata stores the same minified JSON that reads always returned.
func compactMetadata(metadata string) (string, error) {
	switch metadata {
	case "":
		return "null", nil
	case "{}":
		return metadata, nil
	}
	var compact bytes.Buffer
	compact.Grow(len(metadata))
	if err := json.Compact(&compact, []byte(metadata)); err != nil {
		return "", err
	}
	return compact.String(), nil
}
