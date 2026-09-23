package foundationdb

import (
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestEventDocumentMigrationPreservesEnvelopeAndPrecision(t *testing.T) {
	original := []byte(`{"event_id":"legacy","event_type":"Created","data":"{\"eventType\":\"Created\",\"number\":9223372036854775807}","date_created":"unchanged"}`)
	typed, err := migrateRecordEventType(original)
	require.NoError(t, err)
	migrated, err := migrateRecordEventID(typed)
	require.NoError(t, err)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(migrated, &record))
	require.NotContains(t, record, "event_id")
	require.NotContains(t, record, "event_type")
	require.Equal(t, `"unchanged"`, string(record["date_created"]))
	var data string
	require.NoError(t, json.Unmarshal(record["data"], &data))
	require.JSONEq(t, `{"__eventId":"legacy","__eventType":"Created","number":9223372036854775807}`, data)
	require.Contains(t, data, "9223372036854775807")
}

func TestEventDocumentIDMigrationRejectsCollision(t *testing.T) {
	_, err := migrateRecordEventID([]byte(`{"event_id":"legacy","data":"{\"__eventId\":null}"}`))
	require.ErrorContains(t, err, "conflicts")
}
