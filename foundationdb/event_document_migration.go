package foundationdb

import (
	"fmt"
	"time"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"github.com/goccy/go-json"
)

// Preserve fields owned by later migrations when converting an older record.
func migrateRecordEventType(raw []byte) ([]byte, error) {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	var data string
	if err := json.Unmarshal(record["data"], &data); err != nil {
		return nil, err
	}
	converted, err := eventdata.MigrateLegacyEventType([]byte(data))
	if err != nil {
		return nil, err
	}
	record["data"], err = json.Marshal(string(converted))
	if err != nil {
		return nil, err
	}
	delete(record, "event_type")
	return json.Marshal(record)
}

func migrateRecordEventID(raw []byte) ([]byte, error) {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	var data, id string
	if err := json.Unmarshal(record["data"], &data); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(record["event_id"], &id); err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("event data must be an object")
	}
	if _, exists := fields["__eventId"]; exists {
		return nil, fmt.Errorf("legacy event %q conflicts with reserved __eventId", id)
	}
	fields["__eventId"] = record["event_id"]
	converted, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	record["data"], err = json.Marshal(string(converted))
	if err != nil {
		return nil, err
	}
	delete(record, "event_id")
	return json.Marshal(record)
}

// Metadata and timestamps become document fields. The native position key and
// stored last-offset component remain the authority for commit-derived fields.
func migrateRecordEnvelope(raw []byte) ([]byte, error) {
	var record map[string]json.RawMessage
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	var data string
	if err := json.Unmarshal(record["data"], &data); err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("event data must be an object")
	}
	for _, key := range []string{"__metadata", "__dateCreated", "__writeLastOffset", "__commitPosition", "__preparePosition", "__writeId"} {
		if _, ok := fields[key]; ok {
			return nil, fmt.Errorf("legacy event data conflicts with reserved %s", key)
		}
	}
	var metadata, created string
	if raw := record["metadata"]; raw != nil {
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return nil, err
		}
	}
	if err := json.Unmarshal(record["date_created"], &created); err != nil {
		return nil, err
	}
	date, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return nil, err
	}
	var last *uint16
	if raw := record["write_last_offset"]; raw != nil {
		if err := json.Unmarshal(raw, &last); err != nil {
			return nil, err
		}
	}
	stored, err := eventdata.WithFields(data, map[string]any{"__metadata": eventdata.MetadataValue(metadata), "__dateCreated": date.UTC().Format(time.RFC3339Nano), "__writeLastOffset": last})
	if err != nil {
		return nil, err
	}
	return json.Marshal(eventRecord{Data: stored})
}
