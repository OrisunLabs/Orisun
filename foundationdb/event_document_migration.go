package foundationdb

import (
	"fmt"

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
