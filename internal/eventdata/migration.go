package eventdata

import (
	"fmt"

	"github.com/goccy/go-json"
)

// MigrateLegacyEventType is used only by versioned storage migrations. It never
// aliases legacy keys at query time or modifies nested application data.
func MigrateLegacyEventType(encoded []byte) ([]byte, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, fmt.Errorf("event data must be an object")
	}
	if err := renameLegacyType(object); err != nil {
		return nil, err
	}
	return json.Marshal(object)
}

func renameLegacyType(object map[string]json.RawMessage) error {
	value, exists := object["eventType"]
	if !exists {
		return nil
	}
	if _, collision := object["__eventType"]; collision {
		return fmt.Errorf("legacy eventType conflicts with existing __eventType")
	}
	object["__eventType"] = value
	delete(object, "eventType")
	return nil
}

// MigrateLegacyConsistency preserves observations and positions, renaming only
// the event-type key in each complete query's criteria.
func MigrateLegacyConsistency(encoded []byte) ([]byte, error) {
	var observations []map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &observations); err != nil {
		return nil, err
	}
	for _, observation := range observations {
		var query map[string]json.RawMessage
		if err := json.Unmarshal(observation["query"], &query); err != nil {
			return nil, err
		}
		var criteria []map[string]json.RawMessage
		if err := json.Unmarshal(query["criteria"], &criteria); err != nil {
			return nil, err
		}
		for _, criterion := range criteria {
			if err := renameLegacyType(criterion); err != nil {
				return nil, err
			}
		}
		var err error
		query["criteria"], err = json.Marshal(criteria)
		if err != nil {
			return nil, err
		}
		observation["query"], err = json.Marshal(query)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(observations)
}

// MigrateLegacyIndexKeys changes key references, never condition values.
func MigrateLegacyIndexKeys(encoded []byte, keyProperty string) ([]byte, bool, error) {
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &entries); err != nil {
		return nil, false, err
	}
	changed := false
	for _, entry := range entries {
		var key string
		if err := json.Unmarshal(entry[keyProperty], &key); err != nil {
			return nil, false, err
		}
		if key == "eventType" {
			entry[keyProperty] = json.RawMessage(`"__eventType"`)
			changed = true
		}
	}
	if !changed {
		return encoded, false, nil
	}
	result, err := json.Marshal(entries)
	return result, true, err
}
