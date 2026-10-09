package eventdata

import (
	"fmt"

	"github.com/goccy/go-json"
)

// WithFields builds a stored document without converting application numbers
// through floating point. Only the backend may supply reserved fields.
func WithFields(data string, values map[string]any) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &fields); err != nil {
		return "", err
	}
	if fields == nil {
		return "", fmt.Errorf("event data must be an object")
	}
	for key, value := range values {
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		fields[key] = encoded
	}
	encoded, err := json.Marshal(fields)
	return string(encoded), err
}

func MetadataValue(metadata string) json.RawMessage {
	if metadata == "" {
		return json.RawMessage(`null`)
	}
	return json.RawMessage(metadata)
}
