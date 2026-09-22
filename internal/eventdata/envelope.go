package eventdata

import (
	"fmt"

	"github.com/goccy/go-json"
)

// WithEnvelope builds the backend's queryable document without changing the
// caller's data or converting numeric values through floating point.
func WithEnvelope(data, id, eventType string) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(data), &fields); err != nil {
		return "", err
	}
	if fields == nil {
		return "", fmt.Errorf("event data must be an object")
	}
	for key, value := range envelope(id, eventType) {
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		fields[key] = encoded
	}
	encoded, err := json.Marshal(fields)
	return string(encoded), err
}

// EnvelopeFields supplies the same logical content view for live filtering as
// a database query, while the event itself retains its transport-neutral shape.
func EnvelopeFields(data, id, eventType string) (map[string]any, error) {
	var fields map[string]any
	if err := json.Unmarshal([]byte(data), &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("event data must be an object")
	}
	for key, value := range envelope(id, eventType) {
		fields[key] = value
	}
	return fields, nil
}

func envelope(id, eventType string) map[string]string {
	return map[string]string{"__eventId": id, "__eventType": eventType}
}
