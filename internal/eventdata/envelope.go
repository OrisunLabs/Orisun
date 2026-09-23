package eventdata

import (
	"bytes"
	"fmt"
	"time"

	"github.com/goccy/go-json"
)

// Envelope describes the logical queryable fields independently of storage format.
// FoundationDB obtains positions from its native commit-ordered key.
type Envelope struct {
	EventID, EventType, WriteID, Metadata string
	CommitPosition, PreparePosition       int64
	DateCreated                           time.Time
}

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

// EnvelopeFields supplies live filters with the same logical document as the
// backend, while the delivered event retains its public envelope shape.
func EnvelopeFields(data string, envelope Envelope) (map[string]any, error) {
	var fields map[string]any
	if err := json.Unmarshal([]byte(data), &fields); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("event data must be an object")
	}
	var metadata any
	decoder := json.NewDecoder(bytes.NewReader(MetadataValue(envelope.Metadata)))
	decoder.UseNumber()
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}
	fields["__eventId"] = envelope.EventID
	fields["__eventType"] = envelope.EventType
	fields["__writeId"] = envelope.WriteID
	fields["__commitPosition"] = envelope.CommitPosition
	fields["__preparePosition"] = envelope.PreparePosition
	fields["__dateCreated"] = envelope.DateCreated.UTC().Format(time.RFC3339Nano)
	fields["__metadata"] = metadata
	return fields, nil
}
