// Package eventdata owns the translation between Orisun's queryable storage
// representation and consumer-facing domain event data.
package eventdata

import "encoding/json"

// WithoutStorageEventType removes Orisun's top-level eventType discriminator
// from consumer-facing event data. The discriminator remains in persisted and
// internally published JSON so criteria and indexes can continue to use it.
//
// Invalid JSON is returned unchanged. Storage backends already enforce valid
// event objects, and preserving unexpected input keeps this translation from
// hiding the original corruption from downstream strict decoders.
func WithoutStorageEventType(encoded string) string {
	if encoded == "" {
		return encoded
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &object); err != nil {
		return encoded
	}
	if _, exists := object["eventType"]; !exists {
		return encoded
	}
	delete(object, "eventType")
	publicData, err := json.Marshal(object)
	if err != nil {
		return encoded
	}
	return string(publicData)
}
