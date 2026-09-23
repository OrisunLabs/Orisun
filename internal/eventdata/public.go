// Package eventdata owns the translation between Orisun's queryable storage
// representation and consumer-facing domain event data.
package eventdata

import "github.com/goccy/go-json"

// WithoutStorageEnvelope removes reserved top-level fields from backend read
// results. Persisted fields remain available for content queries and indexes.
//
// Invalid JSON is returned unchanged. Storage backends already enforce valid
// event objects, and preserving unexpected input keeps this translation from
// hiding the original corruption from downstream strict decoders.
func WithoutStorageEnvelope(encoded string) string {
	if encoded == "" {
		return encoded
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(encoded), &object); err != nil {
		return encoded
	}
	changed := false
	for key := range object {
		if IsReservedKey(key) {
			delete(object, key)
			changed = true
		}
	}
	if !changed {
		return encoded
	}
	publicData, err := json.Marshal(object)
	if err != nil {
		return encoded
	}
	return string(publicData)
}
