package eventdata

import "strings"

// IsReservedKey identifies names owned by Orisun at the root of event data.
// Nested objects and the separate metadata value do not reserve this namespace.
func IsReservedKey(key string) bool {
	return strings.HasPrefix(key, "__")
}

// IsPositionKey identifies fields that are unavailable until positions are assigned.
// FoundationDB resolves them through the native key rather than a secondary index.
func IsPositionKey(key string) bool {
	switch key {
	case "__commitPosition", "__preparePosition", "__writeId":
		return true
	}
	return false
}
