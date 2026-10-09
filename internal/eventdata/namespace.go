package eventdata

import "strings"

// IsReservedKey identifies names owned by Orisun at the root of event data.
// Nested objects and the separate metadata value do not reserve this namespace.
func IsReservedKey(key string) bool {
	return strings.HasPrefix(key, "__")
}
