package sqlite

import (
	"strings"
)

func normalizeIndexValueType(valueType string) string {
	switch strings.ToLower(strings.TrimSpace(valueType)) {
	case "numeric":
		return "numeric"
	case "boolean":
		return "boolean"
	case "timestamptz":
		return "timestamptz"
	default:
		return "text"
	}
}

// SQLite scans an index in either direction, so ascending and descending
// position suffixes both serve ordered reads.
func sqliteIndexOrdersByPosition(ddl string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(ddl), " "))
	return strings.Contains(normalized, "transaction_id, global_id)") ||
		strings.Contains(normalized, "transaction_id desc, global_id desc)")
}
