package postgres

import (
	"fmt"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/lib/pq"
	"strings"
)

// boundaryIndexExpressions is shared by online creation and transactional migrations.
func boundaryIndexExpressions(fields []eventstore.BoundaryIndexField, conditions []eventstore.BoundaryIndexCondition, combinator string) (string, string, error) {
	keys, predicate, err := boundaryIndexKeyExpressions(fields, conditions, combinator)
	if err != nil {
		return "", "", err
	}
	// CCC and ordered reads need the latest full position within the indexed
	// context. Keep filtering and ordering in the same B-tree access path.
	return keys + ", transaction_id DESC, global_id DESC", predicate, nil
}

// The key-only definition is also used to verify indexes during their migration.
func boundaryIndexKeyExpressions(fields []eventstore.BoundaryIndexField, conditions []eventstore.BoundaryIndexCondition, combinator string) (string, string, error) {
	if len(fields) == 0 {
		return "", "", fmt.Errorf("at least one field is required")
	}
	if combinator == "" {
		combinator = eventstore.IndexCombinatorAND
	}

	// Build index expression list
	exprs := make([]string, len(fields))
	for i, f := range fields {
		key := pq.QuoteLiteral(f.JsonKey)
		switch f.ValueType {
		case "numeric":
			exprs[i] = "((data->>" + key + ")::numeric)"
		case "boolean":
			exprs[i] = "((data->>" + key + ")::boolean)"
		case "timestamptz":
			exprs[i] = "((data->>" + key + ")::timestamptz)"
		default: // "text"
			exprs[i] = "(data->>" + key + ")"
		}
	}

	// Build WHERE clause
	var whereClause string
	if len(conditions) > 0 {
		validOps := map[string]bool{"=": true, ">": true, "<": true, ">=": true, "<=": true}
		validCombinators := map[string]bool{eventstore.IndexCombinatorAND: true, eventstore.IndexCombinatorOR: true}

		if !validCombinators[combinator] {
			return "", "", fmt.Errorf("invalid combinator %q: must be AND or OR", combinator)
		}

		predicates := make([]string, len(conditions))
		for i, c := range conditions {
			if !validOps[c.Operator] {
				return "", "", fmt.Errorf("invalid operator %q: must be one of =, >, <, >=, <=", c.Operator)
			}
			predicates[i] = "(data->>" + pq.QuoteLiteral(c.Key) + ") " + c.Operator + " " + pq.QuoteLiteral(c.Value)
		}
		whereClause = " WHERE " + strings.Join(predicates, " "+combinator+" ")
	}

	return strings.Join(exprs, ", "), whereClause, nil
}
