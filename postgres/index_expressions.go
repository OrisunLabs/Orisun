package postgres

import (
	"fmt"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
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

	// Envelope keys resolve to their columns, exactly as criteria render them,
	// so the planner matches index expressions and partial predicates.
	row := storedRow("")
	exprs := make([]string, len(fields))
	for i, f := range fields {
		scalar, _, _ := row.field(f.JsonKey)
		switch f.ValueType {
		case "numeric":
			exprs[i] = "(" + scalar + "::numeric)"
		case "boolean":
			exprs[i] = "(" + scalar + "::boolean)"
		case "timestamptz":
			exprs[i] = "(" + scalar + "::timestamptz)"
		default: // "text"
			exprs[i] = scalar
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
			scalar, _, _ := row.field(c.Key)
			predicates[i] = scalar + " " + c.Operator + " " + postgresQuoteLiteral(c.Value)
		}
		whereClause = " WHERE " + strings.Join(predicates, " "+combinator+" ")
	}

	return strings.Join(exprs, ", "), whereClause, nil
}
