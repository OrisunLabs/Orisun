package postgres

import (
	"fmt"
	"sort"
	"strings"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// criterionSource locates criterion keys. Stored rows keep the envelope in
// typed columns and application fields in data; in-batch candidates are one
// JSON document holding both. orisun_stored_criterion_sql renders stored rows
// identically for reads.
type criterionSource struct {
	document string // candidate document expression; empty for stored rows
	prefix   string // stored row qualifier, such as "stored."
}

func storedRow(prefix string) criterionSource { return criterionSource{prefix: prefix} }

func candidateDocument(expression string) criterionSource {
	return criterionSource{document: expression}
}

type fieldKind int

const (
	jsonField     fieldKind = iota // typed by jsonb_typeof at evaluation time
	stringColumn                   // always a string, or NULL when absent
	numericColumn                  // always a JSON number
)

// field returns a key's text value, how ordered comparisons treat it, and,
// for JSON fields, the JSONB expression whose type selects the comparison.
func (s criterionSource) field(key string) (scalar string, kind fieldKind, value string) {
	if s.document != "" {
		literal := postgresQuoteLiteral(key)
		return "(" + s.document + " ->> " + literal + ")", jsonField, s.document + " -> " + literal
	}
	p := s.prefix
	switch key {
	case "__eventId":
		return "(" + p + "event_id::TEXT)", stringColumn, ""
	case "__eventType":
		return "(" + p + "event_type)", stringColumn, ""
	case "__dateCreated":
		return "(" + p + "date_created)", stringColumn, ""
	case "__writeId":
		// NULL for events saved before write contexts were recorded.
		return "(" + p + "transaction_id::TEXT || ':' || " + p + "write_id::TEXT)", stringColumn, ""
	case "__commitPosition":
		return "(" + p + "transaction_id::TEXT)", numericColumn, ""
	case "__preparePosition":
		return "(" + p + "global_id::TEXT)", numericColumn, ""
	case "__metadata":
		// Metadata is one JSON value; its root stands in for the field.
		return "(" + p + "metadata #>> '{}')", jsonField, p + "metadata"
	}
	literal := postgresQuoteLiteral(key)
	return "(" + p + "data ->> " + literal + ")", jsonField, p + "data -> " + literal
}

// criterionSQL renders one AND criterion. Equality fields come first so cheap
// guards run before typed comparisons where PostgreSQL retains order.
func (s criterionSource) criterionSQL(fields map[string]any) (string, error) {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		_, iString := fields[keys[i]].(string)
		_, jString := fields[keys[j]].(string)
		if iString != jString {
			return iString
		}
		return keys[i] < keys[j]
	})
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		predicates, err := eventstore.DecodeTagPredicates(fields[key])
		if err != nil {
			return "", err
		}
		scalar, kind, value := s.field(key)
		for _, predicate := range predicates {
			operator, err := eventstore.CanonicalTagOperator(predicate.Operator)
			if err != nil {
				return "", err
			}
			sqlOperator, _ := eventstore.TagSQLOperator(operator)
			target := postgresQuoteLiteral(predicate.Value)
			switch {
			case operator == "eq" || operator == "ne":
				parts = append(parts, "("+scalar+" "+sqlOperator+" "+target+")")
			case kind == stringColumn:
				parts = append(parts, fmt.Sprintf(`(%s COLLATE "C" %s %s COLLATE "C")`, scalar, sqlOperator, target))
			case kind == numericColumn:
				parts = append(parts, fmt.Sprintf(`(orisun_compare_number(%s, %s) %s 0)`, scalar, target, sqlOperator))
			default:
				parts = append(parts, fmt.Sprintf(
					`(CASE jsonb_typeof(%s) WHEN 'string' THEN (%s COLLATE "C" %s %s COLLATE "C") WHEN 'number' THEN (orisun_compare_number(%s, %s) %s 0) ELSE FALSE END)`,
					value, scalar, sqlOperator, target, scalar, target, sqlOperator))
			}
		}
	}
	if len(parts) == 0 {
		return "TRUE", nil
	}
	return "(" + strings.Join(parts, " AND ") + ")", nil
}

// postgresQuoteLiteral matches PostgreSQL quote_literal: quotes and
// backslashes are doubled, and a backslash selects the E” form.
func postgresQuoteLiteral(value string) string {
	var b strings.Builder
	if strings.Contains(value, `\`) {
		b.WriteByte('E')
	}
	b.WriteByte('\'')
	for i := 0; i < len(value); i++ {
		if value[i] == '\'' || value[i] == '\\' {
			b.WriteByte(value[i])
		}
		b.WriteByte(value[i])
	}
	b.WriteByte('\'')
	return b.String()
}
