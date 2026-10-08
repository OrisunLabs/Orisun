package orisun

import (
	"strconv"
	"strings"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"github.com/goccy/go-json"
)

// MatchTagValue supplies the same scalar comparison semantics for live
// subscriptions and storage engines that evaluate predicates in Go. A missing
// field must be rejected by the caller; JSON null never matches any operator.
func MatchTagValue(value any, target, operator string) bool {
	op, err := CanonicalTagOperator(operator)
	if err != nil || value == nil {
		return false
	}
	if op == "eq" {
		return eventTagEquals(value, target)
	}
	if op == "ne" {
		return !eventTagEquals(value, target)
	}
	var cmp int
	var comparable bool
	switch v := value.(type) {
	case string:
		cmp, comparable = strings.Compare(v, target), true
	case json.Number:
		cmp, comparable = eventdata.CompareJSONNumbers(string(v), target)
	case float64:
		cmp, comparable = eventdata.CompareJSONNumbers(strconv.FormatFloat(v, 'g', -1, 64), target)
	case int64:
		cmp, comparable = eventdata.CompareJSONNumbers(strconv.FormatInt(v, 10), target)
	case int:
		cmp, comparable = eventdata.CompareJSONNumbers(strconv.Itoa(v), target)
	}
	if !comparable {
		return false
	}
	switch op {
	case "gt":
		return cmp > 0
	case "gte":
		return cmp >= 0
	case "lt":
		return cmp < 0
	case "lte":
		return cmp <= 0
	}
	return false
}
