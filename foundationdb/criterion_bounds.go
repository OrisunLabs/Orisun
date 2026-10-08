package foundationdb

import (
	"math"
	"strconv"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"github.com/OrisunLabs/Orisun/orisun"
)

// commitBounds computes the smallest contiguous native commit interval covering
// the predicate. ne leaves a hole which the document matcher removes. Decimal
// targets are compared exactly even though native positions are integers.
func commitBounds(encoded any) (int64, int64, bool) {
	predicates, err := orisun.DecodeTagPredicates(encoded)
	if err != nil {
		return 0, 0, false
	}
	lower, upper := int64(0), int64(math.MaxInt64)
	for _, predicate := range predicates {
		op, _ := orisun.CanonicalTagOperator(predicate.Operator)
		switch op {
		case "eq":
			value, err := strconv.ParseInt(predicate.Value, 10, 64)
			if err != nil || value < 0 || strconv.FormatInt(value, 10) != predicate.Value {
				return 0, 0, false
			}
			lower = max(lower, value)
			upper = min(upper, value)
		case "ne": // The full comparator excludes the unequal value.
		case "gt", "gte", "lt", "lte":
			if _, valid := eventdata.CompareJSONNumbers("0", predicate.Value); !valid {
				return 0, 0, false
			}
			strict := op == "gt" || op == "lte"
			first, found := firstCommit(predicate.Value, strict)
			if op == "gt" || op == "gte" {
				if !found {
					return 0, 0, false
				}
				lower = max(lower, first)
			} else if found {
				if first == 0 {
					return 0, 0, false
				}
				upper = min(upper, first-1)
			}
		default:
			return 0, 0, false
		}
	}
	return lower, upper, lower <= upper
}

func firstCommit(target string, strict bool) (int64, bool) {
	accepts := func(value int64) bool {
		cmp, _ := eventdata.CompareJSONNumbers(strconv.FormatInt(value, 10), target)
		return cmp > 0 || (!strict && cmp == 0)
	}
	low, high := int64(0), int64(math.MaxInt64)
	if !accepts(high) {
		return 0, false
	}
	for low < high {
		middle := low + (high-low)/2
		if accepts(middle) {
			high = middle
		} else {
			low = middle + 1
		}
	}
	return low, true
}
