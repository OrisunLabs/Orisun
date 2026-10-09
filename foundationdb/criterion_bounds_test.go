package foundationdb

import (
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestCommitPredicateBounds(t *testing.T) {
	for _, tc := range []struct {
		op, value    string
		lower, upper int64
		ok           bool
	}{
		{"gt", "2", 3, math.MaxInt64, true}, {"gte", "2", 2, math.MaxInt64, true},
		{"lt", "2", 0, 1, true}, {"lte", "2", 0, 2, true},
		{"gt", "2.5", 3, math.MaxInt64, true}, {"lte", "2.5", 0, 2, true},
		{"gt", "-10", 0, math.MaxInt64, true}, {"lt", "0", 0, 0, false},
		{"gt", "9223372036854775807", 0, 0, false}, {"gte", "9223372036854775807", math.MaxInt64, math.MaxInt64, true},
		{"lt", "1e100", 0, math.MaxInt64, true}, {"gt", "bad", 0, 0, false},
		{"ne", "2", 0, math.MaxInt64, true},
	} {
		lower, upper, ok := commitBounds([]orisun.TagPredicate{{Operator: tc.op, Value: tc.value}})
		require.Equal(t, tc.ok, ok, tc.op+tc.value)
		if ok {
			require.Equal(t, tc.lower, lower)
			require.Equal(t, tc.upper, upper)
		}
	}
	lower, upper, ok := commitBounds([]orisun.TagPredicate{{Operator: "gte", Value: "10"}, {Operator: "lt", Value: "20"}})
	require.True(t, ok)
	require.EqualValues(t, 10, lower)
	require.EqualValues(t, 19, upper)
}
