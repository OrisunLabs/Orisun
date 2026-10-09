package eventdata

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompareJSONNumbers(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		cmp  int
	}{
		{"0", "-0.000e900", 0}, {"1", "1.00e0", 0}, {"10", "2", 1}, {"-10", "-2", -1},
		{"9007199254740993", "9007199254740992", 1}, {"0.10000000000000000001", "0.1", 1},
		{"1e999999999999999999999999999999", "2e999999999999999999999999999998", 1},
		{"1e-999999999999999999999999999999", "0", 1}, {"-1e100", "0", -1}, {"0", "0.1", -1},
	} {
		cmp, ok := CompareJSONNumbers(tc.a, tc.b)
		require.True(t, ok)
		require.Equal(t, tc.cmp, cmp, tc.a+" vs "+tc.b)
		cmp, ok = CompareJSONNumbers(tc.b, tc.a)
		require.True(t, ok)
		require.Equal(t, -tc.cmp, cmp)
	}
	for _, invalid := range []string{"", "01", "+1", "NaN", "Infinity", "1.", " 1", "1\n", "1e", "0x10"} {
		_, ok := CompareJSONNumbers("1", invalid)
		require.False(t, ok, invalid)
	}
}
