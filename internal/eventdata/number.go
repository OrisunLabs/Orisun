package eventdata

import (
	"math/big"
	"regexp"
	"strings"
)

var jsonNumberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// CompareJSONNumbers compares decimal JSON numbers exactly, without converting
// integers to float64 or allocating exponent-sized buffers. Invalid operands
// are incomparable, rather than silently converted to zero.
func CompareJSONNumbers(a, b string) (int, bool) {
	ad, ae, an, ok := decimalParts(a)
	if !ok {
		return 0, false
	}
	bd, be, bn, ok := decimalParts(b)
	if !ok {
		return 0, false
	}
	if an != bn {
		if an {
			return -1, true
		}
		return 1, true
	}
	cmp := 0
	switch {
	case ad == "" && bd == "":
	case ad == "":
		cmp = -1
	case bd == "":
		cmp = 1
	default:
		cmp = ae.Cmp(be)
		if cmp == 0 {
			for i := 0; i < len(ad) || i < len(bd); i++ {
				ac, bc := byte('0'), byte('0')
				if i < len(ad) {
					ac = ad[i]
				}
				if i < len(bd) {
					bc = bd[i]
				}
				if ac < bc {
					cmp = -1
					break
				}
				if ac > bc {
					cmp = 1
					break
				}
			}
		}
	}
	if an {
		cmp = -cmp
	}
	return cmp, true
}

func decimalParts(s string) (digits string, magnitude *big.Int, negative, ok bool) {
	if !jsonNumberPattern.MatchString(s) {
		return "", nil, false, false
	}
	negative = s[0] == '-'
	if negative {
		s = s[1:]
	}
	magnitude = new(big.Int)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		magnitude.SetString(s[i+1:], 10)
		s = s[:i]
	}
	fractional := 0
	if i := strings.IndexByte(s, '.'); i >= 0 {
		fractional = len(s) - i - 1
		s = s[:i] + s[i+1:]
	}
	digits = strings.TrimLeft(s, "0")
	if digits == "" {
		return "", magnitude, false, true
	}
	magnitude.Add(magnitude, big.NewInt(int64(len(digits)-fractional)))
	return digits, magnitude, negative, true
}
