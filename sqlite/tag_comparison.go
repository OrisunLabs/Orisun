package sqlite

import (
	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"zombiezen.com/go/sqlite"
)

// SQLite's REAL affinity rounds large integers and high precision decimals.
// Compare the original JSON number tokens instead, so CCC and live filters
// cannot disagree at a range boundary.
func registerTagComparison(conn *sqlite.Conn) error {
	return conn.CreateFunction("orisun_compare_number", &sqlite.FunctionImpl{
		NArgs: 2, Deterministic: true,
		Scalar: func(_ sqlite.Context, args []sqlite.Value) (sqlite.Value, error) {
			cmp, ok := eventdata.CompareJSONNumbers(args[0].Text(), args[1].Text())
			if !ok {
				return sqlite.Value{}, nil
			}
			return sqlite.IntegerValue(int64(cmp)), nil
		},
	})
}
