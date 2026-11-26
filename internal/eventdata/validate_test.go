package eventdata

import (
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestValidateObject(t *testing.T) {
	for _, data := range []string{
		`{}`,
		` { "a" : 1 } `,
		`{"a":{"__nested":1},"b":["__value",{"__x":1}],"c":"__string"}`,
		`{"_single":1,"a__b":2}`,
		`{"a":"quote \" , \"__fake\":1"}`,
		`{"a":"__value","b":"{","c":"}"}`,
	} {
		require.NoError(t, ValidateObject(data), data)
	}
	for data, message := range map[string]string{
		`invalid`:               "not valid JSON",
		``:                      "not valid JSON",
		`{"a":1`:                "not valid JSON",
		`null`:                  "must be an object",
		`[{"a":1}]`:             "must be an object",
		`"text"`:                "must be an object",
		`{"__eventId":"spoof"}`: "reserved",
		`{"a":1,"__other":2}`:   "reserved",
		`{"a":{"b":1},"__c":2}`: "reserved",
		`{"a":[1,2],"__c":2}`:   "reserved",
		`{"__eventId":"x"}`:     "reserved",
		`{"__x":1}`:             "reserved",
	} {
		err := ValidateObject(data)
		require.Error(t, err, data)
		require.Contains(t, err.Error(), message, data)
	}
}

// The scanner must agree with a real decode on every root key it can see.
func TestReservedRootKeyMatchesDecodedKeys(t *testing.T) {
	values := []string{`1`, `"s"`, `"__v"`, `null`, `[{"__n":1},"x"]`, `{"__n":{"__m":2}}`, `"a,\"b"`, `[]`, `{}`}
	keys := []string{"a", "__r", "b\"c", "_x", "é", "__é"}
	for _, key := range keys {
		for _, value := range values {
			for _, other := range keys {
				encodedKey, err := json.Marshal(key)
				require.NoError(t, err)
				encodedOther, err := json.Marshal(other)
				require.NoError(t, err)
				data := "{" + string(encodedKey) + ":" + value + "," + string(encodedOther) + ":" + value + "}"
				var decoded map[string]json.RawMessage
				require.NoError(t, json.Unmarshal([]byte(data), &decoded))
				want := false
				for name := range decoded {
					want = want || strings.HasPrefix(name, "__")
				}
				_, got, err := reservedRootKey(data)
				require.NoError(t, err)
				require.Equal(t, want, got, data)
			}
		}
	}
}
