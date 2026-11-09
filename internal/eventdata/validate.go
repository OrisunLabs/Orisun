package eventdata

import (
	"errors"
	"fmt"
	"strings"

	"github.com/goccy/go-json"
)

// ValidateObject accepts a JSON object whose root owns no reserved key. Stored
// application data keeps its bytes, so this never decodes values into a map.
func ValidateObject(data string) error {
	if !json.Valid([]byte(data)) {
		return errors.New("event data is not valid JSON")
	}
	if !strings.HasPrefix(strings.TrimLeft(data, " \t\r\n"), "{") {
		return errors.New("event data must be an object")
	}
	key, reserved, err := reservedRootKey(data)
	if err != nil {
		return err
	}
	if reserved {
		return fmt.Errorf("top-level field %q is reserved for Orisun (prefix __)", key)
	}
	return nil
}

// reservedRootKey walks an already validated object. A string is a root key
// only when it opens the root object or follows a root-level comma.
func reservedRootKey(data string) (string, bool, error) {
	depth, expectKey := 0, false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case '{':
			depth++
			expectKey = depth == 1
		case '[':
			depth++
		case '}', ']':
			depth--
		case ',':
			expectKey = depth == 1
		case '"':
			start := i + 1
			for i++; data[i] != '"'; i++ {
				if data[i] == '\\' {
					i++
				}
			}
			if !expectKey {
				continue
			}
			expectKey = false
			key := data[start:i]
			if strings.IndexByte(key, '\\') >= 0 {
				// Escapes can spell a reserved prefix, as in "__eventId".
				if err := json.Unmarshal([]byte(data[start-1:i+1]), &key); err != nil {
					return "", false, err
				}
			}
			if IsReservedKey(key) {
				return key, true, nil
			}
		}
	}
	return "", false, nil
}
