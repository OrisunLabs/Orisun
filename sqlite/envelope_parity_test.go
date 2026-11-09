package sqlite

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
)

// legacyPredicate is the v0.14.3 renderer, which read every key from the document.
func legacyPredicate(key, op, lit string) string {
	path := jsonPathLiteral(key)
	text := fmt.Sprintf("CASE json_type(data, %s) WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' ELSE CAST(json_extract(data, %s) AS TEXT) END", path, path)
	if op == "=" || op == "<>" {
		return "(" + text + " " + op + " " + lit + ")"
	}
	return fmt.Sprintf("((CASE json_type(data, %s) WHEN 'text' THEN (%s COLLATE BINARY %s %s) WHEN 'integer' THEN (orisun_compare_number(data -> %s, %s) %s 0) WHEN 'real' THEN (orisun_compare_number(data -> %s, %s) %s 0) ELSE 0 END))", path, text, op, lit, path, lit, op, path, lit, op)
}

// Every envelope key must select the same events before and after the split.
func TestEnvelopeCriteriaMatchLegacyDocuments(t *testing.T) {
	const documents = `
 INSERT INTO orisun_es_write VALUES(2, '[]'), (5, '[]'), (10, '[]');
 INSERT INTO orisun_es_event(data) VALUES
 ('{"__eventId":"a","__eventType":"Alpha","__commitPosition":2,"__preparePosition":1,"__writeId":"2:2","__dateCreated":"2026-10-09T00:00:00Z","__metadata":{"tier":"gold","n":1},"k":"v"}'),
 ('{"__eventId":"b","__eventType":"Beta","__commitPosition":2,"__preparePosition":2,"__writeId":"2:2","__dateCreated":"2026-10-09T00:00:01Z","__metadata":"plain","k":1}'),
 ('{"__eventId":"c","__eventType":"10","__commitPosition":5,"__preparePosition":5,"__writeId":"5:5","__dateCreated":"2026-10-10T00:00:00.5Z","__metadata":7,"k":true}'),
 ('{"__eventId":"d","__eventType":"Delta","__commitPosition":10,"__preparePosition":10,"__writeId":"10:10","__dateCreated":"2026-10-11T00:00:00Z","__metadata":true,"k":null}'),
 ('{"__eventId":"e","__eventType":"Echo","__commitPosition":11,"__preparePosition":11,"__writeId":null,"__dateCreated":"2026-10-12T00:00:00Z","__metadata":null}'),
 ('{"__eventId":"f","__eventType":"Foxtrot","__commitPosition":12,"__preparePosition":12,"__writeId":null,"__dateCreated":"2026-10-12T00:00:01Z","__metadata":2.5}');
 PRAGMA user_version=8;`
	open := func(name string) *sqlite.Conn {
		conn := openMigrationTestConn(t, filepath.Join(t.TempDir(), name))
		require.NoError(t, registerTagComparison(conn))
		execScript(t, conn, legacyEventDDL+documents)
		return conn
	}
	legacy, upgraded := open("legacy.db"), open("upgraded.db")
	require.NoError(t, applyMigrations(upgraded))

	keys := []string{"__eventId", "__eventType", "__commitPosition", "__preparePosition", "__writeId", "__dateCreated", "__metadata", "k", "__absent"}
	operators := []string{"eq", "ne", "gt", "gte", "lt", "lte"}
	values := []string{"", "a", "c", "Beta", "10", "2", "2.0", "2.5", "5", "1e1", "2:2", "5:5", "plain", "true", "null", "v",
		`{"tier":"gold","n":1}`, "2026-10-09T00:00:01Z", "2026-10-10", "-1", "9007199254740993", "it's"}
	checked := 0
	for _, key := range keys {
		for _, operator := range operators {
			sqlOperator, err := orisun.TagSQLOperator(operator)
			require.NoError(t, err)
			for _, value := range values {
				literal, err := sqlValueLiteral(value)
				require.NoError(t, err)
				current, err := renderCriterionPredicate(key, []orisun.TagPredicate{{Operator: operator, Value: value}})
				require.NoError(t, err)
				want := queryStrings(t, legacy, "SELECT json_extract(data, '$.__eventId') FROM orisun_es_event WHERE "+legacyPredicate(key, sqlOperator, literal)+" ORDER BY transaction_id, global_id")
				got := queryStrings(t, upgraded, "SELECT event_id FROM orisun_es_event WHERE "+current+" ORDER BY transaction_id, global_id")
				require.Equal(t, strings.Join(want, ","), strings.Join(got, ","), "%s %s %q", key, operator, value)
				checked++
			}
		}
	}
	require.Equal(t, len(keys)*len(operators)*len(values), checked)
}
