// Unit tests for the FoundationDB-free backend logic. No build tag and no
// libfdb_c needed, so these run under plain `go test ./...` and catch
// matching / index-selection / position / encoding regressions without a
// cluster — the gap the container-only suite used to leave open.

package foundationdb

import (
	"testing"

	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
)

func textField(key string) eventstore.BoundaryIndexField {
	return eventstore.BoundaryIndexField{JsonKey: key, ValueType: "text"}
}

func readyIndex(name string, fields ...eventstore.BoundaryIndexField) indexDefinition {
	return indexDefinition{Name: name, Fields: fields, State: indexStateReady, Combinator: eventstore.IndexCombinatorAND}
}

func TestEstimateSaveBytes(t *testing.T) {
	prepared := []preparedEvent{
		{record: eventRecord{EventID: "id", EventType: "T", Data: "1234567890", Metadata: "ab"}},
		{record: eventRecord{EventID: "id2", EventType: "T2", Data: "xyz", Metadata: ""}},
	}
	got := estimateSaveBytes(prepared, []indexDefinition{readyIndex("by_type", textField("type"))})
	want := (10 + 2 + 2 + 1 + perEventOverheadBytes) + (3 + 0 + 3 + 2 + perEventOverheadBytes)
	if got != want {
		t.Fatalf("estimateSaveBytes = %d, want %d", got, want)
	}
}

func TestEstimateSaveBytesIncludesMatchingIndexes(t *testing.T) {
	prepared := []preparedEvent{
		{
			record: eventRecord{EventID: "id", EventType: "T", Data: "{}", Metadata: "{}"},
			data:   map[string]any{"type": "Created", "status": "open"},
		},
	}
	idx := indexDefinition{
		Name:   "by_type",
		Fields: []eventstore.BoundaryIndexField{textField("type")},
		Conditions: []eventstore.BoundaryIndexCondition{
			{Key: "status", Operator: "=", Value: "open"},
		},
		Combinator: eventstore.IndexCombinatorAND,
		State:      indexStateReady,
	}
	got := estimateSaveBytes(prepared, []indexDefinition{idx})
	base := len("{}") + len("{}") + len("id") + len("T") + perEventOverheadBytes
	want := base + len("by_type") + len("Created") + perIndexEntryOverheadBytes
	if got != want {
		t.Fatalf("estimateSaveBytes = %d, want %d", got, want)
	}
}

func TestEventValueEquals(t *testing.T) {
	cases := []struct {
		value any
		want  string
		ok    bool
	}{
		{"abc", "abc", true},
		{float64(1), "1", true},
		{float64(1.5), "1.5", true},
		{true, "true", true},
		{nil, "null", true},
		{nil, "", true},
		{"abc", "abcd", false},
		{float64(2), "3", false},
	}
	for _, c := range cases {
		if got := eventValueEquals(c.value, c.want); got != c.ok {
			t.Errorf("eventValueEquals(%v, %q) = %v, want %v", c.value, c.want, got, c.ok)
		}
	}
}

func TestCompareCondition(t *testing.T) {
	cases := []struct {
		value    any
		op       string
		expected string
		want     bool
	}{
		{"x", "=", "x", true},
		{"x", "", "x", true},
		{float64(5), ">", "3", true},
		{float64(5), "<", "3", false},
		{float64(3), ">=", "3", true},
		{float64(2), "<=", "3", true},
		{"b", ">", "a", true}, // string fallback when not numeric
		{float64(5), "??", "3", false},
	}
	for _, c := range cases {
		if got := compareCondition(c.value, c.op, c.expected); got != c.want {
			t.Errorf("compareCondition(%v, %q, %q) = %v, want %v", c.value, c.op, c.expected, got, c.want)
		}
	}
}

func TestEventMatchesIndexConditionsCombinators(t *testing.T) {
	data := map[string]any{"a": "1", "b": "2"}

	and := indexDefinition{Conditions: []eventstore.BoundaryIndexCondition{
		{Key: "a", Operator: "=", Value: "1"},
		{Key: "b", Operator: "=", Value: "2"},
	}, Combinator: eventstore.IndexCombinatorAND}
	if !eventMatchesIndexConditions(data, and) {
		t.Fatal("AND conditions should match")
	}
	and.Conditions[1].Value = "9"
	if eventMatchesIndexConditions(data, and) {
		t.Fatal("AND with one false condition should not match")
	}

	or := indexDefinition{Conditions: []eventstore.BoundaryIndexCondition{
		{Key: "a", Operator: "=", Value: "1"},
		{Key: "b", Operator: "=", Value: "9"},
	}, Combinator: eventstore.IndexCombinatorOR}
	if !eventMatchesIndexConditions(data, or) {
		t.Fatal("OR with one true condition should match")
	}
}

// TestChooseCoveringIndexFullCoverage locks in the invariant the #6 fast path
// relies on: chooseCoveringIndex only returns an index whose fields+equality
// conditions cover EVERY criterion key. If it ever returns a partial index the
// dropped post-scan re-check would let non-matching events through.
func TestChooseCoveringIndexFullCoverage(t *testing.T) {
	indexes := []indexDefinition{
		readyIndex("by_et", textField("eventType")),
		readyIndex("by_et_user", textField("eventType"), textField("user_id")),
	}

	// Criterion fully covered by the two-field index.
	crit := map[string]string{"eventType": "Created", "user_id": "u1"}
	idx, ok := chooseCoveringIndex(indexes, crit)
	if !ok || idx.Name != "by_et_user" {
		t.Fatalf("expected by_et_user, got ok=%v name=%q", ok, idx.Name)
	}

	// Criterion references a key no index covers → no covering index.
	crit2 := map[string]string{"eventType": "Created", "missing": "x"}
	if _, ok := chooseCoveringIndex(indexes, crit2); ok {
		t.Fatal("expected no covering index for an uncovered key")
	}

	// A single-field index must not be chosen for a two-key criterion.
	only := []indexDefinition{readyIndex("by_et", textField("eventType"))}
	if _, ok := chooseCoveringIndex(only, crit); ok {
		t.Fatal("single-field index must not cover a two-key criterion")
	}
}

func TestIndexCoversCriterionWithEqualityConditions(t *testing.T) {
	idx := indexDefinition{
		Fields: []eventstore.BoundaryIndexField{textField("eventType")},
		Conditions: []eventstore.BoundaryIndexCondition{
			{Key: "status", Operator: "=", Value: "open"},
		},
	}
	if !indexCoversCriterion(idx, map[string]string{"eventType": "X", "status": "open"}) {
		t.Fatal("equality condition key should count toward coverage")
	}
	// A range condition key does not contribute coverage.
	idx.Conditions[0].Operator = ">"
	if indexCoversCriterion(idx, map[string]string{"eventType": "X", "status": "open"}) {
		t.Fatal("range condition key must not count as covered")
	}
}

func TestComparePositionsAndSort(t *testing.T) {
	a := &eventstore.Position{CommitPosition: 1, PreparePosition: 0}
	b := &eventstore.Position{CommitPosition: 1, PreparePosition: 5}
	c := &eventstore.Position{CommitPosition: 2, PreparePosition: 0}

	if comparePositions(a, b) != -1 || comparePositions(c, a) != 1 || comparePositions(a, a) != 0 {
		t.Fatal("comparePositions ordering wrong")
	}

	events := []*eventstore.Event{{Position: c}, {Position: a}, {Position: b}}
	sortEvents(events, eventstore.Direction_ASC)
	if events[0].Position != a || events[2].Position != c {
		t.Fatal("ASC sort wrong")
	}
	sortEvents(events, eventstore.Direction_DESC)
	if events[0].Position != c || events[2].Position != a {
		t.Fatal("DESC sort wrong")
	}
}

func TestPositionMatches(t *testing.T) {
	from := &eventstore.Position{CommitPosition: 5, PreparePosition: 0}
	below := &eventstore.Position{CommitPosition: 4, PreparePosition: 0}
	above := &eventstore.Position{CommitPosition: 6, PreparePosition: 0}

	if !positionMatches(above, from, eventstore.Direction_ASC) || positionMatches(below, from, eventstore.Direction_ASC) {
		t.Fatal("ASC cursor inclusive-from semantics wrong")
	}
	if !positionMatches(below, from, eventstore.Direction_DESC) || positionMatches(above, from, eventstore.Direction_DESC) {
		t.Fatal("DESC cursor inclusive-from semantics wrong")
	}
	if !positionMatches(from, from, eventstore.Direction_ASC) {
		t.Fatal("cursor must be inclusive at the boundary")
	}
}

func TestValidateIdentifier(t *testing.T) {
	good := []string{"a", "_x", "idx_1", "EventType"}
	bad := []string{"", "1abc", "has space", "dash-name", string(make([]byte, 64))}
	for _, s := range good {
		if err := validateIdentifier(s); err != nil {
			t.Errorf("validateIdentifier(%q) unexpected error: %v", s, err)
		}
	}
	for _, s := range bad {
		if err := validateIdentifier(s); err == nil {
			t.Errorf("validateIdentifier(%q) expected error", s)
		}
	}
}

func TestUserEncodeDecodeRoundTrip(t *testing.T) {
	user := eventstore.User{
		Id: "u1", Name: "Ada", Username: "ada", HashedPassword: "h",
		Roles: []eventstore.Role{eventstore.Role("admin")},
	}
	raw, err := encodeUser(user)
	if err != nil {
		t.Fatalf("encodeUser: %v", err)
	}
	got, err := decodeUser(raw)
	if err != nil {
		t.Fatalf("decodeUser: %v", err)
	}
	if got.Id != user.Id || got.Username != user.Username || len(got.Roles) != 1 || got.Roles[0] != user.Roles[0] {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}
}

func TestPrepareEventsAndRoundTrip(t *testing.T) {
	batch, err := eventstore.PrepareEventsForSave([]eventstore.EventWithMapTags{{
		EventId:   "e1",
		EventType: "Created",
		Data:      map[string]any{"user_id": "u1"},
		Metadata:  map[string]any{"src": "test"},
	}})
	if err != nil {
		t.Fatalf("PrepareEventsForSave: %v", err)
	}
	prepared, err := prepareEvents(batch)
	if err != nil {
		t.Fatalf("prepareEvents: %v", err)
	}
	if prepared[0].data["user_id"] != "u1" {
		t.Fatalf("data map not parsed: %+v", prepared[0].data)
	}

	prepared[0].record.DateCreated = "2026-06-30T00:00:00Z"
	value, err := json.Marshal(prepared[0].record)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	event, err := readEventFromRecord(value, 7, 3)
	if err != nil {
		t.Fatalf("readEventFromRecord: %v", err)
	}
	if event.EventId != "e1" || event.CommitPosition != 7 || event.PreparePosition != 3 {
		t.Fatalf("event roundtrip mismatch: %+v", event)
	}
}

func TestEventMatchesCriterion(t *testing.T) {
	data := `{"eventType":"Created","amount":5}`
	if !eventMatchesCriterion(data, map[string]string{"eventType": "Created", "amount": "5"}) {
		t.Fatal("expected criterion match")
	}
	if eventMatchesCriterion(data, map[string]string{"eventType": "Other"}) {
		t.Fatal("expected criterion mismatch")
	}
}
