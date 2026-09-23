// Pure, FoundationDB-free backend logic: criteria matching, index selection,
// position math, encoding and validation. Deliberately carries NO build tag so
// `go test ./...` (without -tags foundationdb and without libfdb_c) compiles and
// unit-tests this logic — the cgo/cluster paths in backend.go still need the
// container suite. Nothing here may import the fdb bindings.

package foundationdb

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
)

const (
	indexStateBuilding = "building"
	indexStateReady    = "ready"
	// perEventOverheadBytes approximates the event key, JSON envelope and a
	// couple of index entries written alongside each event's payload.
	perEventOverheadBytes      = 256
	perIndexEntryOverheadBytes = 128
)

type eventRecord struct {
	Data string `json:"data"`
}

type indexDefinition struct {
	Name       string                              `json:"name"`
	Generation string                              `json:"generation,omitempty"`
	Fields     []eventstore.BoundaryIndexField     `json:"fields"`
	Conditions []eventstore.BoundaryIndexCondition `json:"conditions"`
	Combinator string                              `json:"combinator"`
	State      string                              `json:"state"`
}

type storedUser struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Username       string   `json:"username"`
	HashedPassword string   `json:"password_hash"`
	Roles          []string `json:"roles"`
}

type preparedEvent struct {
	record eventRecord
	data   map[string]any
}

func prepareEvents(events eventstore.PreparedEventBatch, created time.Time) ([]preparedEvent, error) {
	out := make([]preparedEvent, len(events))
	for i, event := range events {
		stored, err := eventdata.WithFields(event.DataJSON, map[string]any{
			"__eventId": event.EventId, "__eventType": event.EventType,
			"__metadata":        eventdata.MetadataValue(event.MetadataJSON),
			"__dateCreated":     created.UTC().Format(time.RFC3339Nano),
			"__writeLastOffset": uint16(len(events) - 1),
		})
		if err != nil {
			return nil, err
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(stored), &data); err != nil {
			return nil, err
		}
		out[i] = preparedEvent{
			record: eventRecord{
				Data: stored,
			},
			data: data,
		}
	}
	return out, nil
}

// estimateSaveBytes approximates the FoundationDB write footprint of a batch so
// an oversized Save is rejected before it reaches the cluster. It includes the
// event record plus every matching index key that Save would write.
func estimateSaveBytes(prepared []preparedEvent, indexes []indexDefinition) int {
	total := 0
	for _, e := range prepared {
		total += len(e.record.Data) + perEventOverheadBytes
		for _, idx := range indexes {
			if !eventMatchesIndexConditions(e.data, idx) {
				continue
			}
			indexBytes, ok := estimateIndexEntryBytes(e.data, idx)
			if ok {
				total += indexBytes
			}
		}
	}
	return total
}

func estimateIndexEntryBytes(data map[string]any, idx indexDefinition) (int, bool) {
	total := len(idx.Name) + len(idx.Generation) + perIndexEntryOverheadBytes
	for _, field := range idx.Fields {
		value, ok := data[field.JsonKey]
		if !ok {
			return 0, false
		}
		total += len(indexValueString(value))
	}
	return total, true
}

func decodeEventRecord(value []byte) (eventRecord, map[string]any, error) {
	var record eventRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return eventRecord{}, nil, err
	}
	data := map[string]any{}
	if err := json.Unmarshal([]byte(record.Data), &data); err != nil {
		return eventRecord{}, nil, err
	}
	return record, data, nil
}

func readEventFromRecord(value []byte, tx, gid int64) (eventstore.ReadEvent, error) {
	var record eventRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return eventstore.ReadEvent{}, err
	}
	var fields struct {
		EventID         string          `json:"__eventId"`
		EventType       string          `json:"__eventType"`
		Metadata        json.RawMessage `json:"__metadata"`
		DateCreated     string          `json:"__dateCreated"`
		WriteLastOffset *uint16         `json:"__writeLastOffset"`
	}
	if err := json.Unmarshal([]byte(record.Data), &fields); err != nil {
		return eventstore.ReadEvent{}, err
	}
	created, err := time.Parse(time.RFC3339Nano, fields.DateCreated)
	if err != nil {
		return eventstore.ReadEvent{}, fmt.Errorf("invalid stored event timestamp: %w", err)
	}
	writeID := ""
	if fields.WriteLastOffset != nil {
		writeID = eventstore.WriteID(tx, (gid & ^int64(65535))|int64(*fields.WriteLastOffset))
	}
	return eventstore.ReadEvent{
		WriteId:         writeID,
		EventId:         fields.EventID,
		EventType:       fields.EventType,
		Data:            eventdata.WithoutStorageEnvelope(record.Data),
		Metadata:        string(fields.Metadata),
		CommitPosition:  tx,
		PreparePosition: gid,
		DateCreated:     created,
	}, nil
}

func encodeUser(user eventstore.User) ([]byte, error) {
	roles := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roles[i] = string(role)
	}
	return json.Marshal(storedUser{
		ID:             user.Id,
		Name:           user.Name,
		Username:       user.Username,
		HashedPassword: user.HashedPassword,
		Roles:          roles,
	})
}

func decodeUser(value []byte) (eventstore.User, error) {
	var stored storedUser
	if err := json.Unmarshal(value, &stored); err != nil {
		return eventstore.User{}, err
	}
	roles := make([]eventstore.Role, len(stored.Roles))
	for i, role := range stored.Roles {
		roles[i] = eventstore.Role(role)
	}
	return eventstore.User{
		Id:             stored.ID,
		Name:           stored.Name,
		Username:       stored.Username,
		HashedPassword: stored.HashedPassword,
		Roles:          roles,
	}, nil
}

func eventMatchesCriterion(data map[string]any, criterion map[string]string) bool {
	for key, expected := range criterion {
		actual, ok := data[key]
		if !ok || !eventValueEquals(actual, expected) {
			return false
		}
	}
	return true
}

func eventMatchesIndexConditions(data map[string]any, idx indexDefinition) bool {
	if len(idx.Conditions) == 0 {
		return true
	}
	matches := 0
	for _, condition := range idx.Conditions {
		value, ok := data[condition.Key]
		if ok && compareCondition(value, condition.Operator, condition.Value) {
			matches++
		}
	}
	if idx.Combinator == eventstore.IndexCombinatorOR {
		return matches > 0
	}
	return matches == len(idx.Conditions)
}

func compareCondition(value any, operator, expected string) bool {
	switch operator {
	case "", "=":
		return eventValueEquals(value, expected)
	case ">", "<", ">=", "<=":
		actualFloat, actualErr := strconv.ParseFloat(indexValueString(value), 64)
		expectedFloat, expectedErr := strconv.ParseFloat(expected, 64)
		if actualErr == nil && expectedErr == nil {
			switch operator {
			case ">":
				return actualFloat > expectedFloat
			case "<":
				return actualFloat < expectedFloat
			case ">=":
				return actualFloat >= expectedFloat
			case "<=":
				return actualFloat <= expectedFloat
			}
		}
		actual := indexValueString(value)
		switch operator {
		case ">":
			return actual > expected
		case "<":
			return actual < expected
		case ">=":
			return actual >= expected
		case "<=":
			return actual <= expected
		}
	default:
		return false
	}
	return false
}

func chooseIndex(indexes []indexDefinition, criterion map[string]string) (indexDefinition, bool) {
	for _, idx := range indexes {
		if len(idx.Fields) == 0 {
			continue
		}
		covered := true
		for _, field := range idx.Fields {
			if _, ok := criterion[field.JsonKey]; !ok {
				covered = false
				break
			}
		}
		if !covered || !criterionImpliesIndexConditions(idx, criterion) {
			continue
		}
		return idx, true
	}
	return indexDefinition{}, false
}

func chooseCoveringIndex(indexes []indexDefinition, criterion map[string]string) (indexDefinition, bool) {
	for _, idx := range indexes {
		if _, ok := chooseIndex([]indexDefinition{idx}, criterion); !ok {
			continue
		}
		if indexCoversCriterion(idx, criterion) {
			return idx, true
		}
	}
	return indexDefinition{}, false
}

func indexCoversCriterion(idx indexDefinition, criterion map[string]string) bool {
	covered := make(map[string]struct{}, len(idx.Fields)+len(idx.Conditions))
	for _, field := range idx.Fields {
		covered[field.JsonKey] = struct{}{}
	}
	for _, condition := range idx.Conditions {
		if condition.Operator == "" || condition.Operator == "=" {
			covered[condition.Key] = struct{}{}
		}
	}
	for key := range criterion {
		if _, ok := covered[key]; !ok {
			return false
		}
	}
	return true
}

func criterionImpliesIndexConditions(idx indexDefinition, criterion map[string]string) bool {
	for _, condition := range idx.Conditions {
		if condition.Operator != "" && condition.Operator != "=" {
			return false
		}
		if criterion[condition.Key] != condition.Value {
			return false
		}
	}
	return true
}

func eventValueEquals(value any, target string) bool {
	switch v := value.(type) {
	case string:
		return v == target
	case bool:
		return strconv.FormatBool(v) == target
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64) == target
	case json.Number:
		return string(v) == target
	case nil:
		return target == "" || target == "null"
	default:
		return fmt.Sprintf("%v", v) == target
	}
}

func indexValueString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case json.Number:
		return string(v)
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func positionMatches(pos, from *eventstore.Position, direction eventstore.Direction) bool {
	if from == nil {
		return true
	}
	cmp := comparePositions(pos, from)
	if direction == eventstore.Direction_DESC {
		return cmp <= 0
	}
	return cmp >= 0
}

func comparePositions(a, b *eventstore.Position) int {
	if a.CommitPosition == b.CommitPosition && a.PreparePosition == b.PreparePosition {
		return 0
	}
	if a.CommitPosition < b.CommitPosition || (a.CommitPosition == b.CommitPosition && a.PreparePosition < b.PreparePosition) {
		return -1
	}
	return 1
}

func sortEvents(events []*eventstore.Event, direction eventstore.Direction) {
	sort.Slice(events, func(i, j int) bool {
		cmp := comparePositions(events[i].Position, events[j].Position)
		if direction == eventstore.Direction_DESC {
			return cmp > 0
		}
		return cmp < 0
	})
}

func validateIdentifier(name string) error {
	if len(name) == 0 || len(name) > 63 {
		return fmt.Errorf("identifier must be 1-63 characters, got %d", len(name))
	}
	if !isAlpha(name[0]) && name[0] != '_' {
		return fmt.Errorf("identifier must start with a letter or underscore, got %q", name[0])
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !isAlpha(c) && !isDigit(c) && c != '_' {
			return fmt.Errorf("identifier may only contain letters, digits, underscores, invalid char %q at %d", c, i)
		}
	}
	return nil
}

func isAlpha(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }
