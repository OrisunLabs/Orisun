package eventstore

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPositionOrdering(t *testing.T) {
	positions := []struct {
		name  string
		left  Position
		right Position
		want  int
	}{
		{
			name:  "earlier commit",
			left:  Position{CommitPosition: 3, PreparePosition: 9},
			right: Position{CommitPosition: 4, PreparePosition: 1},
			want:  -1,
		},
		{
			name:  "earlier prepare in same commit",
			left:  Position{CommitPosition: 4, PreparePosition: 1},
			right: Position{CommitPosition: 4, PreparePosition: 2},
			want:  -1,
		},
		{
			name:  "equal",
			left:  Position{CommitPosition: 4, PreparePosition: 2},
			right: Position{CommitPosition: 4, PreparePosition: 2},
			want:  0,
		},
		{
			name:  "later prepare in same commit",
			left:  Position{CommitPosition: 4, PreparePosition: 3},
			right: Position{CommitPosition: 4, PreparePosition: 2},
			want:  1,
		},
		{
			name:  "later commit",
			left:  Position{CommitPosition: 5, PreparePosition: 0},
			right: Position{CommitPosition: 4, PreparePosition: 99},
			want:  1,
		},
	}

	for _, test := range positions {
		t.Run(test.name, func(t *testing.T) {
			if got := test.left.Compare(test.right); got != test.want {
				t.Fatalf("Compare() = %d, want %d", got, test.want)
			}
			if got := test.left.After(test.right); got != (test.want > 0) {
				t.Fatalf("After() = %t, want %t", got, test.want > 0)
			}
			if got := test.left.Before(test.right); got != (test.want < 0) {
				t.Fatalf("Before() = %t, want %t", got, test.want < 0)
			}
		})
	}
}

func TestQueryJSONUsesStableWireFieldNames(t *testing.T) {
	encoded, err := json.Marshal(Query{Criteria: []Criterion{{
		Tags: []Tag{{Key: "__eventType", Value: "$BoundaryCreated"}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"criteria":[{"tags":[{"key":"__eventType","value":"$BoundaryCreated"}]}]}`
	if string(encoded) != want {
		t.Fatalf("query JSON = %s, want %s", encoded, want)
	}
}

func TestPositionSentinels(t *testing.T) {
	if got := NotExistsPosition(); got != (Position{CommitPosition: -1, PreparePosition: -1}) {
		t.Fatalf("NotExistsPosition() = %#v", got)
	}
	if got := BeginningPosition(); got != (Position{}) {
		t.Fatalf("BeginningPosition() = %#v", got)
	}
	if !NotExistsPosition().Before(BeginningPosition()) {
		t.Fatal("not-exists position must precede the beginning cursor")
	}
}

func TestNeutralRequestAndResultShapes(t *testing.T) {
	from := Position{CommitPosition: 7, PreparePosition: 8}
	query := Query{Criteria: []Criterion{{Tags: []Tag{
		{Key: "__eventType", Value: "OrderPlaced"},
		{Key: "orderId", Value: "o-1"},
	}}}}
	read := ReadRequest{
		Boundary:     "orders",
		FromPosition: &from,
		Count:        100,
		Direction:    DirectionAscending,
		Query:        query,
	}
	if read.FromPosition != &from || len(read.Query.Criteria) != 1 {
		t.Fatalf("unexpected read request: %#v", read)
	}

	created := time.Date(2026, time.July, 23, 10, 30, 0, 0, time.UTC)
	event := ReadEvent{
		EventID:     "event-1",
		EventType:   "OrderPlaced",
		Data:        `{"__eventType":"OrderPlaced","orderId":"o-1"}`,
		Metadata:    `{}`,
		Position:    Position{CommitPosition: 9, PreparePosition: 10},
		DateCreated: created,
	}
	appendResult := AppendResult{Position: event.Position}
	if appendResult.Position != event.Position || !event.DateCreated.Equal(created) {
		t.Fatalf("unexpected neutral result shapes: %#v %#v", event, appendResult)
	}
}
