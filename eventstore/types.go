package eventstore

import (
	"context"
	"time"
)

// Position identifies one event in the total order of a boundary.
type Position struct {
	CommitPosition  int64 `json:"commit_position"`
	PreparePosition int64 `json:"prepare_position"`
}

// NotExistsPosition is the optimistic-concurrency position used when no
// matching event exists.
func NotExistsPosition() Position {
	return Position{CommitPosition: -1, PreparePosition: -1}
}

// BeginningPosition is the public cursor for reading from the start of a
// boundary.
func BeginningPosition() Position {
	return Position{}
}

// Compare returns -1 when p is before other, 0 when they are equal, and 1 when
// p is after other.
func (p Position) Compare(other Position) int {
	if p.CommitPosition < other.CommitPosition ||
		p.CommitPosition == other.CommitPosition && p.PreparePosition < other.PreparePosition {
		return -1
	}
	if p == other {
		return 0
	}
	return 1
}

// After reports whether p is strictly after other.
func (p Position) After(other Position) bool {
	return p.Compare(other) > 0
}

// Before reports whether p is strictly before other.
func (p Position) Before(other Position) bool {
	return p.Compare(other) < 0
}

// Tag is one equality predicate over canonical event data.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Criterion is a conjunction of tags. A Query matches when any one of its
// criteria matches.
type Criterion struct {
	Tags []Tag `json:"tags"`
}

// Query selects events by canonical data content.
type Query struct {
	Criteria []Criterion `json:"criteria"`
}

// Direction controls event read order.
type Direction uint8

const (
	DirectionAscending Direction = iota
	DirectionDescending
)

// ReadRequest describes one bounded page read.
//
// A nil FromPosition starts at the backend's default cursor. Query with no
// criteria selects all events.
type ReadRequest struct {
	Boundary     string    `json:"boundary"`
	FromPosition *Position `json:"from_position,omitempty"`
	Count        uint32    `json:"count"`
	Direction    Direction `json:"direction"`
	Query        Query     `json:"query"`
}

// ReadEvent is one committed event in a boundary.
type ReadEvent struct {
	WriteID     string    `json:"write_id,omitempty"`
	EventID     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	Data        string    `json:"data"`
	Metadata    string    `json:"metadata"`
	Position    Position  `json:"position"`
	DateCreated time.Time `json:"date_created"`
}

// ReadEventBatch is one contiguous page of committed events.
type ReadEventBatch []ReadEvent

// LatestByCriteriaRequest asks for the latest match of each criterion from one
// consistent read snapshot.
type LatestByCriteriaRequest struct {
	Boundary string      `json:"boundary"`
	Criteria []Criterion `json:"criteria"`
}

// LatestCriterionMatch is positionally aligned with its request criterion.
// Event is meaningful only when Found is true.
type LatestCriterionMatch struct {
	Event ReadEvent `json:"event"`
	Found bool      `json:"found"`
}

// LatestByCriteriaResult contains the latest match for every request criterion
// and the combined context position for a subsequent append.
type LatestByCriteriaResult struct {
	Matches         []LatestCriterionMatch `json:"matches"`
	ContextPosition Position               `json:"context_position"`
}

// EventToAppend is one canonical event ready to append. Data and Metadata are
// JSON values encoded as strings.
type EventToAppend struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	Data      string `json:"data"`
	Metadata  string `json:"metadata"`
}

// AppendRequest describes one atomic append and its consistency condition.
type AppendRequest struct {
	Boundary         string          `json:"boundary"`
	Events           []EventToAppend `json:"events"`
	ExpectedPosition *Position       `json:"expected_position,omitempty"`
	Subset           Query           `json:"subset"`
}

// AppendResult identifies the final position committed by an append.
type AppendResult struct {
	Position Position `json:"position"`
}

// SubscribeRequest describes a durable catch-up subscription.
type SubscribeRequest struct {
	Boundary       string    `json:"boundary"`
	SubscriberName string    `json:"subscriber_name"`
	AfterPosition  *Position `json:"after_position,omitempty"`
	Query          Query     `json:"query"`
}

// EventHandler receives one event at a time in subscription order. Returning
// an error stops the subscription.
type EventHandler = func(context.Context, ReadEvent) error
