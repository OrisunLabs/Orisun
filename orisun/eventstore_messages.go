package orisun

import "time"

// Position identifies one event in a boundary's total order.
type Position struct {
	CommitPosition  int64 `json:"commit_position"`
	PreparePosition int64 `json:"prepare_position"`
}

type Tag struct {
	Key   string
	Value string
}

type Criterion struct {
	Tags []*Tag
}

type Query struct {
	Criteria []*Criterion
}

type EventToSave struct {
	EventId   string
	EventType string
	Data      string
	Metadata  string
}

// Event is the transport-neutral event shape used by legacy in-process
// command handlers. Storage backends use ReadEvent directly.
type Event struct {
	EventId     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	Data        string    `json:"data"`
	Metadata    string    `json:"metadata"`
	Position    *Position `json:"position"`
	DateCreated time.Time `json:"date_created"`
}

type WriteResult struct {
	LogPosition *Position
}

type SaveQuery struct {
	ExpectedPosition *Position
	SubsetQuery      *Query
}

type SaveEventsRequest struct {
	Boundary string
	Query    *SaveQuery
	Events   []*EventToSave
}

type Direction int32

const (
	Direction_ASC Direction = iota
	Direction_DESC
)

func (d Direction) String() string {
	if d == Direction_DESC {
		return "DESC"
	}
	return "ASC"
}

type GetEventsRequest struct {
	Query        *Query
	FromPosition *Position
	Count        uint32
	Direction    Direction
	Boundary     string
}

type GetEventsResponse struct {
	Events []*Event
}

type GetLatestByCriteriaRequest struct {
	Boundary string
	Criteria []*Criterion
}

type LatestCriterionResult struct {
	Criterion *Criterion
	Event     *Event
}

type GetLatestByCriteriaResponse struct {
	Results         []*LatestCriterionResult
	ContextPosition *Position
}

type ValueType int32

const (
	ValueType_TEXT ValueType = iota
	ValueType_NUMERIC
	ValueType_BOOLEAN
	ValueType_TIMESTAMPTZ
)

func (v ValueType) String() string {
	switch v {
	case ValueType_NUMERIC:
		return "NUMERIC"
	case ValueType_BOOLEAN:
		return "BOOLEAN"
	case ValueType_TIMESTAMPTZ:
		return "TIMESTAMPTZ"
	default:
		return "TEXT"
	}
}

type ConditionCombinator int32

const (
	ConditionCombinator_AND ConditionCombinator = iota
	ConditionCombinator_OR
)

type IndexField struct {
	JsonKey   string
	ValueType ValueType
}

type IndexCondition struct {
	Key      string
	Operator string
	Value    string
}

type CreateIndexRequest struct {
	Boundary            string
	Name                string
	Fields              []*IndexField
	Conditions          []*IndexCondition
	ConditionCombinator ConditionCombinator
}

type DropIndexRequest struct {
	Boundary string
	Name     string
}

type ListIndexesRequest struct {
	Boundary string
}

type ListIndexesResponse struct {
	Indexes []*BoundaryIndex
}

type GetIndexRequest struct {
	Boundary string
	Name     string
}

type GetIndexResponse struct {
	Index *BoundaryIndex
}
