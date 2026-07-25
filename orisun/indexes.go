package orisun

import "context"

type BoundaryIndexField struct {
	JsonKey   string
	ValueType string
}

type BoundaryIndexCondition struct {
	Key      string
	Operator string
	Value    string
}

const (
	IndexCombinatorAND = "AND"
	IndexCombinatorOR  = "OR"

	BoundaryIndexStateBuilding = "building"
	BoundaryIndexStateReady    = "ready"
)

type BoundaryIndex struct {
	Name       string
	Fields     []BoundaryIndexField
	Conditions []BoundaryIndexCondition
	Combinator string
	State      string
}

type BoundaryIndexManager interface {
	CreateBoundaryIndex(ctx context.Context, boundary, name string, fields []BoundaryIndexField, conditions []BoundaryIndexCondition, combinator string) error
	DropBoundaryIndex(ctx context.Context, boundary, name string) error
	ListBoundaryIndexes(ctx context.Context, boundary string) ([]BoundaryIndex, error)
	GetBoundaryIndex(ctx context.Context, boundary, name string) (*BoundaryIndex, error)
}
