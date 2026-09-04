package orisun

import (
	"context"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
)

func TestSaveEndpointsAuthorizeBeforeValidatingRequests(t *testing.T) {
	logger, err := logging.ZapLogger("error")
	if err != nil {
		t.Fatal(err)
	}
	store := &EventStore{logger: logger}
	ctx := context.WithValue(t.Context(), UserContextKey, User{Roles: nil})

	if _, err = store.SaveEvents(ctx, nil); statuscode.CodeOf(err) != statuscode.PermissionDenied {
		t.Fatalf("SaveEvents() error = %v, want permission denied", err)
	}
	if _, err := store.SaveEventsV2(ctx, nil); statuscode.CodeOf(err) != statuscode.PermissionDenied {
		t.Fatalf("SaveEventsV2() error = %v, want permission denied", err)
	}
}

func TestConsistencyChecksKeepOnePositionPerCompleteQuery(t *testing.T) {
	position := &Position{CommitPosition: 12, PreparePosition: 9}
	query := &Query{Criteria: []*Criterion{
		{Tags: []*Tag{{Key: "product_id", Value: "p-1"}, {Key: "eventType", Value: "StockAdjusted"}}},
		{Tags: []*Tag{{Key: "eventType", Value: "StockCounted"}, {Key: "product_id", Value: "p-1"}}},
	}}

	checks, err := consistencyChecksFromObservations([]*ConsistencyObservation{{
		Query: query, Position: position,
	}})
	if err != nil {
		t.Fatalf("consistencyChecksFromObservations() error = %v", err)
	}
	if len(checks) != 1 || len(checks[0].Criteria) != 2 || checks[0].Position != *position {
		t.Fatalf("checks = %#v", checks)
	}
	if checks[0].Criteria[0].Tags[0].Key != "eventType" || checks[0].Criteria[1].Tags[0].Key != "eventType" {
		t.Fatalf("criteria were not normalized: %#v", checks[0].Criteria)
	}

	query.Criteria[0].Tags[0].Value = "mutated"
	position.CommitPosition = 99
	if checks[0].Criteria[1].Tags[1].Value != "p-1" || checks[0].Position.CommitPosition != 12 {
		t.Fatalf("normalized check aliases request memory: %#v", checks[0])
	}
}

func TestConsistencyChecksDeduplicateEquivalentQueriesAndRejectContradictions(t *testing.T) {
	first := &Query{Criteria: []*Criterion{
		{Tags: []*Tag{{Key: "kind", Value: "credit"}, {Key: "account", Value: "a-1"}}},
		{Tags: []*Tag{{Key: "account", Value: "a-2"}}},
	}}
	second := &Query{Criteria: []*Criterion{
		{Tags: []*Tag{{Key: "account", Value: "a-2"}}},
		{Tags: []*Tag{{Key: "account", Value: "a-1"}, {Key: "kind", Value: "credit"}}},
	}}
	position := &Position{CommitPosition: 4, PreparePosition: 3}

	checks, err := consistencyChecksFromObservations([]*ConsistencyObservation{
		{Query: first, Position: position},
		{Query: second, Position: &Position{CommitPosition: 4, PreparePosition: 3}},
	})
	if err != nil || len(checks) != 1 {
		t.Fatalf("equivalent observations = %#v, %v", checks, err)
	}

	_, err = consistencyChecksFromObservations([]*ConsistencyObservation{
		{Query: first, Position: position},
		{Query: second, Position: &Position{CommitPosition: 5, PreparePosition: 4}},
	})
	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("contradictory observations error = %v", err)
	}
}

func TestConsistencyChecksRejectInvalidObservations(t *testing.T) {
	validQuery := &Query{Criteria: []*Criterion{{Tags: []*Tag{{Key: "id", Value: "1"}}}}}
	tests := []struct {
		name        string
		observation *ConsistencyObservation
	}{
		{name: "nil observation"},
		{name: "missing query", observation: &ConsistencyObservation{Position: &Position{}}},
		{name: "empty query", observation: &ConsistencyObservation{Query: &Query{}, Position: &Position{}}},
		{name: "empty criterion", observation: &ConsistencyObservation{Query: &Query{Criteria: []*Criterion{{}}}, Position: &Position{}}},
		{name: "nil tag", observation: &ConsistencyObservation{Query: &Query{Criteria: []*Criterion{{Tags: []*Tag{nil}}}}, Position: &Position{}}},
		{name: "empty key", observation: &ConsistencyObservation{Query: &Query{Criteria: []*Criterion{{Tags: []*Tag{{}}}}}, Position: &Position{}}},
		{name: "missing position", observation: &ConsistencyObservation{Query: validQuery}},
		{name: "partial negative", observation: &ConsistencyObservation{Query: validQuery, Position: &Position{CommitPosition: -1, PreparePosition: 0}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := consistencyChecksFromObservations([]*ConsistencyObservation{test.observation})
			if statuscode.CodeOf(err) != statuscode.InvalidArgument {
				t.Fatalf("error = %v", err)
			}
		})
	}

	tooMany := make([]*ConsistencyObservation, maxConsistencyObservations+1)
	_, err := consistencyChecksFromObservations(tooMany)
	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("excessive observations error = %v", err)
	}

	criteria := make([]*Criterion, maxConsistencyCriteria+1)
	for i := range criteria {
		criteria[i] = &Criterion{Tags: []*Tag{{Key: "id", Value: "1"}}}
	}
	_, err = consistencyChecksFromObservations([]*ConsistencyObservation{{
		Query: &Query{Criteria: criteria}, Position: &Position{},
	}})
	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("excessive criteria error = %v", err)
	}

	tags := make([]*Tag, maxConsistencyTags+1)
	for i := range tags {
		tags[i] = &Tag{Key: "id", Value: "1"}
	}
	_, err = consistencyChecksFromObservations([]*ConsistencyObservation{{
		Query: &Query{Criteria: []*Criterion{{Tags: tags}}}, Position: &Position{},
	}})
	if statuscode.CodeOf(err) != statuscode.InvalidArgument {
		t.Fatalf("excessive tags error = %v", err)
	}
}

func TestLegacyConsistencyChecksProduceOneQueryObservation(t *testing.T) {
	query := &Query{Criteria: []*Criterion{{Tags: []*Tag{{Key: "order_id", Value: "o-1"}}}}}
	checks, err := LegacyConsistencyChecks(nil, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) != 1 || checks[0].Position != NotExistsPosition() {
		t.Fatalf("checks = %#v", checks)
	}
	checks, err = LegacyConsistencyChecks(&Position{CommitPosition: 7, PreparePosition: 6}, query)
	if err != nil || checks[0].Position != (Position{CommitPosition: 7, PreparePosition: 6}) {
		t.Fatalf("checks = %#v, %v", checks, err)
	}
	checks, err = LegacyConsistencyChecks(&Position{}, nil)
	if err != nil || len(checks) != 0 {
		t.Fatalf("unscoped legacy check = %#v, %v", checks, err)
	}
}
