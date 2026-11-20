package postgres

import (
	"errors"
	"fmt"

	"github.com/OrisunLabs/Orisun/internal/eventdata"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/google/uuid"
)

// preparedPostgresSave owns its serialized input and compiled CCC checks. No
// caller-owned slices or criteria survive preparation, and the writer never
// validates, encodes, or compiles an individual request. Positions and CCC
// decisions remain database-owned.
type preparedPostgresSave struct {
	payload    postgresBatchPayload
	checks     []compiledCheck
	eventCount int
}

func (s *PostgresSaveEvents) prepareSave(events eventstore.PreparedEventBatch, consistency []eventstore.ConsistencyCheck) (preparedPostgresSave, error) {
	if err := validatePostgresSaveInput(events, consistency); err != nil {
		return preparedPostgresSave{}, s.mapSaveError(err)
	}
	checks, err := eventstore.MarshalConsistency(consistency)
	if err != nil {
		return preparedPostgresSave{}, statuscode.Errorf(statuscode.Internal, "failed to marshal consistency condition: %v", err)
	}
	encoded, err := events.MarshalJSON()
	if err != nil {
		return preparedPostgresSave{}, statuscode.Errorf(statuscode.Internal, "failed to marshal events: %v", err)
	}
	compiled, err := compileConsistencyChecks(consistency)
	if err != nil {
		return preparedPostgresSave{}, statuscode.Errorf(statuscode.InvalidArgument, "invalid consistency query: %v", err)
	}
	return preparedPostgresSave{payload: postgresBatchPayload{Consistency: checks, Events: encoded}, checks: compiled, eventCount: len(events)}, nil
}

// Validate backend-facing requests before sharing a transaction. A malformed
// request must never abort valid neighbors or advance their criterion state.
func validatePostgresSaveInput(events eventstore.PreparedEventBatch, consistency []eventstore.ConsistencyCheck) error {
	if len(events) == 0 {
		return errors.New("events cannot be empty")
	}
	for _, event := range events {
		if event.EventType == "" {
			return errors.New("event_type cannot be empty")
		}
		if _, err := uuid.Parse(event.EventId); err != nil {
			return fmt.Errorf("invalid event_id: %w", err)
		}
		// Storage owns top-level __* keys; data persists application fields only.
		if err := eventdata.ValidateObject(event.DataJSON); err != nil {
			return err
		}
	}
	for _, check := range consistency {
		if err := eventstore.ValidateReadCriteria(check.Criteria); err != nil {
			return err
		}
		if len(check.Criteria) == 0 {
			return errors.New("consistency query has no criteria")
		}
		for _, criterion := range check.Criteria {
			if len(criterion.Tags) == 0 {
				return errors.New("consistency criterion has no tags")
			}
		}
	}
	return nil
}
