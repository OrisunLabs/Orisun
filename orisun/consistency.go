package orisun

import (
	"fmt"
	"sort"
	"strings"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
)

const (
	maxConsistencyObservations = 1024
	maxConsistencyCriteria     = 4096
	maxConsistencyTags         = 16384
)

func consistencyChecksFromObservations(observations []*ConsistencyObservation) ([]ConsistencyCheck, error) {
	if len(observations) > maxConsistencyObservations {
		return nil, statuscode.Errorf(
			statuscode.InvalidArgument,
			"consistency has %d observations, maximum is %d",
			len(observations),
			maxConsistencyObservations,
		)
	}

	checks := make([]ConsistencyCheck, 0, len(observations))
	seen := make(map[string]Position, len(observations))
	totalCriteria := 0
	totalTags := 0
	for index, observation := range observations {
		if observation == nil {
			return nil, statuscode.Errorf(statuscode.InvalidArgument, "consistency observation %d is nil", index)
		}
		if observation.Position == nil {
			return nil, statuscode.Errorf(statuscode.InvalidArgument, "consistency observation %d has no position", index)
		}
		if err := validateConsistencyPosition(*observation.Position); err != nil {
			return nil, statuscode.Errorf(statuscode.InvalidArgument, "consistency observation %d: %v", index, err)
		}
		if observation.Query != nil {
			totalCriteria += len(observation.Query.Criteria)
			if totalCriteria > maxConsistencyCriteria {
				return nil, statuscode.Errorf(statuscode.InvalidArgument, "consistency has more than %d criteria", maxConsistencyCriteria)
			}
			for _, criterion := range observation.Query.Criteria {
				if criterion != nil {
					totalTags += len(criterion.Tags)
					if totalTags > maxConsistencyTags {
						return nil, statuscode.Errorf(statuscode.InvalidArgument, "consistency has more than %d tags", maxConsistencyTags)
					}
				}
			}
		}
		criteria, key, err := normalizeConsistencyQuery(observation.Query)
		if err != nil {
			return nil, statuscode.Errorf(statuscode.InvalidArgument, "consistency observation %d: %v", index, err)
		}
		position := *observation.Position
		if previous, duplicate := seen[key]; duplicate {
			if previous != position {
				return nil, statuscode.Errorf(
					statuscode.InvalidArgument,
					"consistency observation %d contradicts an earlier observation for the same query",
					index,
				)
			}
			continue
		}
		seen[key] = position
		checks = append(checks, ConsistencyCheck{Criteria: criteria, Position: position})
	}
	return checks, nil
}

// LegacyConsistencyChecks translates the deprecated single-query save shape
// into the canonical backend representation.
func LegacyConsistencyChecks(expected *Position, query *Query) ([]ConsistencyCheck, error) {
	return consistencyChecksFromObservations(legacyConsistencyObservations(expected, query))
}

func legacyConsistencyObservations(expected *Position, query *Query) []*ConsistencyObservation {
	if query == nil || len(query.Criteria) == 0 {
		return nil
	}
	position := NotExistsPosition()
	if expected != nil {
		position = *expected
	}
	return []*ConsistencyObservation{{
		Query: query, Position: &position,
	}}
}

func validateConsistencyPosition(position Position) error {
	if position == NotExistsPosition() {
		return nil
	}
	if position.CommitPosition < 0 || position.PreparePosition < 0 {
		return fmt.Errorf("position must be non-negative or exactly (-1, -1)")
	}
	return nil
}

func normalizeConsistencyQuery(query *Query) ([]ReadCriterion, string, error) {
	if query == nil || len(query.Criteria) == 0 {
		return nil, "", fmt.Errorf("query must contain at least one criterion")
	}

	criteriaByKey := make(map[string]ReadCriterion, len(query.Criteria))
	keys := make([]string, 0, len(query.Criteria))
	for criterionIndex, criterion := range query.Criteria {
		if criterion == nil || len(criterion.Tags) == 0 {
			return nil, "", fmt.Errorf("criterion %d has no tags", criterionIndex)
		}
		tagsByKey := make(map[string]string, len(criterion.Tags))
		for tagIndex, tag := range criterion.Tags {
			if tag == nil {
				return nil, "", fmt.Errorf("criterion %d tag %d is nil", criterionIndex, tagIndex)
			}
			if tag.Key == "" {
				return nil, "", fmt.Errorf("criterion %d tag %d has no key", criterionIndex, tagIndex)
			}
			if previous, duplicate := tagsByKey[tag.Key]; duplicate && previous != tag.Value {
				return nil, "", fmt.Errorf("criterion %d repeats key %q with a different value", criterionIndex, tag.Key)
			}
			tagsByKey[tag.Key] = tag.Value
		}

		tagKeys := make([]string, 0, len(tagsByKey))
		for key := range tagsByKey {
			tagKeys = append(tagKeys, key)
		}
		sort.Strings(tagKeys)
		criterionKey := strings.Builder{}
		normalized := ReadCriterion{Tags: make([]ReadTag, 0, len(tagKeys))}
		for _, key := range tagKeys {
			value := tagsByKey[key]
			fmt.Fprintf(&criterionKey, "%d:%s%d:%s", len(key), key, len(value), value)
			normalized.Tags = append(normalized.Tags, ReadTag{Key: key, Value: value})
		}
		key := criterionKey.String()
		if _, duplicate := criteriaByKey[key]; duplicate {
			continue
		}
		criteriaByKey[key] = normalized
		keys = append(keys, key)
	}

	sort.Strings(keys)
	criteria := make([]ReadCriterion, len(keys))
	queryKey := strings.Builder{}
	for index, key := range keys {
		criteria[index] = criteriaByKey[key]
		fmt.Fprintf(&queryKey, "%d:%s", len(key), key)
	}
	return criteria, queryKey.String(), nil
}
