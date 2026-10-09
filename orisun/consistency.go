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

// ConsistencyChecksFromObservations validates query observations for native storage.
func ConsistencyChecksFromObservations(observations []*ConsistencyObservation) ([]ConsistencyCheck, error) {
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
		predicates := make(map[string]ReadTag, len(criterion.Tags))
		equalities := make(map[string]string)
		for tagIndex, tag := range criterion.Tags {
			if tag == nil {
				return nil, "", fmt.Errorf("criterion %d tag %d is nil", criterionIndex, tagIndex)
			}
			if err := ValidateTag(tag.Key, tag.Value, tag.Operator); err != nil {
				return nil, "", err
			}
			operator, _ := CanonicalTagOperator(tag.Operator)
			if operator == "eq" {
				if previous, exists := equalities[tag.Key]; exists && previous != tag.Value {
					return nil, "", fmt.Errorf("criterion %d repeats key %q with a different value", criterionIndex, tag.Key)
				}
				equalities[tag.Key] = tag.Value
				operator = ""
			}
			identity := fmt.Sprintf("%d:%s%d:%s%d:%s", len(tag.Key), tag.Key, len(operator), operator, len(tag.Value), tag.Value)
			predicates[identity] = ReadTag{Key: tag.Key, Value: tag.Value, Operator: operator}
		}
		normalized := ReadCriterion{Tags: make([]ReadTag, 0, len(predicates))}
		for _, tag := range predicates {
			normalized.Tags = append(normalized.Tags, tag)
		}
		sort.Slice(normalized.Tags, func(i, j int) bool {
			a, b := normalized.Tags[i], normalized.Tags[j]
			if a.Key != b.Key {
				return a.Key < b.Key
			}
			if a.Operator != b.Operator {
				return a.Operator < b.Operator
			}
			return a.Value < b.Value
		})
		criterionKey := strings.Builder{}
		for _, tag := range normalized.Tags {
			fmt.Fprintf(&criterionKey, "%d:%s%d:%s%d:%s", len(tag.Key), tag.Key, len(tag.Operator), tag.Operator, len(tag.Value), tag.Value)
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
