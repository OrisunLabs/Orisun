package orisun

import (
	"fmt"
	"sort"
	"strings"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
)

// TagPredicate is a comparison against one field. The field name is owned by
// the enclosing criterion. A string field in the durable encoding is eq;
// an array of predicates represents a conjunction on that same field.
type TagPredicate struct {
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

func CanonicalTagOperator(operator string) (string, error) {
	switch operator {
	case "", "eq":
		return "eq", nil
	case "ne", "gt", "gte", "lt", "lte":
		return operator, nil
	default:
		return "", fmt.Errorf("unsupported tag operator %q", operator)
	}
}

func TagSQLOperator(operator string) (string, error) {
	op, err := CanonicalTagOperator(operator)
	if err != nil {
		return "", err
	}
	return map[string]string{"eq": "=", "ne": "<>", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op], nil
}

func ValidateTag(key, value, operator string) error {
	if key == "" {
		return statuscode.New(statuscode.InvalidArgument, "tag key cannot be empty")
	}
	if strings.ContainsRune(key, 0) || strings.ContainsRune(value, 0) {
		return statuscode.New(statuscode.InvalidArgument, "tag key and value cannot contain NUL")
	}
	if _, err := CanonicalTagOperator(operator); err != nil {
		return statuscode.New(statuscode.InvalidArgument, err.Error())
	}
	return nil
}

func ValidateQuery(query *Query) error {
	if query == nil {
		return nil
	}
	for _, criterion := range query.Criteria {
		if criterion == nil {
			return statuscode.New(statuscode.InvalidArgument, "criterion cannot be nil")
		}
		for _, tag := range criterion.Tags {
			if tag == nil {
				return statuscode.New(statuscode.InvalidArgument, "tag cannot be nil")
			}
			if err := ValidateTag(tag.Key, tag.Value, tag.Operator); err != nil {
				return err
			}
		}
	}
	return nil
}

func ValidateReadCriteria(criteria []ReadCriterion) error {
	for _, criterion := range criteria {
		for _, tag := range criterion.Tags {
			if err := ValidateTag(tag.Key, tag.Value, tag.Operator); err != nil {
				return err
			}
		}
	}
	return nil
}

// EncodeCriterion preserves the equality representation already stored in write
// contexts. Non-equality and repeated-key predicates retain every comparison.
func EncodeCriterion(tags []ReadTag) map[string]any {
	fields := make(map[string][]TagPredicate, len(tags))
	for _, tag := range tags {
		op := tag.Operator
		if op == "" {
			op = "eq"
		}
		fields[tag.Key] = append(fields[tag.Key], TagPredicate{Operator: op, Value: tag.Value})
	}
	result := make(map[string]any, len(fields))
	for key, predicates := range fields {
		if len(predicates) == 1 && predicates[0].Operator == "eq" {
			result[key] = predicates[0].Value
		} else {
			result[key] = predicates
		}
	}
	return result
}

func EncodeReadCriteria(criteria []ReadCriterion) []map[string]any {
	result := make([]map[string]any, len(criteria))
	for i, criterion := range criteria {
		result[i] = EncodeCriterion(criterion.Tags)
	}
	return result
}

func EncodeQueryCriteria(query *Query) []map[string]any {
	if query == nil {
		return nil
	}
	criteria := make([]ReadCriterion, len(query.Criteria))
	for i, criterion := range query.Criteria {
		for _, tag := range criterion.Tags {
			criteria[i].Tags = append(criteria[i].Tags, ReadTag{Key: tag.Key, Value: tag.Value, Operator: tag.Operator})
		}
	}
	return EncodeReadCriteria(criteria)
}

func DecodeTagPredicates(value any) ([]TagPredicate, error) {
	if text, ok := value.(string); ok {
		return []TagPredicate{{Operator: "eq", Value: text}}, nil
	}
	if predicates, ok := value.([]TagPredicate); ok {
		return predicates, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var predicates []TagPredicate
	if err := json.Unmarshal(encoded, &predicates); err != nil {
		return nil, fmt.Errorf("invalid tag predicates: %w", err)
	}
	if len(predicates) == 0 {
		return nil, fmt.Errorf("tag predicates cannot be empty")
	}
	return predicates, nil
}

func DecodeCriterion(fields map[string]any) ([]ReadTag, error) {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var tags []ReadTag
	for _, key := range keys {
		predicates, err := DecodeTagPredicates(fields[key])
		if err != nil {
			return nil, err
		}
		for _, predicate := range predicates {
			if err := ValidateTag(key, predicate.Value, predicate.Operator); err != nil {
				return nil, err
			}
			op := predicate.Operator
			if op == "eq" {
				op = ""
			}
			tags = append(tags, ReadTag{Key: key, Value: predicate.Value, Operator: op})
		}
	}
	return tags, nil
}
