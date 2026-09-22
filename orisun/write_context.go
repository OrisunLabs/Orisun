package orisun

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
)

// WriteContext is store-owned evidence of the observations checked for one save.
// WriteId is boundary-scoped and identifies the complete final position in that save.
// An existing record with no observations means an unconditional append. Events
// predating context recording have no write ID and no context record.
type WriteContext struct {
	WriteId     string
	Consistency []*ConsistencyObservation
}

type GetWriteContextRequest struct {
	Boundary string
	WriteId  string
}

// WriteContextRetriever is implemented by backends that retain write provenance.
type WriteContextRetriever interface {
	GetWriteContext(context.Context, *GetWriteContextRequest) (*WriteContext, error)
}

// storedConsistencyObservation is the durable query representation shared by
// storage engines. Keep each complete OR query paired with its observed position.
type storedConsistencyObservation struct {
	Query struct {
		Criteria []map[string]string `json:"criteria"`
	} `json:"query"`
	Position struct {
		TransactionID int64 `json:"transaction_id"`
		GlobalID      int64 `json:"global_id"`
	} `json:"position"`
}

func MarshalConsistency(checks []ConsistencyCheck) ([]byte, error) {
	observations := make([]storedConsistencyObservation, len(checks))
	for i, check := range checks {
		o := &observations[i]
		o.Query.Criteria = make([]map[string]string, len(check.Criteria))
		for j, criterion := range check.Criteria {
			tags := make(map[string]string, len(criterion.Tags))
			for _, tag := range criterion.Tags {
				tags[tag.Key] = tag.Value
			}
			o.Query.Criteria[j] = tags
		}
		o.Position.TransactionID = check.Position.CommitPosition
		o.Position.GlobalID = check.Position.PreparePosition
	}
	return json.Marshal(observations)
}

func DecodeWriteContext(writeID string, data []byte) (*WriteContext, error) {
	var stored []storedConsistencyObservation
	if err := json.Unmarshal(data, &stored); err != nil {
		return nil, fmt.Errorf("decode write context: %w", err)
	}
	result := &WriteContext{WriteId: writeID, Consistency: make([]*ConsistencyObservation, len(stored))}
	for i, o := range stored {
		query := &Query{Criteria: make([]*Criterion, len(o.Query.Criteria))}
		for j, tags := range o.Query.Criteria {
			keys := make([]string, 0, len(tags))
			for key := range tags {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			criterion := &Criterion{Tags: make([]*Tag, 0, len(keys))}
			for _, key := range keys {
				criterion.Tags = append(criterion.Tags, &Tag{Key: key, Value: tags[key]})
			}
			query.Criteria[j] = criterion
		}
		result.Consistency[i] = &ConsistencyObservation{Query: query, Position: &Position{CommitPosition: o.Position.TransactionID, PreparePosition: o.Position.GlobalID}}
	}
	return result, nil
}

// WriteID identifies a save by its complete final position. Treat it as opaque.
func WriteID(commit, prepare int64) string {
	return strconv.FormatInt(commit, 10) + ":" + strconv.FormatInt(prepare, 10)
}

func ValidateWriteContextRequest(req *GetWriteContextRequest) (Position, error) {
	invalid := statuscode.New(statuscode.InvalidArgument, "boundary and a valid write_id are required")
	if req == nil || req.Boundary == "" {
		return Position{}, invalid
	}
	parts := strings.Split(req.WriteId, ":")
	if len(parts) != 2 {
		return Position{}, invalid
	}
	commit, e1 := strconv.ParseInt(parts[0], 10, 64)
	prepare, e2 := strconv.ParseInt(parts[1], 10, 64)
	if e1 != nil || e2 != nil || commit < 0 || prepare < 0 || WriteID(commit, prepare) != req.WriteId {
		return Position{}, invalid
	}
	return Position{CommitPosition: commit, PreparePosition: prepare}, nil
}
