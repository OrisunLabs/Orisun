//go:build foundationdb

package foundationdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// These helpers support the FoundationDB-backed implementation and therefore
// belong behind the same build tag as backend.go and lock.go.

func criteriaAsMaps(query *eventstore.Query) []map[string]any {
	return nonemptyCriteria(eventstore.EncodeQueryCriteria(query))
}
func readCriteriaAsMaps(criteria []eventstore.ReadCriterion) []map[string]any {
	return nonemptyCriteria(eventstore.EncodeReadCriteria(criteria))
}
func nonemptyCriteria(criteria []map[string]any) []map[string]any {
	result := make([]map[string]any, 0, len(criteria))
	for _, criterion := range criteria {
		if len(criterion) > 0 {
			result = append(result, criterion)
		}
	}
	return result
}

func hasCriteria(query *eventstore.Query) bool {
	return len(criteriaAsMaps(query)) > 0
}

func readyIndexes(indexes []indexDefinition) []indexDefinition {
	ready := make([]indexDefinition, 0, len(indexes))
	for _, idx := range indexes {
		if idx.State == indexStateReady {
			ready = append(ready, idx)
		}
	}
	return ready
}

func contextStatusErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	switch err := ctx.Err(); {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return statuscode.New(statuscode.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return statuscode.New(statuscode.DeadlineExceeded, err.Error())
	default:
		return err
	}
}

type optimisticConflictError struct {
	expectedTx, expectedGid int64
	actualTx, actualGid     int64
}

func (e optimisticConflictError) Error() string {
	return fmt.Sprintf(
		"OptimisticConcurrencyException:StreamVersionConflict: Expected (%d, %d), Actual (%d, %d)",
		e.expectedTx, e.expectedGid, e.actualTx, e.actualGid,
	)
}

func optimisticConflict(expectedTx, expectedGid, actualTx, actualGid int64) error {
	return optimisticConflictError{
		expectedTx: expectedTx, expectedGid: expectedGid,
		actualTx: actualTx, actualGid: actualGid,
	}
}

func isOptimisticConflict(err error) bool {
	var conflict optimisticConflictError
	return errors.As(err, &conflict) || strings.Contains(err.Error(), "OptimisticConcurrencyException")
}
