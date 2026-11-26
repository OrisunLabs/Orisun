package postgres

import (
	"context"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// Save keeps backend tests concise while production accepts prepared batches
// exclusively through SavePrepared.
func (s *PostgresSaveEvents) Save(
	ctx context.Context,
	events []eventstore.EventWithMapTags,
	boundary string,
	observations []*eventstore.ConsistencyObservation,
) (string, int64, error) {
	prepared, err := eventstore.PrepareEventsForSave(events)
	if err != nil {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "invalid event data: %v", err)
	}
	consistency, err := eventstore.ConsistencyChecksFromObservations(observations)
	if err != nil {
		return "", 0, err
	}
	return s.SavePrepared(ctx, prepared, boundary, consistency)
}

// insertRequestsArgs compiles raw batch payloads exactly as prepareSave would,
// for tests that drive the boundary write function directly.
func insertRequestsArgs(t testing.TB, boundary, schema string, payloads []postgresBatchPayload) []any {
	t.Helper()
	checks := make([][]compiledCheck, len(payloads))
	for i, payload := range payloads {
		var stored []struct {
			Query struct {
				Criteria []map[string]any `json:"criteria"`
			} `json:"query"`
			Position struct {
				TransactionID int64 `json:"transaction_id"`
				GlobalID      int64 `json:"global_id"`
			} `json:"position"`
		}
		if len(payload.Consistency) > 0 {
			require.NoError(t, json.Unmarshal(payload.Consistency, &stored))
		}
		for _, observation := range stored {
			check := compiledCheck{transactionID: observation.Position.TransactionID, globalID: observation.Position.GlobalID}
			for _, fields := range observation.Query.Criteria {
				criterion, err := compileCriterion(fields)
				require.NoError(t, err)
				check.criteria = append(check.criteria, criterion)
			}
			checks[i] = append(checks[i], check)
		}
	}
	args, err := insertEventRequestsArgs(boundary, schema, payloads, checks)
	require.NoError(t, err)
	return args
}
