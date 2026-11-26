package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/goccy/go-json"
)

type postgresBatchPayload struct {
	Consistency json.RawMessage `json:"consistency"`
	Events      json.RawMessage `json:"events"`
}

// insertEventRequestsArgs binds one flush to the boundary write function.
func insertEventRequestsArgs(boundary, schema string, payloads []postgresBatchPayload, checks [][]compiledCheck) ([]any, error) {
	payloadJSON, err := json.Marshal(payloads)
	if err != nil {
		return nil, fmt.Errorf("marshal group commit payload: %w", err)
	}
	plan := planPostgresFlush(postgresEventTable(schema, boundary), checks)
	return []any{
		payloadJSON, plan.criterionCount,
		plan.requestCheckOffsets, plan.checkCriterionOffsets, plan.checkCriterionIDs,
		plan.expectedTransactions, plan.expectedGlobals,
		plan.latestQuery, plan.equalityCriteria, plan.criterionShapes, plan.predicateQuery,
	}, nil
}

// executeBatch owns database execution only. It returns a complete ordered set
// of outcomes after consuming the SQL result to completion; it never completes
// callers. PostgreSQL owns the writer lock, CCC matching, and atomic commit.
func (s *PostgresSaveEvents) executeBatch(
	ctx context.Context,
	boundary string,
	requests []preparedPostgresSave,
) ([]postgresSaveResult, error) {
	entry, ok := s.registry.lookup(boundary)
	if !ok {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "no schema found for boundary: %s", boundary)
	}

	if len(requests) == 0 {
		return nil, nil
	}
	payloads := make([]postgresBatchPayload, len(requests))
	checks := make([][]compiledCheck, len(requests))
	for i, request := range requests {
		payloads[i] = request.payload
		checks[i] = request.checks
	}
	args, err := insertEventRequestsArgs(boundary, entry.mapping.Schema, payloads, checks)
	if err != nil {
		return nil, err
	}
	if s.logger.IsDebugEnabled() {
		s.logger.Debugf("postgres group commit: boundary=%s requests=%d", boundary, len(requests))
	}
	rows, err := s.db.QueryContext(ctx, entry.insertEventRequests, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	seen := make([]bool, len(requests))
	sqlOutcomes := make([]postgresSaveResult, len(requests))
	for rows.Next() {
		var (
			index         int
			newGlobalID   sql.NullInt64
			transactionID sql.NullInt64
			globalID      sql.NullInt64
			errorCode     sql.NullString
			errorMessage  sql.NullString
		)
		if err := rows.Scan(
			&index,
			&newGlobalID,
			&transactionID,
			&globalID,
			&errorCode,
			&errorMessage,
		); err != nil {
			return nil, fmt.Errorf("scan group commit result: %w", err)
		}
		if index < 0 || index >= len(requests) {
			return nil, fmt.Errorf("group commit returned request index %d for batch of %d", index, len(requests))
		}
		if seen[index] {
			return nil, fmt.Errorf("group commit returned duplicate request index %d", index)
		}
		seen[index] = true

		outcome := postgresSaveResult{}
		if errorMessage.Valid {
			outcome.err = s.mapSaveError(errors.New(errorMessage.String))
		} else if !transactionID.Valid || !globalID.Valid {
			return nil, fmt.Errorf(
				"group commit request %d returned neither a position nor an error (SQLSTATE %q)",
				index,
				errorCode.String,
			)
		} else {
			outcome.transactionID = strconv.FormatInt(transactionID.Int64, 10)
			outcome.globalID = globalID.Int64
		}
		sqlOutcomes[index] = outcome
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for index, wasSeen := range seen {
		if !wasSeen {
			return nil, fmt.Errorf("group commit returned no result for request index %d", index)
		}
	}
	return sqlOutcomes, nil
}
