package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestTagOperatorsWithinGroupCommit(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	inRange := []orisun.ReadTag{{Key: "amount", Value: "10", Operator: "gte"}, {Key: "amount", Value: "20", Operator: "lt"}}
	above := []orisun.ReadTag{{Key: "amount", Value: "20", Operator: "gt"}}
	cases := []struct {
		value    int
		tags     []orisun.ReadTag
		position orisun.Position
		conflict bool
	}{
		{15, nil, orisun.Position{}, false},
		{99, inRange, orisun.NotExistsPosition(), true},
		{25, above, orisun.NotExistsPosition(), false},
		{99, above, orisun.NotExistsPosition(), true},
		{5, inRange, orisun.Position{CommitPosition: 1, PreparePosition: 0}, false},
		{40, inRange, orisun.Position{CommitPosition: 1, PreparePosition: 0}, false},
		{50, []orisun.ReadTag{{Key: "amount", Value: "15"}}, orisun.Position{CommitPosition: 1, PreparePosition: 0}, false},
	}
	payloads := make([]postgresBatchPayload, len(cases))
	for i, tc := range cases {
		events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{EventId: uuid.NewString(), EventType: "Amount", Data: map[string]any{"amount": tc.value}}})
		require.NoError(t, err)
		payloads[i].Events, err = json.Marshal(events)
		require.NoError(t, err)
		var checks []orisun.ConsistencyCheck
		if tc.tags != nil {
			checks = []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: tc.tags}}, Position: tc.position}}
		}
		payloads[i].Consistency, err = orisun.MarshalConsistency(checks)
		require.NoError(t, err)
	}
	payload, err := json.Marshal(payloads)
	require.NoError(t, err)
	rows, err := db.QueryContext(t.Context(), fmt.Sprintf(insertEventRequestsWithConsistency, "public"), "test_boundary", "public", payload)
	require.NoError(t, err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		var index int
		var gid, tx, last sql.NullInt64
		var code, message sql.NullString
		require.NoError(t, rows.Scan(&index, &gid, &tx, &last, &code, &message))
		require.Equal(t, cases[index].conflict, message.Valid, "request %d: %s", index, message.String)
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, len(cases), count)
}

// Every operator must see earlier accepted requests in the same SQL batch,
// including the equality boundary. None of these rows exists before the call.
func TestTagOperatorGroupCommitMatrix(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	var payloads []postgresBatchPayload
	var conflicts []bool
	for _, operator := range []string{"", "eq", "ne", "gt", "gte", "lt", "lte"} {
		for _, value := range []int{5, 10, 15} {
			scope := uuid.NewString()
			matches := map[string]bool{"": value == 10, "eq": value == 10, "ne": value != 10, "gt": value > 10, "gte": value >= 10, "lt": value < 10, "lte": value <= 10}[operator]
			for _, checked := range []bool{false, true} {
				eventData := map[string]any{"operator_scope": scope, "amount": value}
				if checked {
					eventData = map[string]any{}
				}
				events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{EventId: uuid.NewString(), EventType: "BatchMatrix", Data: eventData}})
				require.NoError(t, err)
				var checks []orisun.ConsistencyCheck
				if checked {
					checks = []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "operator_scope", Value: scope}, {Key: "amount", Value: "10", Operator: operator}}}}, Position: orisun.NotExistsPosition()}}
				}
				payload := postgresBatchPayload{}
				payload.Events, err = json.Marshal(events)
				require.NoError(t, err)
				payload.Consistency, err = orisun.MarshalConsistency(checks)
				require.NoError(t, err)
				payloads = append(payloads, payload)
				conflicts = append(conflicts, checked && matches)
			}
		}
	}
	encoded, err := json.Marshal(payloads)
	require.NoError(t, err)
	rows, err := db.QueryContext(t.Context(), fmt.Sprintf(insertEventRequestsWithConsistency, "public"), "test_boundary", "public", encoded)
	require.NoError(t, err)
	defer rows.Close()
	seen := make(map[int]bool)
	for rows.Next() {
		var index int
		var gid, tx, last sql.NullInt64
		var code, message sql.NullString
		require.NoError(t, rows.Scan(&index, &gid, &tx, &last, &code, &message))
		require.GreaterOrEqual(t, index, 0)
		require.Less(t, index, len(conflicts))
		require.False(t, seen[index])
		seen[index] = true
		require.Equal(t, conflicts[index], message.Valid, "request %d: %s", index, message.String)
		require.Equal(t, !conflicts[index], tx.Valid, "request %d", index)
		if conflicts[index] {
			require.Equal(t, "P0001", code.String)
			require.Contains(t, message.String, "OptimisticConcurrencyException:StreamVersionConflict")
		} else {
			require.False(t, code.Valid)
		}
	}
	require.NoError(t, rows.Err())
	require.Len(t, seen, len(payloads))
}
