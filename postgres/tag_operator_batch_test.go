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
