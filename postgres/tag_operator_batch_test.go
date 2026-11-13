package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGroupCommitReadsStateAfterAcquiringBoundaryLock(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	for _, checked := range []bool{true, false} {
		t.Run(fmt.Sprintf("checked_%t", checked), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			payload := func(checks []orisun.ConsistencyCheck) []any {
				events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{EventId: uuid.NewString(), EventType: "LockGuard", Data: map[string]any{"scope": t.Name(), "amount": 15}}})
				require.NoError(t, err)
				eventJSON, err := json.Marshal(events)
				require.NoError(t, err)
				checkJSON, err := orisun.MarshalConsistency(checks)
				require.NoError(t, err)
				return insertRequestsArgs(t, "test_boundary", "public", []postgresBatchPayload{{Events: eventJSON, Consistency: checkJSON}})
			}
			var checks []orisun.ConsistencyCheck
			if checked {
				checks = []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "scope", Value: t.Name()}, {Key: "amount", Operator: "gt", Value: "10"}}}}, Position: orisun.NotExistsPosition()}}
			}
			waitingPayload, firstPayload := payload(checks), payload(nil)
			first, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer first.Rollback()
			_, err = first.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtext('public.test_boundary::position_draw'))")
			require.NoError(t, err)
			second, err := db.Conn(ctx)
			require.NoError(t, err)
			defer second.Close()
			var pid int
			require.NoError(t, second.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid))
			type result struct {
				message sql.NullString
				err     error
			}
			done := make(chan result, 1)
			query := "SELECT error_message FROM (" + fmt.Sprintf(insertEventRequestsWithConsistency, "public") + ") AS outcome"
			go func() {
				var r result
				r.err = second.QueryRowContext(ctx, query, waitingPayload...).Scan(&r.message)
				done <- r
			}()
			// Observe actual lock contention, rather than depending on a sleep
			// to guess when the waiting writer has reached its state reads.
			require.Eventually(t, func() bool {
				var waiting bool
				err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND locktype = 'advisory' AND NOT granted)", pid).Scan(&waiting)
				return err == nil && waiting
			}, 5*time.Second, 10*time.Millisecond)
			var firstError sql.NullString
			require.NoError(t, first.QueryRowContext(ctx, query, firstPayload...).Scan(&firstError))
			require.False(t, firstError.Valid)
			require.NoError(t, first.Commit())
			r := <-done
			require.NoError(t, r.err)
			require.Equal(t, checked, r.message.Valid)
			if checked {
				require.Contains(t, r.message.String, "OptimisticConcurrencyException")
			}
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.test_boundary_orisun_es_event WHERE data->>'scope' = $1", t.Name()).Scan(&count))
			if checked {
				require.Equal(t, 1, count)
			} else {
				require.Equal(t, 2, count)
			}
		})
	}
}

// Grouping operator matches must retain intermediate matches, their latest
// position, and request rejection isolation. Compare against persisted reads
// between individual requests, which do not use in-batch criterion updates.
func TestTagOperatorMultiEventBatchMatchesSequential(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, RunDbScripts(db, "reference_boundary", "public", false, t.Context()))
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	saver := NewPostgresSaveEvents(t.Context(), db, logger, map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary":      {Boundary: "test_boundary", Schema: "public"},
		"reference_boundary": {Boundary: "reference_boundary", Schema: "public"},
	})
	defer saver.close()
	check := func(position orisun.Position, tags ...orisun.ReadTag) orisun.ConsistencyCheck {
		return orisun.ConsistencyCheck{Criteria: []orisun.ReadCriterion{{Tags: tags}}, Position: position}
	}
	request := func(values []int, checks ...orisun.ConsistencyCheck) *postgresTestRequest {
		events := make([]orisun.EventWithMapTags, len(values))
		for i, value := range values {
			events[i] = orisun.EventWithMapTags{EventId: uuid.NewString(), EventType: "Amount", Data: map[string]any{"amount": value}}
		}
		prepared, err := orisun.PrepareEventsForSave(events)
		require.NoError(t, err)
		return &postgresTestRequest{ctx: t.Context(), events: prepared, consistency: checks}
	}
	rangeTags := []orisun.ReadTag{{Key: "amount", Value: "10", Operator: "gte"}, {Key: "amount", Value: "20", Operator: "lt"}}
	requests := []*postgresTestRequest{
		request([]int{15, 18, 25}),
		request([]int{99, 100}, check(orisun.NotExistsPosition(), rangeTags...)),
		request([]int{1}, check(orisun.NotExistsPosition(), orisun.ReadTag{Key: "amount", Value: "90", Operator: "gt"})),
		request([]int{2}, check(orisun.Position{CommitPosition: 3, PreparePosition: 1}, rangeTags...)),
		request([]int{3}, check(orisun.Position{CommitPosition: 3, PreparePosition: 0}, orisun.ReadTag{Key: "amount", Value: "15"})),
	}
	batched, err := saver.executeTestBatch(t.Context(), "test_boundary", requests)
	require.NoError(t, err)
	require.Len(t, batched, len(requests))
	for i, req := range requests {
		sequential, err := saver.executeTestBatch(t.Context(), "reference_boundary", []*postgresTestRequest{req})
		require.NoError(t, err)
		require.Len(t, sequential, 1)
		if i == 1 {
			require.ErrorContains(t, batched[i].err, "OptimisticConcurrencyException")
			require.EqualError(t, sequential[0].err, batched[i].err.Error())
		} else {
			require.NoError(t, batched[i].err, "request %d", i)
			require.NoError(t, sequential[0].err, "request %d", i)
			require.Equal(t, sequential[0].transactionID, batched[i].transactionID)
			require.Equal(t, sequential[0].globalID, batched[i].globalID)
		}
	}
	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM public.test_boundary_orisun_es_event").Scan(&count))
	require.Equal(t, 6, count)
}

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
	rows, err := db.QueryContext(t.Context(), fmt.Sprintf(insertEventRequestsWithConsistency, "public"), insertRequestsArgs(t, "test_boundary", "public", payloads)...)
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
	rows, err := db.QueryContext(t.Context(), fmt.Sprintf(insertEventRequestsWithConsistency, "public"), insertRequestsArgs(t, "test_boundary", "public", payloads)...)
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
