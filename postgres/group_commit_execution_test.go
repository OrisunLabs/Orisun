package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func newExecutionTest(t *testing.T) (*sql.DB, *PostgresSaveEvents, *PostgresGetEvents) {
	t.Helper()
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.container.Terminate(context.Background())) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, RunDbScripts(db, "reference_boundary", "public", false, t.Context()))
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	mappings := map[string]config.BoundaryToPostgresSchemaMapping{
		"test_boundary":      {Boundary: "test_boundary", Schema: "public"},
		"reference_boundary": {Boundary: "reference_boundary", Schema: "public"},
	}
	saver := NewPostgresSaveEvents(t.Context(), db, logger, mappings)
	t.Cleanup(saver.close)
	return db, saver, NewPostgresGetEvents(db, logger, mappings)
}

// The oracle reads persisted SQL state between individual writes. The batch
// must agree across variable check/OR sizes, all six operators, shared shapes,
// duplicate criteria, intermediate event matches, and rejected requests.
func TestGroupCommitExecutionPlanMatchesSequential(t *testing.T) {
	db, saver, getter := newExecutionTest(t)
	operators := []string{"eq", "ne", "gt", "gte", "lt", "lte"}
	var requests []*postgresTestRequest
	var expected []postgresTestOutcome
	for i := 0; i < 96; i++ {
		var checks []orisun.ConsistencyCheck
		if i%4 != 0 {
			for j := 0; j < 1+i%3; j++ {
				tags := []orisun.ReadTag{{Key: "amount", Operator: operators[(i+j)%6], Value: fmt.Sprint(5 + (i+j)%3)}}
				if j%2 == 0 {
					tags = append(tags, orisun.ReadTag{Key: "scope", Value: fmt.Sprint((i + j) % 3)})
				}
				criteria := []orisun.ReadCriterion{{Tags: tags}}
				if i%2 == 0 {
					criteria = append(criteria, orisun.ReadCriterion{Tags: []orisun.ReadTag{{Key: "scope", Value: "absent"}}})
				}
				if i%5 == 0 {
					criteria = append(criteria, criteria[0])
				}
				query := &orisun.Query{}
				for _, c := range criteria {
					criterion := &orisun.Criterion{}
					for _, tag := range c.Tags {
						criterion.Tags = append(criterion.Tags, &orisun.Tag{Key: tag.Key, Value: tag.Value, Operator: tag.Operator})
					}
					query.Criteria = append(query.Criteria, criterion)
				}
				rows, err := getter.GetBatch(t.Context(), &orisun.GetEventsRequest{Boundary: "reference_boundary", Query: query, Count: 1, Direction: orisun.Direction_DESC})
				require.NoError(t, err)
				position := orisun.NotExistsPosition()
				if len(rows) > 0 {
					position = orisun.Position{CommitPosition: rows[0].CommitPosition, PreparePosition: rows[0].PreparePosition}
				}
				if i%7 == 0 && j == i%3 {
					position = orisun.Position{CommitPosition: 999999, PreparePosition: 999999}
				}
				checks = append(checks, orisun.ConsistencyCheck{Criteria: criteria, Position: position})
			}
		}
		var events []orisun.EventWithMapTags
		for e := 0; e < 1+i%4; e++ {
			events = append(events, orisun.EventWithMapTags{EventId: uuid.NewString(), EventType: "Fact", Data: map[string]any{"scope": fmt.Sprint(i % 3), "amount": (i + e*3) % 11}, Metadata: map[string]any{"request": i}})
		}
		prepared, err := orisun.PrepareEventsForSave(events)
		require.NoError(t, err)
		request := &postgresTestRequest{ctx: t.Context(), events: prepared, consistency: checks}
		result, err := saver.executeTestBatch(t.Context(), "reference_boundary", []*postgresTestRequest{request})
		require.NoError(t, err)
		requests = append(requests, request)
		expected = append(expected, result[0])
	}
	actual, err := saver.executeTestBatch(t.Context(), "test_boundary", requests)
	require.NoError(t, err)
	require.Len(t, actual, len(expected))
	accepted, rejected := 0, 0
	for i, want := range expected {
		require.Same(t, requests[i], actual[i].req)
		require.Equal(t, statuscode.CodeOf(want.err), statuscode.CodeOf(actual[i].err), "request %d", i)
		if want.err == nil {
			accepted++
			require.Equal(t, want.globalID, actual[i].globalID)
			require.Equal(t, want.transactionID, actual[i].transactionID)
		} else {
			rejected++
			require.EqualError(t, actual[i].err, want.err.Error())
		}
	}
	require.Positive(t, accepted)
	require.Positive(t, rejected)
	for _, query := range []string{
		`SELECT jsonb_agg(jsonb_build_array(transaction_id, global_id, write_id, event_id, event_type, metadata, data) ORDER BY global_id)::text FROM public.%s_orisun_es_event`,
		`SELECT jsonb_agg(jsonb_build_array(write_id,consistency) ORDER BY write_id)::text FROM public.%s_orisun_es_write`,
	} {
		var batch, reference string
		require.NoError(t, db.QueryRow(fmt.Sprintf(query, "test_boundary")).Scan(&batch))
		require.NoError(t, db.QueryRow(fmt.Sprintf(query, "reference_boundary")).Scan(&reference))
		require.JSONEq(t, reference, batch)
	}
	// All-rejected batches must still return one ordered outcome per request.
	stale := requests[1]
	stale.consistency = []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "__eventType", Value: "Fact"}}}}, Position: orisun.NotExistsPosition()}}
	results, err := saver.executeTestBatch(t.Context(), "test_boundary", []*postgresTestRequest{stale, stale})
	require.NoError(t, err)
	require.Len(t, results, 2)
	for _, result := range results {
		require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(result.err))
	}
}

func TestGroupCommitPersistenceFailureInvalidatesEntirePlan(t *testing.T) {
	db, saver, _ := newExecutionTest(t)
	_, err := db.Exec(`CREATE FUNCTION public.reject_planned_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type = 'Fail' THEN RAISE EXCEPTION 'injected persistence failure'; END IF; RETURN NEW; END $$;
 CREATE TRIGGER reject_planned_event BEFORE INSERT ON public.test_boundary_orisun_es_event FOR EACH ROW EXECUTE FUNCTION public.reject_planned_event()`)
	require.NoError(t, err)
	request := func(kind string, checks []orisun.ConsistencyCheck) *postgresTestRequest {
		events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{{EventId: uuid.NewString(), EventType: kind, Data: map[string]any{"scope": "shared"}}})
		require.NoError(t, err)
		return &postgresTestRequest{ctx: t.Context(), events: events, consistency: checks}
	}
	checks := []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: []orisun.ReadTag{{Key: "scope", Value: "shared"}}}}, Position: orisun.NotExistsPosition()}}
	requests := []*postgresTestRequest{request("First", nil), request("Conflict", checks), request("Fail", nil), request("Last", nil)}
	outcomes, err := saver.executeTestBatch(t.Context(), "test_boundary", requests)
	require.ErrorContains(t, err, "injected persistence failure")
	require.Nil(t, outcomes)
	var events, writes int
	require.NoError(t, db.QueryRow(`SELECT (SELECT count(*) FROM public.test_boundary_orisun_es_event),(SELECT count(*) FROM public.test_boundary_orisun_es_write)`).Scan(&events, &writes))
	require.Zero(t, events)
	require.Zero(t, writes)
	// A subsequent request sees none of the rolled-back plan's criterion state.
	results, err := saver.executeTestBatch(t.Context(), "test_boundary", []*postgresTestRequest{request("Retry", checks)})
	require.NoError(t, err)
	require.NoError(t, results[0].err)
}
