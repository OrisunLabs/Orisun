package postgres

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
)

func TestBoundaryIndexDefinitionOwnership(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.container.Terminate(context.Background())) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	admin := NewPostgresAdminDB(db, logger, "public", "test_boundary", map[string]config.BoundaryToPostgresSchemaMapping{"test_boundary": {Boundary: "test_boundary", Schema: "public"}})
	ctx := t.Context()
	fields := []eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: "text"}}
	conditions := []eventstore.BoundaryIndexCondition{{Key: "kind", Operator: "=", Value: "Created"}, {Key: "active", Operator: "=", Value: "true"}}
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test_boundary", "guard", fields, conditions, "AND"))
	before, err := admin.GetBoundaryIndex(ctx, "test_boundary", "guard")
	require.NoError(t, err)
	physical := func() string {
		var ddl string
		require.NoError(t, db.QueryRow("SELECT pg_get_indexdef('public.test_boundary_guard_idx'::regclass)").Scan(&ddl))
		return ddl
	}
	ddl := physical()
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test_boundary", "guard", fields, conditions, ""))
	for _, tc := range []struct {
		fields     []eventstore.BoundaryIndexField
		conditions []eventstore.BoundaryIndexCondition
		combinator string
	}{
		{[]eventstore.BoundaryIndexField{{JsonKey: "other", ValueType: "text"}}, conditions, "AND"},
		{[]eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: "numeric"}}, conditions, "AND"},
		{[]eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: "timestamptz"}}, conditions, "AND"},
		{fields, nil, "AND"}, {fields, conditions, "OR"},
	} {
		err := admin.CreateBoundaryIndex(ctx, "test_boundary", "guard", tc.fields, tc.conditions, tc.combinator)
		require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
		after, err := admin.GetBoundaryIndex(ctx, "test_boundary", "guard")
		require.NoError(t, err)
		require.Equal(t, before, after)
		require.Equal(t, ddl, physical())
	}
	_, err = db.Exec("DELETE FROM public.test_boundary_orisun_boundary_index_metadata WHERE name='guard'")
	require.NoError(t, err)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(admin.CreateBoundaryIndex(ctx, "test_boundary", "guard", fields, nil, "")))
	_, err = admin.GetBoundaryIndex(ctx, "test_boundary", "guard")
	require.Equal(t, statuscode.NotFound, statuscode.CodeOf(err))
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test_boundary", "guard", fields, conditions, "AND"))
	require.NoError(t, admin.DropBoundaryIndex(ctx, "test_boundary", "guard"))
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test_boundary", "guard", fields, nil, ""))

	// Stored metadata alone is not proof of the physical definition.
	_, err = db.Exec("DROP INDEX public.test_boundary_guard_idx; CREATE INDEX test_boundary_guard_idx ON public.test_boundary_orisun_es_event ((data->>'unexpected'))")
	require.NoError(t, err)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(admin.CreateBoundaryIndex(ctx, "test_boundary", "guard", fields, nil, "")))
	require.NoError(t, admin.DropBoundaryIndex(ctx, "test_boundary", "guard"))
	_, err = db.Exec("CREATE TABLE public.test_boundary_collision_idx (id int)")
	require.NoError(t, err)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(admin.CreateBoundaryIndex(ctx, "test_boundary", "collision", fields, nil, "")))
	_, err = admin.GetBoundaryIndex(ctx, "test_boundary", "collision")
	require.Equal(t, statuscode.NotFound, statuscode.CodeOf(err))
	require.Equal(t, statuscode.InvalidArgument, statuscode.CodeOf(admin.CreateBoundaryIndex(ctx, "test_boundary", strings.Repeat("x", 63), fields, nil, "")))

	// A failed reservation retains its definition even before a physical index exists.
	_, err = db.Exec(`INSERT INTO public.test_boundary_orisun_boundary_index_metadata(name,fields,conditions,combinator,state) VALUES ('building','[{"JsonKey":"value","ValueType":"text"}]','[]','AND','building')`)
	require.NoError(t, err)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(admin.CreateBoundaryIndex(ctx, "test_boundary", "building", []eventstore.BoundaryIndexField{{JsonKey: "other", ValueType: "text"}}, nil, "")))
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test_boundary", "building", fields, nil, ""))

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i, key := range []string{"first", "second"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = admin.CreateBoundaryIndex(ctx, "test_boundary", "race", []eventstore.BoundaryIndexField{{JsonKey: key, ValueType: "text"}}, nil, "")
		}()
	}
	wg.Wait()
	winner := 0
	if results[0] != nil {
		winner = 1
	}
	require.NoError(t, results[winner])
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(results[1-winner]))
	index, err := admin.GetBoundaryIndex(ctx, "test_boundary", "race")
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}[winner], index.Fields[0].JsonKey)
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test_boundary", "race", index.Fields, nil, ""))
}
