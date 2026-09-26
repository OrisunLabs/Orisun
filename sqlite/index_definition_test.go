package sqlite

import (
	"sync"
	"testing"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/logging"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestIndexDefinitionsAreImmutable(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	admin := NewSqliteAdminDB(pools, "test", logger)
	ctx := t.Context()
	fields := []eventstore.BoundaryIndexField{{JsonKey: "value", ValueType: "text"}}
	conditions := []eventstore.BoundaryIndexCondition{{Key: "kind", Operator: "=", Value: "Created"}, {Key: "active", Operator: "=", Value: "true"}}
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test", "guard", fields, conditions, "AND"))
	before, err := admin.GetBoundaryIndex(ctx, "test", "guard")
	require.NoError(t, err)
	physical := func() string {
		conn, err := pools["test"].Read.Take(ctx)
		require.NoError(t, err)
		defer pools["test"].Read.Put(conn)
		var ddl string
		require.NoError(t, sqlitex.Execute(conn, "SELECT sql FROM sqlite_schema WHERE name='guard_idx'", &sqlitex.ExecOptions{ResultFunc: func(s *sqlite.Stmt) error { ddl = s.ColumnText(0); return nil }}))
		return ddl
	}
	ddl := physical()
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test", "guard", fields, conditions, ""))
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
		err := admin.CreateBoundaryIndex(ctx, "test", "guard", tc.fields, tc.conditions, tc.combinator)
		require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
		after, err := admin.GetBoundaryIndex(ctx, "test", "guard")
		require.NoError(t, err)
		require.Equal(t, before, after)
		require.Equal(t, ddl, physical())
	}
	// Adopt only a physical definition matching the requested one.
	conn, err := pools["test"].Write.Take(ctx)
	require.NoError(t, err)
	require.NoError(t, sqlitex.Execute(conn, "DELETE FROM orisun_boundary_index_metadata WHERE name='guard'", nil))
	pools["test"].Write.Put(conn)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(admin.CreateBoundaryIndex(ctx, "test", "guard", fields, nil, "")))
	_, err = admin.GetBoundaryIndex(ctx, "test", "guard")
	require.Equal(t, statuscode.NotFound, statuscode.CodeOf(err))
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test", "guard", fields, conditions, "AND"))
	require.NoError(t, admin.DropBoundaryIndex(ctx, "test", "guard"))
	require.NoError(t, admin.CreateBoundaryIndex(ctx, "test", "guard", fields, nil, ""))
	conn, err = pools["test"].Write.Take(ctx)
	require.NoError(t, err)
	require.NoError(t, sqlitex.ExecuteScript(conn, `DROP INDEX guard_idx; CREATE INDEX guard_idx ON orisun_es_event(global_id);`, nil))
	pools["test"].Write.Put(conn)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(admin.CreateBoundaryIndex(ctx, "test", "guard", fields, nil, "")))

}

func TestConcurrentIndexDefinitionsHaveOneWinner(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, err := logging.ZapLogger("error")
	require.NoError(t, err)
	admin := NewSqliteAdminDB(pools, "test", logger)
	var wg sync.WaitGroup
	errors := make([]error, 2)
	for i, key := range []string{"first", "second"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errors[i] = admin.CreateBoundaryIndex(t.Context(), "test", "race", []eventstore.BoundaryIndexField{{JsonKey: key, ValueType: "text"}}, nil, "")
		}()
	}
	wg.Wait()
	winner := 0
	if errors[0] != nil {
		winner = 1
	}
	require.NoError(t, errors[winner])
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(errors[1-winner]))
	index, err := admin.GetBoundaryIndex(t.Context(), "test", "race")
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}[winner], index.Fields[0].JsonKey)
	require.NoError(t, admin.CreateBoundaryIndex(t.Context(), "test", "race", index.Fields, nil, ""))
}
