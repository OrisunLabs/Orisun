package postgres

import (
	"context"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/storagecontract"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTagOperatorsContract(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer container.container.Terminate(context.Background())
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, RunDbScripts(db, "test_boundary", "operator_tenant", false, t.Context()))
	_, err = db.Exec("DROP FUNCTION public.orisun_criterion_sql(jsonb,text); DROP FUNCTION public.orisun_compare_number(text,text)")
	require.NoError(t, err)
	logger, _ := logging.ZapLogger("error")
	mapping := map[string]config.BoundaryToPostgresSchemaMapping{"test_boundary": {Boundary: "test_boundary", Schema: "operator_tenant"}}
	saver := NewPostgresSaveEvents(t.Context(), db, logger, mapping)
	defer saver.close()
	getter := NewPostgresGetEvents(db, logger, mapping)
	storagecontract.TagOperators(t, saver, getter, getter, "test_boundary")
}
