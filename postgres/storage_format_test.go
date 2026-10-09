package postgres

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBoundaryStorageRejectsOlderFormatsWithoutConversion(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	defer func() { require.NoError(t, container.container.Terminate(context.Background())) }()
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	defer db.Close()
	for _, version := range []int{0, 1, 4, 6} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			boundary := fmt.Sprintf("old_format_%d", version)
			table := "public." + boundary + "_orisun_es_event"
			_, err := db.Exec("CREATE TABLE " + table + "(data JSONB); INSERT INTO " + table + " VALUES ('{\"original\":true}')")
			require.NoError(t, err)
			if version != 0 {
				_, err = db.Exec("CREATE TABLE public." + boundary + "_orisun_schema_version(id INTEGER PRIMARY KEY,version INTEGER); INSERT INTO public." + boundary + "_orisun_schema_version VALUES (1," + fmt.Sprint(version) + ")")
				require.NoError(t, err)
			}
			require.ErrorContains(t, RunDbScripts(db, boundary, "public", false, t.Context()), "unsupported")
			var data string
			require.NoError(t, db.QueryRow("SELECT data::TEXT FROM "+table).Scan(&data))
			require.JSONEq(t, `{"original":true}`, data)
			if version != 0 {
				var got int
				require.NoError(t, db.QueryRow("SELECT version FROM public."+boundary+"_orisun_schema_version WHERE id=1").Scan(&got))
				require.Equal(t, version, got)
			}
		})
	}
}
