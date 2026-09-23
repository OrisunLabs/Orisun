package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEventEnvelopeMigrationPreservesHistoryAndRejectsCollision(t *testing.T) {
	container, err := setupTestContainer(t)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.container.Terminate(context.Background()) })
	db, err := setupTestDatabase(t, container)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	restoreLegacyEnvelopeColumns(t, db)
	_, err = db.Exec(`
 INSERT INTO test_boundary_orisun_es_write VALUES(9007199254740993,'[]');
 INSERT INTO test_boundary_orisun_es_event(transaction_id,global_id,write_id,data,metadata,date_created) VALUES
 (9007199254740994,9007199254740993,9007199254740993,'{"__eventId":"00000000-0000-0000-0000-000000000001","__eventType":"Created","__metadata":null,"number":9223372036854775807}','{"__trace":9223372036854775807}','2026-01-02T03:04:05.123456Z');
 CREATE INDEX custom_envelope_index ON test_boundary_orisun_es_event((data->>'number'),global_id) WHERE transaction_id>0;
 `)
	require.NoError(t, err)
	require.ErrorContains(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()), "conflicts")
	var version int
	require.NoError(t, db.QueryRow("SELECT version FROM test_boundary_orisun_schema_version").Scan(&version))
	require.Equal(t, 2, version)
	_, err = db.Exec("UPDATE test_boundary_orisun_es_event SET data=data-'__metadata'")
	require.NoError(t, err)
	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()))
	require.NoError(t, RunDbScripts(db, "test_boundary", "public", false, t.Context()))
	var data, metadata, created string
	var tx, gid, wid int64
	require.NoError(t, db.QueryRow("SELECT data,metadata,date_created,transaction_id,global_id,write_id FROM test_boundary_orisun_es_event").Scan(&data, &metadata, &created, &tx, &gid, &wid))
	require.Contains(t, data, `"__commitPosition": 9007199254740994`)
	require.Contains(t, data, `"__writeId": "9007199254740994:9007199254740993"`)
	require.Contains(t, data, "9223372036854775807")
	require.JSONEq(t, `{"__trace":9223372036854775807}`, metadata)
	require.Equal(t, "2026-01-02T03:04:05.123456Z", created)
	require.EqualValues(t, 9007199254740994, tx)
	require.EqualValues(t, 9007199254740993, gid)
	require.Equal(t, gid, wid)
	var generated, indexCount int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM information_schema.columns WHERE table_name='test_boundary_orisun_es_event' AND is_generated='ALWAYS'").Scan(&generated))
	require.Equal(t, 5, generated)
	require.NoError(t, db.QueryRow("SELECT count(*) FROM pg_indexes WHERE indexname='custom_envelope_index'").Scan(&indexCount))
	require.Equal(t, 1, indexCount)
	_, err = db.Exec("UPDATE test_boundary_orisun_es_event SET global_id=1")
	require.Error(t, err)
}
