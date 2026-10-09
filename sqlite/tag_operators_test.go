package sqlite

import (
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/internal/storagecontract"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestTagOperatorsContract(t *testing.T) {
	pools, cleanup := newTestPools(t)
	defer cleanup()
	logger, _ := logging.ZapLogger("error")
	saver := NewSqliteSaveEvents(pools, logger)
	defer saver.close()
	getter := NewSqliteGetEvents(pools, logger)
	storagecontract.TagOperators(t, saver, getter, getter, "test")
}

func TestTagOperatorsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	logger, _ := logging.ZapLogger("error")
	open := func() (*BoundaryPools, *SqliteSaveEvents, *SqliteGetEvents) {
		pool, err := OpenBoundaryPools(t.Context(), dir, "test")
		require.NoError(t, err)
		pools := map[string]*BoundaryPools{"test": pool}
		return pool, NewSqliteSaveEvents(pools, logger), NewSqliteGetEvents(pools, logger)
	}
	pool, saver, getter := open()
	tags := []orisun.ReadTag{{Key: "amount", Value: "10", Operator: "gte"}, {Key: "amount", Value: "20", Operator: "lt"}}
	checks := []orisun.ConsistencyCheck{{Criteria: []orisun.ReadCriterion{{Tags: tags}}, Position: orisun.NotExistsPosition()}}
	events, err := orisun.PrepareEventsForSave([]orisun.EventWithMapTags{mustEvent(t, "Amount", map[string]any{"amount": 15}, nil)})
	require.NoError(t, err)
	tx, gid, err := saver.SavePrepared(t.Context(), events, "test", checks)
	require.NoError(t, err)
	commit, err := strconv.ParseInt(tx, 10, 64)
	require.NoError(t, err)
	saver.close()
	require.NoError(t, pool.Close())
	pool, saver, getter = open()
	defer pool.Close()
	defer saver.close()
	latest, err := getter.GetLatestByCriteria(t.Context(), orisun.LatestByCriteriaQuery{Boundary: "test", Criteria: checks[0].Criteria})
	require.NoError(t, err)
	require.True(t, latest.Matches[0].Found)
	require.Equal(t, events[0].EventId, latest.Matches[0].Event.EventId)
	_, _, err = saver.SavePrepared(t.Context(), events, "test", checks)
	require.Equal(t, statuscode.AlreadyExists, statuscode.CodeOf(err))
	recorded, err := getter.GetWriteContext(t.Context(), &orisun.GetWriteContextRequest{Boundary: "test", WriteId: orisun.WriteID(commit, gid)})
	require.NoError(t, err)
	require.Equal(t, "gte", recorded.Consistency[0].Query.Criteria[0].Tags[0].Operator)
	require.Equal(t, "lt", recorded.Consistency[0].Query.Criteria[0].Tags[1].Operator)
}
