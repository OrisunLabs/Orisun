package sqlite

import (
	"context"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
	"zombiezen.com/go/sqlite"
)

// Mutable fixtures exercise invalid prepared input and deterministic flushes.
// Production execution receives only preparedSQLiteSave and returns outcomes.
type sqliteTestRequest struct {
	ctx             context.Context
	inserts         eventstore.PreparedEventBatch
	consistency     []eventstore.ConsistencyCheck
	consistencyJSON string
	preparedChecks  []preparedConsistencyCheck
	result          chan sqliteSaveResult
}

func (r *sqliteTestRequest) prepared() preparedSQLiteSave {
	return preparedSQLiteSave{ctx: r.ctx, inserts: prepareEventRows(r.inserts), consistencyJSON: r.consistencyJSON, preparedChecks: r.preparedChecks}
}
func (s *SqliteSaveEvents) runTestFlush(boundary string, _ *BoundaryPools, requests []*sqliteTestRequest) {
	inputs := make([]preparedSQLiteSave, len(requests))
	for i, req := range requests {
		inputs[i] = req.prepared()
	}
	results, err := s.executeBatch(context.Background(), boundary, inputs)
	for i, req := range requests {
		if err != nil {
			req.result <- sqliteSaveResult{err: statuscode.Errorf(statuscode.Internal, "group commit flush: %v", err)}
		} else {
			req.result <- results[i]
		}
	}
}

type acceptedTestSave struct {
	req           *sqliteTestRequest
	transactionID string
	globalID      int64
}

func (s *SqliteSaveEvents) flushTestTx(conn *sqlite.Conn, requests []*sqliteTestRequest) ([]acceptedTestSave, error) {
	inputs := make([]preparedSQLiteSave, len(requests))
	for i, req := range requests {
		inputs[i] = req.prepared()
	}
	results, err := s.executeTransaction(conn, inputs)
	if err != nil {
		return nil, err
	}
	var accepted []acceptedTestSave
	for i, res := range results {
		if res.err != nil {
			requests[i].result <- res
		} else {
			accepted = append(accepted, acceptedTestSave{req: requests[i], transactionID: res.transactionID, globalID: res.globalID})
		}
	}
	return accepted, nil
}
func configureGCTest(s *SqliteSaveEvents, change func(*config.SqliteGroupCommitConfig)) {
	current := s.gc.Config()
	cfg := config.SqliteGroupCommitConfig{MaxBatchRequests: current.MaxBatchRequests, MaxBatchEvents: current.MaxBatchEvents, MaxPending: current.MaxPending, MaxDelay: current.MaxDelay, FlushTimeout: current.FlushTimeout}
	change(&cfg)
	s.gc.Close()
	s.initGroupCommit(cfg)
}
