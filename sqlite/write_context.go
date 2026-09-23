package sqlite

import (
	"context"
	"strings"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/orisun"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func insertWriteContext(conn *sqlite.Conn, id int64, consistencyJSON string) error {
	return sqlitex.Execute(conn, "INSERT INTO orisun_es_write(write_id, consistency) VALUES (?, ?)", &sqlitex.ExecOptions{Args: []any{id, consistencyJSON}})
}

const sqliteMaxWriteContextsPerInsert = sqliteMaxInsertParams / 2

// insertWriteContextBatch writes only accepted requests. All chunks and their
// events share the caller's transaction, including rollback on any later failure.
func insertWriteContextBatch(conn *sqlite.Conn, accepted []acceptedSave) error {
	for start := 0; start < len(accepted); start += sqliteMaxWriteContextsPerInsert {
		chunk := accepted[start:min(start+sqliteMaxWriteContextsPerInsert, len(accepted))]
		var query strings.Builder
		query.Grow(80 + len(chunk)*8)
		query.WriteString("INSERT INTO orisun_es_write(write_id, consistency) VALUES ")
		args := make([]any, 0, len(chunk)*2)
		for i, saved := range chunk {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString("(?, ?)")
			args = append(args, saved.globalID, saved.req.consistencyJSON)
		}
		if err := sqlitex.Execute(conn, query.String(), &sqlitex.ExecOptions{Args: args}); err != nil {
			return err
		}
	}
	return nil
}

func (s *SqliteGetEvents) GetWriteContext(ctx context.Context, req *orisun.GetWriteContextRequest) (*orisun.WriteContext, error) {
	id, err := orisun.ValidateWriteContextRequest(req)
	if err != nil {
		return nil, err
	}
	pool, ok := s.registry.eventPool(req.Boundary)
	if !ok {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "unknown boundary: %s", req.Boundary)
	}
	conn, err := pool.Read.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer pool.Read.Put(conn)
	var result *orisun.WriteContext
	err = sqlitex.Execute(conn, "SELECT consistency FROM orisun_es_write w JOIN orisun_es_event e ON e.global_id = w.write_id WHERE w.write_id = ? AND e.transaction_id = ?", &sqlitex.ExecOptions{
		Args: []any{id.PreparePosition, id.CommitPosition},
		ResultFunc: func(stmt *sqlite.Stmt) error {
			var decodeErr error
			result, decodeErr = orisun.DecodeWriteContext(req.WriteId, []byte(stmt.ColumnText(0)))
			return decodeErr
		},
	})
	if err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "read write context: %v", err)
	}
	if result == nil {
		return nil, statuscode.New(statuscode.NotFound, "write context not found")
	}
	return result, nil
}
