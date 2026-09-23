package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/lib/pq"
)

func (s *PostgresGetEvents) GetWriteContext(ctx context.Context, req *orisun.GetWriteContextRequest) (*orisun.WriteContext, error) {
	id, err := orisun.ValidateWriteContextRequest(req)
	if err != nil {
		return nil, err
	}
	entry, ok := s.registry.lookup(req.Boundary)
	if !ok {
		return nil, statuscode.Errorf(statuscode.InvalidArgument, "unknown boundary: %s", req.Boundary)
	}
	query := fmt.Sprintf("SELECT w.consistency FROM %s.%s w JOIN %s.%s e ON e.global_id = w.write_id WHERE w.write_id = $1 AND e.transaction_id = $2", pq.QuoteIdentifier(entry.mapping.Schema), pq.QuoteIdentifier(req.Boundary+"_orisun_es_write"), pq.QuoteIdentifier(entry.mapping.Schema), pq.QuoteIdentifier(req.Boundary+"_orisun_es_event"))
	var data []byte
	err = s.db.QueryRowContext(ctx, query, id.PreparePosition, id.CommitPosition).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, statuscode.New(statuscode.NotFound, "write context not found")
	}
	if err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "read write context: %v", err)
	}
	result, err := orisun.DecodeWriteContext(req.WriteId, data)
	if err != nil {
		return nil, statuscode.Errorf(statuscode.Internal, "%v", err)
	}
	return result, nil
}
