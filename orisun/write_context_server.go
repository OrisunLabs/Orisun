//go:build !orisun_embedded

package orisun

import (
	"context"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
)

func (s *EventStore) GetWriteContext(ctx context.Context, req *GetWriteContextRequest) (*WriteContext, error) {
	if _, err := ValidateWriteContextRequest(req); err != nil {
		return nil, err
	}
	if err := s.RequireBoundaryActive(req.Boundary); err != nil {
		return nil, err
	}
	retriever, ok := s.getEventsFn.(WriteContextRetriever)
	if !ok {
		return nil, statuscode.New(statuscode.Unimplemented, "backend does not expose write contexts")
	}
	return retriever.GetWriteContext(ctx, req)
}

func (c *OrisunServer) GetWriteContext(ctx context.Context, req *GetWriteContextRequest) (*WriteContext, error) {
	return c.eventStore.GetWriteContext(ctx, req)
}
