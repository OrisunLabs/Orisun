package sqlite

import (
	"context"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// Save keeps backend tests concise while production accepts prepared batches
// exclusively through SavePrepared.
func (s *SqliteSaveEvents) Save(
	ctx context.Context,
	events []eventstore.EventWithMapTags,
	boundary string,
	observations []*eventstore.ConsistencyObservation,
) (string, int64, error) {
	prepared, err := eventstore.PrepareEventsForSave(events)
	if err != nil {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "invalid event data: %v", err)
	}
	consistency, err := eventstore.ConsistencyChecksFromObservations(observations)
	if err != nil {
		return "", 0, err
	}
	return s.SavePrepared(ctx, prepared, boundary, consistency)
}
