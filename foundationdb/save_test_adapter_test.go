//go:build foundationdb

package foundationdb

import (
	"context"

	"github.com/OrisunLabs/Orisun/internal/statuscode"
	eventstore "github.com/OrisunLabs/Orisun/orisun"
)

// Save keeps backend tests concise while production accepts prepared batches
// exclusively through SavePrepared.
func (b *Backend) Save(
	ctx context.Context,
	events []eventstore.EventWithMapTags,
	boundary string,
	expectedPosition *eventstore.Position,
	query *eventstore.Query,
) (string, int64, error) {
	prepared, err := eventstore.PrepareEventsForSave(events)
	if err != nil {
		return "", 0, statuscode.Errorf(statuscode.InvalidArgument, "invalid event data: %v", err)
	}
	consistency, err := eventstore.LegacyConsistencyChecks(expectedPosition, query)
	if err != nil {
		return "", 0, err
	}
	return b.SavePrepared(ctx, prepared, boundary, consistency)
}
