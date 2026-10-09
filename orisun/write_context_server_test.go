//go:build !orisun_embedded

package orisun

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteIDSurvivesSubscriptionBackendRead(t *testing.T) {
	event := ReadEvent{EventId: "e", WriteId: "42:7", CommitPosition: 42, PreparePosition: 7, DateCreated: time.Now().UTC()}
	result := neutralSubscriptionReadEvent(event)
	require.Equal(t, "42:7", result.WriteID)
	require.Equal(t, int64(42), result.Position.CommitPosition)
}
