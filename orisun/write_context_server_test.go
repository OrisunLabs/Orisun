//go:build !orisun_embedded

package orisun

import (
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

func TestWriteIDSurvivesLiveAndCatchUpEnvelopes(t *testing.T) {
	event := ReadEvent{EventId: "e", WriteId: "42:7", CommitPosition: 42, PreparePosition: 7, DateCreated: time.Now().UTC()}
	payload, err := json.Marshal(event)
	require.NoError(t, err)
	var envelope publishedEventEnvelope
	require.NoError(t, json.Unmarshal(payload, &envelope))
	live := neutralPublishedEvent(envelope.event())
	catchUp := neutralSubscriptionReadEvent(event)
	require.Equal(t, "42:7", live.WriteID)
	require.Equal(t, catchUp.WriteID, live.WriteID)
}
