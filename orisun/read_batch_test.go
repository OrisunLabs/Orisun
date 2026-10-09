package orisun

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadEventBatchResponsePreservesRows(t *testing.T) {
	created := time.Unix(1_700_000_000, 321).UTC()
	batch := ReadEventBatch{
		{
			EventId:         "event-1",
			EventType:       "Opened",
			Data:            `{}`,
			Metadata:        `{}`,
			CommitPosition:  4,
			PreparePosition: 9,
			DateCreated:     created,
		},
		{
			EventId:         "event-2",
			EventType:       "Closed",
			Data:            `{}`,
			Metadata:        `{"reason":"done"}`,
			CommitPosition:  5,
			PreparePosition: 10,
			DateCreated:     created.Add(time.Second),
		},
	}

	resp := batch.Response()
	require.Len(t, resp.Events, 2)
	assert.Equal(t, "event-1", resp.Events[0].EventId)
	assert.Equal(t, int64(4), resp.Events[0].Position.CommitPosition)
	assert.Equal(t, int64(9), resp.Events[0].Position.PreparePosition)
	assert.Equal(t, created, resp.Events[0].DateCreated)
	assert.JSONEq(t, `{}`, resp.Events[0].Data)
	assert.Equal(t, `{"reason":"done"}`, resp.Events[1].Metadata)
	assert.JSONEq(t, `{}`, resp.Events[1].Data)
	assert.Equal(t, int64(10), resp.Events[1].Position.PreparePosition)
}

func TestReadEventMaterializationExcludesStorageEventType(t *testing.T) {
	read := ReadEvent{
		EventId:   "event-1",
		EventType: "AccountCredited",
		Data:      `{"accountId":"account-1"}`,
	}

	event := read.Event()
	require.NotNil(t, event)
	assert.Equal(t, "AccountCredited", event.EventType)
	assert.JSONEq(t, `{"accountId":"account-1"}`, event.Data)
	assert.JSONEq(t, `{"accountId":"account-1"}`, read.Data)
}
