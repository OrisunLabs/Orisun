package orisun

import (
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadEventMarshalJSONPreservesPublishedEnvelope(t *testing.T) {
	event := ReadEvent{
		EventId:         "event-1",
		EventType:       "AccountCredited",
		Data:            `{"eventType":"AccountCredited","amount":10}`,
		Metadata:        `{"trace":"abc"}`,
		CommitPosition:  42,
		PreparePosition: 7,
		DateCreated:     time.Unix(1_700_000_000, 123_000_000).UTC(),
	}

	packedJSON, err := json.Marshal(event)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"event_id":"event-1",
		"event_type":"AccountCredited",
		"data":"{\"eventType\":\"AccountCredited\",\"amount\":10}",
		"metadata":"{\"trace\":\"abc\"}",
		"position":{"commit_position":42,"prepare_position":7},
		"date_created":{"seconds":1700000000,"nanos":123000000}
	}`, string(packedJSON))
}

func TestReadEventMarshalJSONProducesValidEscapedJSON(t *testing.T) {
	events := []ReadEvent{
		{
			EventId:     "<event>&\"\n",
			EventType:   "Created\u2028Again",
			Data:        string([]byte{'{', 0xff, '}'}),
			DateCreated: time.Unix(0, 0).UTC(),
		},
		{},
	}
	for _, event := range events {
		packedJSON, err := json.Marshal(event)
		require.NoError(t, err)
		assert.True(t, json.Valid(packedJSON), string(packedJSON))
	}
}

func TestReadEventBatchResponsePreservesRows(t *testing.T) {
	created := time.Unix(1_700_000_000, 321).UTC()
	batch := ReadEventBatch{
		{
			EventId:         "event-1",
			EventType:       "Opened",
			Data:            `{"eventType":"Opened"}`,
			Metadata:        `{}`,
			CommitPosition:  4,
			PreparePosition: 9,
			DateCreated:     created,
		},
		{
			EventId:         "event-2",
			EventType:       "Closed",
			Data:            `{"eventType":"Closed"}`,
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
		Data:      `{"eventType":"AccountCredited","accountId":"account-1"}`,
	}

	event := read.Event()
	require.NotNil(t, event)
	assert.Equal(t, "AccountCredited", event.EventType)
	assert.JSONEq(t, `{"accountId":"account-1"}`, event.Data)
	assert.JSONEq(t, `{"eventType":"AccountCredited","accountId":"account-1"}`, read.Data)
}

func BenchmarkPublisherEventMarshal(b *testing.B) {
	event := ReadEvent{
		EventId:         "event-1",
		EventType:       "AccountCredited",
		Data:            `{"eventType":"AccountCredited","account_id":"account-1","amount":10}`,
		Metadata:        `{"trace_id":"trace-1"}`,
		CommitPosition:  42,
		PreparePosition: 7,
		DateCreated:     time.Unix(1_700_000_000, 123_000_000).UTC(),
	}
	materializedEvent := event.Event()

	b.Run("packed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := event.MarshalJSON(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("materialized", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := json.Marshal(materializedEvent); err != nil {
				b.Fatal(err)
			}
		}
	})
}
