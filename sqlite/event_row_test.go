package sqlite

import (
	"testing"

	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/stretchr/testify/require"
)

func TestPrepareEventRowKeepsApplicationBytes(t *testing.T) {
	data := `{"amount":9007199254740993,"ratio":0.123456789012345678901}`
	row := prepareEventRow(orisun.PreparedEvent{EventId: "id", EventType: "A", DataJSON: data, MetadataJSON: ` { "source" : "api" } `})
	require.NoError(t, row.err)
	require.Equal(t, data, row.data)
	require.Equal(t, `{"source":"api"}`, row.metadata)
}

func TestPrepareEventRowDefersValidationErrors(t *testing.T) {
	for name, event := range map[string]orisun.PreparedEvent{
		"invalid data":     {DataJSON: "invalid", MetadataJSON: "{}"},
		"non-object data":  {DataJSON: "[]", MetadataJSON: "{}"},
		"reserved key":     {DataJSON: `{"__eventId":"spoof"}`, MetadataJSON: "{}"},
		"invalid metadata": {DataJSON: "{}", MetadataJSON: "{invalid"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, prepareEventRow(event).err)
		})
	}
	require.Equal(t, "null", prepareEventRow(orisun.PreparedEvent{DataJSON: "{}"}).metadata)
}
