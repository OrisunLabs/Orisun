package eventdata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrateLegacyEventType(t *testing.T) {
	got, err := MigrateLegacyEventType([]byte(`{"eventType":"eventType","id":9223372036854775807,"nested":{"eventType":"domain"}}`))
	require.NoError(t, err)
	require.Contains(t, string(got), `"__eventType":"eventType"`)
	require.Contains(t, string(got), `"id":9223372036854775807`)
	require.Contains(t, string(got), `"nested":{"eventType":"domain"}`)
	_, err = MigrateLegacyEventType([]byte(`{"eventType":"Old","__eventType":"UserValue"}`))
	require.ErrorContains(t, err, "conflicts")
}

func TestMigrateLegacyConsistency(t *testing.T) {
	got, err := MigrateLegacyConsistency([]byte(`[{"query":{"criteria":[{"eventType":"eventType","customer":"c1"},{"account":"a1"}]},"position":{"transaction_id":9223372036854775807,"global_id":9007199254740993}}]`))
	require.NoError(t, err)
	require.Contains(t, string(got), `"__eventType":"eventType"`)
	require.Contains(t, string(got), `"transaction_id":9223372036854775807`)
	require.Contains(t, string(got), `"global_id":9007199254740993`)
	require.Contains(t, string(got), `{"account":"a1"}`)
	got, err = MigrateLegacyConsistency([]byte(`[]`))
	require.NoError(t, err)
	require.Equal(t, `[]`, string(got))
}

func TestMigrateLegacyIndexKeysPreservesConditionValues(t *testing.T) {
	got, changed, err := MigrateLegacyIndexKeys([]byte(`[{"Key":"eventType","Operator":"=","Value":"eventType"}]`), "Key")
	require.NoError(t, err)
	require.True(t, changed)
	require.JSONEq(t, `[{"Key":"__eventType","Operator":"=","Value":"eventType"}]`, string(got))
}

func TestPublicDataPreservesApplicationEventType(t *testing.T) {
	got := WithoutStorageEnvelope(`{"__eventType":"StoredType","eventType":"DomainType","number":9223372036854775807}`)
	require.Equal(t, `{"eventType":"DomainType","number":9223372036854775807}`, got)
}
