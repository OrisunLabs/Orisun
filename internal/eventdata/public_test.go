package eventdata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithoutStorageEnvelopePreservesNestedReservedFields(t *testing.T) {
	got := WithoutStorageEnvelope(`{
		"__eventType":"OrderPlaced",
		"orderId":"order-1",
		"nested":{"__eventType":"domain-value"}
	}`)
	assert.JSONEq(t, `{
		"orderId":"order-1",
		"nested":{"__eventType":"domain-value"}
	}`, got)
}

func TestWithoutStorageEnvelopePreservesDataThatNeedsNoTranslation(t *testing.T) {
	for _, encoded := range []string{
		`{ "orderId": "order-1" }`,
		`not-json`,
		``,
	} {
		if got := WithoutStorageEnvelope(encoded); got != encoded {
			t.Fatalf("WithoutStorageEnvelope(%q) = %q", encoded, got)
		}
	}
}

func TestWithoutStorageEnvelopeRemovesReservedNamespaceOnly(t *testing.T) {
	for _, tc := range []struct{ name, stored, want string }{
		{"future fields", `{"__":null,"___private":[],"__future":{"secret":true},"_single":1,"ordinary__key":2,"nested":{"__future":3},"items":[{"__future":4}],"number":9223372036854775807}`, `{"_single":1,"ordinary__key":2,"nested":{"__future":3},"items":[{"__future":4}],"number":9223372036854775807}`},
		{"all reserved", `{"__eventId":"id","__eventType":"Type","__future":true}`, `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := WithoutStorageEnvelope(tc.stored)
			assert.JSONEq(t, tc.want, got)
			if tc.name == "future fields" {
				assert.Contains(t, got, "9223372036854775807")
			}
		})
	}
}
