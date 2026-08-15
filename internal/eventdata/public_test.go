package eventdata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithoutStorageEventTypeRemovesOnlyTopLevelDiscriminator(t *testing.T) {
	got := WithoutStorageEventType(`{
		"eventType":"OrderPlaced",
		"orderId":"order-1",
		"nested":{"eventType":"domain-value"}
	}`)
	assert.JSONEq(t, `{
		"orderId":"order-1",
		"nested":{"eventType":"domain-value"}
	}`, got)
}

func TestWithoutStorageEventTypePreservesDataThatNeedsNoTranslation(t *testing.T) {
	for _, encoded := range []string{
		`{ "orderId": "order-1" }`,
		`not-json`,
		``,
	} {
		if got := WithoutStorageEventType(encoded); got != encoded {
			t.Fatalf("WithoutStorageEventType(%q) = %q", encoded, got)
		}
	}
}
