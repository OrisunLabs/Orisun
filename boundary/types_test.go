package boundary

import (
	"strings"
	"testing"

	"github.com/OrisunLabs/Orisun/eventstore"
)

func TestValidateName(t *testing.T) {
	tests := []struct {
		label string
		value string
		valid bool
	}{
		{label: "lowercase", value: "orders", valid: true},
		{label: "leading underscore", value: "_admin_2", valid: true},
		{label: "uppercase", value: "Orders", valid: true},
		{label: "empty", value: "", valid: false},
		{label: "leading digit", value: "2orders", valid: false},
		{label: "hyphen", value: "order-items", valid: false},
		{label: "too long", value: strings.Repeat("a", 64), valid: false},
	}
	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			if got := ValidateName(test.value); (got == nil) != test.valid {
				t.Fatalf("ValidateName(%q) = %v, valid=%t", test.value, got, test.valid)
			}
		})
	}
}

func TestBoundaryUsesNeutralPositions(t *testing.T) {
	position := eventstore.Position{CommitPosition: 7, PreparePosition: 8}
	value := Boundary{
		Name:   "orders",
		Status: StatusActive,

		DefinitionPosition: &position,
		StatusPosition:     &position,
	}
	if value.DefinitionPosition != &position || value.StatusPosition != &position {
		t.Fatalf("unexpected boundary positions: %#v", value)
	}
}
