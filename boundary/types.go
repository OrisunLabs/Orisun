package boundary

import (
	"fmt"

	"github.com/OrisunLabs/Orisun/eventstore"
)

// Status describes the latest observed lifecycle state of a boundary.
type Status string

const (
	StatusProvisioning Status = "PROVISIONING"
	StatusActive       Status = "ACTIVE"
	StatusFailed       Status = "FAILED"
)

// Placement is the durable physical location recorded by a boundary
// definition event.
type Placement struct {
	Backend   string `json:"backend"`
	Namespace string `json:"namespace"`
}

// Definition is the immutable input shared by commands, embedded stores, and
// backend provisioners.
type Definition struct {
	Name        string
	Description string
	Placement   Placement
}

// Boundary is the event-rebuilt lifecycle representation.
type Boundary struct {
	Name        string
	Description string
	Placement   Placement
	Status      Status

	LastError          string
	DefinitionPosition *eventstore.Position
	StatusPosition     *eventstore.Position
}

// ValidateName applies the identifier contract shared by all storage
// backends.
func ValidateName(name string) error {
	if len(name) == 0 || len(name) > 63 {
		return fmt.Errorf("boundary name must be 1-63 characters, got %d", len(name))
	}
	if !letter(name[0]) && name[0] != '_' {
		return fmt.Errorf("boundary name must start with a letter or underscore, got %q", name[0])
	}
	for i := 1; i < len(name); i++ {
		character := name[i]
		if !letter(character) && !digit(character) && character != '_' {
			return fmt.Errorf("boundary name can only contain letters, digits, and underscores, invalid character %q at position %d", character, i)
		}
	}
	return nil
}

func letter(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
}

func digit(character byte) bool {
	return character >= '0' && character <= '9'
}
