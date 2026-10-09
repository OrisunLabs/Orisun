package events

import boundarymodel "github.com/OrisunLabs/Orisun/boundary"

const (
	EventTypeBoundaryCreated   = "$BoundaryCreated"
	EventTypeBoundaryActivated = "$BoundaryActivated"
	EventTypeBoundaryFailed    = "$BoundaryProvisioningFailed"
)

// BoundaryCreated records the immutable boundary definition.
type BoundaryCreated struct {
	Boundary    string                  `json:"boundary"`
	Description string                  `json:"description,omitempty"`
	Placement   boundarymodel.Placement `json:"placement"`
}

// BoundaryActivated records successful physical and runtime provisioning.
type BoundaryActivated struct {
	Boundary string `json:"boundary"`
}

// BoundaryProvisioningFailed records the latest provisioning failure.
type BoundaryProvisioningFailed struct {
	Boundary string `json:"boundary"`
	Error    string `json:"error"`
}
