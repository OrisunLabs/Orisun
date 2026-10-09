package events

import (
	"github.com/OrisunLabs/Orisun/orisun"
)

const (
	AdminStream = "OrisunAdmin"
)

// Event types
const (
	EventTypeUserCreated         = "$UserCreated"
	EventTypeUserDeleted         = "$UserDeleted"
	EventTypeRolesChanged        = "$RolesChanged"
	EventTypeUserPasswordChanged = "$UserPasswordChanged"
)

type UserCreated struct {
	Name         string        `json:"name"`
	UserId       string        `json:"user_id"`
	Username     string        `json:"username"`
	Roles        []orisun.Role `json:"roles,omitempty"`
	PasswordHash string        `json:"password_hash,omitempty"`
}

type UserDeleted struct {
	UserId string `json:"user_id"`
}

type UserPasswordChanged struct {
	UserId       string `json:"user_id"`
	PasswordHash string `json:"password_hash,omitempty"`
}
