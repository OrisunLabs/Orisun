package orisun

import (
	"context"
	"errors"
	"sync"
)

type UserContextKeyType string

const UserContextKey UserContextKeyType = "user"

type Role string

const (
	RoleAdmin      Role = "ADMIN"
	RoleOperations Role = "OPERATIONS"
)

var Roles = []Role{RoleAdmin, RoleOperations}

func (r Role) String() string {
	return string(r)
}

type User struct {
	Id             string `json:"id"`
	Name           string `json:"name"`
	Username       string `json:"username"`
	HashedPassword string `json:"password_hash"`
	Roles          []Role `json:"roles"`
}

type MessageHandler[T any] struct {
	ctx    context.Context
	events chan *T
	once   sync.Once
	closed bool
	mu     sync.RWMutex
}

var ErrQueueFull = errors.New("message handler queue full")

// NewMessageHandler Add constructor and methods for the generic stream
func NewMessageHandler[T any](ctx context.Context) *MessageHandler[T] {
	handler := &MessageHandler[T]{
		ctx:    ctx,
		events: make(chan *T, 100), // Buffer size of 100
	}
	// When context is cancelled, mark handler as closed without closing the events channel
	// to avoid potential send-on-closed-channel panic from concurrent senders.
	go func() {
		<-ctx.Done()
		handler.mu.Lock()
		if !handler.closed {
			handler.once.Do(func() {
				handler.closed = true
			})
		}
		handler.mu.Unlock()
	}()
	return handler
}

// NewMessageHandlerWithBuffer allows configuring the internal buffer size
func NewMessageHandlerWithBuffer[T any](ctx context.Context, bufferSize int) *MessageHandler[T] {
	if bufferSize <= 0 {
		bufferSize = 1
	}
	handler := &MessageHandler[T]{
		ctx:    ctx,
		events: make(chan *T, bufferSize),
	}
	go func() {
		<-ctx.Done()
		handler.mu.Lock()
		if !handler.closed {
			handler.once.Do(func() {
				handler.closed = true
			})
		}
		handler.mu.Unlock()
	}()
	return handler
}

func (s *MessageHandler[T]) Send(event *T) error {
	// Fast path: check context and closed flag
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return errors.New("message handler closed")
	}
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.events <- event:
		return nil
	}
}

// TrySend attempts to enqueue without blocking and returns ErrQueueFull if buffer is full
func (s *MessageHandler[T]) TrySend(event *T) error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return errors.New("message handler closed")
	}
	select {
	case s.events <- event:
		return nil
	default:
		return ErrQueueFull
	}
}

func (s *MessageHandler[T]) Recv() (*T, error) {
	// Wait for either an event or context cancellation
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case event, ok := <-s.events:
		if !ok {
			// Channel was closed
			return nil, context.Canceled
		}
		return event, nil
	}
}

// Close marks the handler as closed to prevent further sends
func (s *MessageHandler[T]) Close() {
	s.mu.Lock()
	if !s.closed {
		s.once.Do(func() {
			s.closed = true
		})
	}
	s.mu.Unlock()
}

func (s *MessageHandler[T]) Context() context.Context {
	return s.ctx
}
