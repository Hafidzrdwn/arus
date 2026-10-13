package ports

import "arus/internal/domain"

// EventPublisher defines the driven outbound port for broadcasting system and download lifecycle events.
// Implementations must be non-blocking or handle slow subscribers without deadlocking engine operations.
type EventPublisher interface {
	// Publish emits a domain event to all registered local or IPC listeners.
	Publish(ev domain.Event)
}
