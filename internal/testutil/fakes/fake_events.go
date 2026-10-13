package fakes

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"sync"
)

var _ ports.EventPublisher = (*FakeEvents)(nil)

// FakeEvents is a thread-safe in-memory event publisher that records emitted domain events.
type FakeEvents struct {
	mu     sync.RWMutex
	events []domain.Event
}

// NewFakeEvents creates a new FakeEvents recorder.
func NewFakeEvents() *FakeEvents {
	return &FakeEvents{}
}

// Publish records the emitted domain event.
func (e *FakeEvents) Publish(ev domain.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, ev)
}

// Events returns a shallow copy of all recorded events in publication order.
func (e *FakeEvents) Events() []domain.Event {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]domain.Event, len(e.events))
	copy(out, e.events)
	return out
}

// Clear clears all previously recorded events.
func (e *FakeEvents) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = nil
}

// Count returns the number of recorded events.
func (e *FakeEvents) Count() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.events)
}
