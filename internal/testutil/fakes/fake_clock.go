package fakes

import (
	"arus/internal/app/ports"
	"sync"
	"time"
)

var (
	_ ports.Clock = (*FakeClock)(nil)
	_ ports.Timer = (*FakeTimer)(nil)
)

// FakeTimer is an in-memory controlled timer for tests.
type FakeTimer struct {
	mu       sync.Mutex
	clock    *FakeClock
	deadline time.Time
	ch       chan time.Time
	active   bool
}

// C returns the channel receiving the tick when the timer expires.
func (t *FakeTimer) C() <-chan time.Time {
	return t.ch
}

// Stop turns off the timer, returning true if the timer was active.
func (t *FakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	wasActive := t.active
	t.active = false
	return wasActive
}

// Reset changes the timer to expire after duration d relative to current clock time.
func (t *FakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	wasActive := t.active
	t.active = true
	t.deadline = t.clock.now.Add(d)

	found := false
	for _, cur := range t.clock.timers {
		if cur == t {
			found = true
			break
		}
	}
	if !found {
		t.clock.timers = append(t.clock.timers, t)
	}

	return wasActive
}

// FakeClock provides deterministic time control for testing.
type FakeClock struct {
	mu     sync.RWMutex
	now    time.Time
	timers []*FakeTimer
}

// NewFakeClock initializes a FakeClock set to the given start time.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{
		now: start,
	}
}

// Now returns the simulated current time.
func (c *FakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

// NewTimer creates a new timer expiring after d.
func (c *FakeClock) NewTimer(d time.Duration) ports.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	t := &FakeTimer{
		clock:    c,
		deadline: c.now.Add(d),
		ch:       make(chan time.Time, 1),
		active:   true,
	}
	c.timers = append(c.timers, t)
	return t
}

// Advance advances the clock by d and triggers any expired timers.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	currentTime := c.now

	for _, t := range c.timers {
		t.mu.Lock()
		if t.active && !currentTime.Before(t.deadline) {
			t.active = false
			select {
			case t.ch <- currentTime:
			default:
			}
		}
		t.mu.Unlock()
	}
	c.mu.Unlock()
}
