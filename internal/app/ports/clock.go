package ports

import "time"

// Timer wraps an active timer channel and control methods.
type Timer interface {
	// C returns the channel on which the ticks are delivered.
	C() <-chan time.Time
	// Stop turns off the timer, returning true if the call stops the timer,
	// or false if the timer has already expired or been stopped.
	Stop() bool
	// Reset changes the timer to expire after duration d.
	Reset(d time.Duration) bool
}

// Clock provides an abstraction over system time for deterministic testing.
type Clock interface {
	// Now returns the current instant in time.
	Now() time.Time
	// NewTimer creates a new Timer that will send the current time on its channel after duration d.
	NewTimer(d time.Duration) Timer
}
