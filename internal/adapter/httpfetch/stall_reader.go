package httpfetch

import (
	"errors"
	"io"
	"sync"
	"time"
)

// ErrStallTimeout indicates that the read operation stalled without returning bytes within timeout.
var ErrStallTimeout = errors.New("stream stalled: no bytes received within timeout")

// StallReader wraps an io.ReadCloser with a watchdog timer that terminates the stream if
// a read operation blocks longer than the specified stall timeout.
type StallReader struct {
	rc      io.ReadCloser
	timeout time.Duration

	mu      sync.Mutex
	timer   *time.Timer
	stalled bool
	closed  bool
}

// NewStallReader wraps rc with watchdog stall protection.
// If timeout <= 0, watchdog protection is disabled.
func NewStallReader(rc io.ReadCloser, timeout time.Duration) *StallReader {
	return &StallReader{
		rc:      rc,
		timeout: timeout,
	}
}

// Read reads from the underlying stream while enforcing the stall timeout.
func (s *StallReader) Read(p []byte) (int, error) {
	s.mu.Lock()
	if s.stalled {
		s.mu.Unlock()
		return 0, ErrStallTimeout
	}
	if s.closed {
		s.mu.Unlock()
		return 0, io.ErrClosedPipe
	}

	if s.timeout > 0 {
		if s.timer != nil {
			s.timer.Stop()
		}
		s.timer = time.AfterFunc(s.timeout, s.onStall)
	}
	s.mu.Unlock()

	n, err := s.rc.Read(p)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.timer != nil {
		s.timer.Stop()
	}

	if s.stalled {
		return 0, ErrStallTimeout
	}

	return n, err
}

func (s *StallReader) onStall() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.stalled {
		return
	}
	s.stalled = true
	_ = s.rc.Close()
}

// Close gracefully terminates the watchdog timer and the underlying stream.
func (s *StallReader) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	if s.timer != nil {
		s.timer.Stop()
	}

	return s.rc.Close()
}
