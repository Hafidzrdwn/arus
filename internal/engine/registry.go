package engine

import "sync/atomic"

// SegState tracks the execution state and atomic boundaries of a download segment.
type SegState struct {
	ID     string
	Start  int64
	End    atomic.Int64
	Cursor atomic.Int64
}

// NewSegState constructs an initialized SegState.
func NewSegState(id string, start, end, cursor int64) *SegState {
	s := &SegState{
		ID:    id,
		Start: start,
	}
	s.End.Store(end)
	s.Cursor.Store(cursor)
	return s
}

// Remaining returns the number of unwritten bytes in the segment.
func (s *SegState) Remaining() int64 {
	rem := s.End.Load() - s.Cursor.Load()
	if rem < 0 {
		return 0
	}
	return rem
}

// IsComplete returns true when the segment cursor reaches or passes the end offset.
func (s *SegState) IsComplete() bool {
	return s.Cursor.Load() >= s.End.Load()
}
