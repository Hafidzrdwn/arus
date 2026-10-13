package engine

import (
	"arus/internal/domain"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	// MinSplit is the minimum segment size for work-stealing splits (TECH_SPEC §6.6).
	MinSplit = 1048576
	// SafetyMargin ensures a victim worker does not write past mid (TECH_SPEC §6.6).
	SafetyMargin = 524288
	// SplitAlign defines the byte alignment boundary for splits.
	SplitAlign = 4096

	minSplit     = MinSplit
	safetyMargin = SafetyMargin
	splitAlign   = SplitAlign
)

// Checkpointer defines the contract for notifying dirty state to persistence.
type Checkpointer interface {
	MarkDirty()
}

// Registry coordinates dynamic segment state, work-stealing, and orphan queues.
type Registry struct {
	mu           sync.RWMutex
	notifyCh     chan struct{}
	segments     []*SegState
	orphans      []*SegState
	checkpointer Checkpointer
	nextSegID    atomic.Int64
	splitCount   atomic.Int64
	disabled     bool
}

// NewRegistry constructs an initialized Registry for planned segments.
func NewRegistry(initial []*SegState, cp Checkpointer) *Registry {
	r := &Registry{
		notifyCh:     make(chan struct{}, 16),
		segments:     make([]*SegState, len(initial)),
		orphans:      make([]*SegState, len(initial)),
		checkpointer: cp,
	}
	copy(r.segments, initial)
	copy(r.orphans, initial)
	r.nextSegID.Store(int64(len(initial)))
	return r
}

// Claim assigns an orphan segment or steals work from a busy segment.
func (r *Registry) Claim(workerID string) (*SegState, bool) {
	r.mu.Lock()
	for len(r.orphans) > 0 {
		orphan := r.orphans[0]
		r.orphans = r.orphans[1:]
		if orphan.IsComplete() {
			continue
		}
		orphan.active.Store(true)
		r.mu.Unlock()
		return orphan, true
	}
	r.mu.Unlock()

	return r.steal(workerID)
}

func (r *Registry) steal(_ string) (*SegState, bool) {
	for {
		r.mu.Lock()
		if r.disabled {
			r.mu.Unlock()
			return nil, false
		}

		type candidate struct {
			victim    *SegState
			remaining int64
		}

		var candidates []candidate
		for _, s := range r.segments {
			if !s.active.Load() {
				continue
			}
			rem := s.End.Load() - s.Cursor.Load()
			if rem >= 2*minSplit {
				candidates = append(candidates, candidate{victim: s, remaining: rem})
			}
		}

		if len(candidates) == 0 {
			r.mu.Unlock()
			return nil, false
		}

		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].remaining > candidates[j].remaining
		})

		var stolen *SegState
		casRetried := false

		for _, c := range candidates {
			victim := c.victim
			pos := victim.Cursor.Load()
			oldEnd := victim.End.Load()
			rem := oldEnd - pos
			if rem < 2*minSplit {
				continue
			}

			segView := domain.Segment{
				Start:  victim.Start,
				End:    oldEnd,
				Cursor: pos,
			}

			mid, ok := domain.ComputeSplitPoint(segView, minSplit, safetyMargin, splitAlign)
			if !ok {
				continue
			}

			if victim.Cursor.Load() > mid-safetyMargin {
				continue
			}

			if !victim.End.CompareAndSwap(oldEnd, mid) {
				casRetried = true
				break
			}

			nextID := r.nextSegID.Add(1) - 1
			newSeg := NewSegState(fmt.Sprintf("seg-%d", nextID), mid, oldEnd, mid)
			newSeg.active.Store(true)
			r.segments = append(r.segments, newSeg)
			r.splitCount.Add(1)
			stolen = newSeg
			break
		}

		if casRetried {
			r.mu.Unlock()
			continue
		}

		cp := r.checkpointer
		r.mu.Unlock()

		if stolen != nil {
			if cp != nil {
				cp.MarkDirty()
			}
			return stolen, true
		}

		return nil, false
	}
}

func (r *Registry) notify() {
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}

// NotifyChan returns a channel signaled when segments are orphaned or released.
func (r *Registry) NotifyChan() <-chan struct{} {
	return r.notifyCh
}

// IsComplete returns true if all segments in the registry have been fully downloaded.
func (r *Registry) IsComplete() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, s := range r.segments {
		if !s.IsComplete() {
			return false
		}
	}
	return true
}

// Orphan returns an unfinished segment to the orphan queue.
func (r *Registry) Orphan(seg *SegState) {
	if seg == nil || seg.IsComplete() {
		return
	}
	seg.active.Store(false)
	r.mu.Lock()
	r.orphans = append(r.orphans, seg)
	cp := r.checkpointer
	r.mu.Unlock()

	r.notify()

	if cp != nil {
		cp.MarkDirty()
	}
}

// Release marks a completed segment as no longer active.
func (r *Registry) Release(seg *SegState) {
	if seg != nil {
		seg.active.Store(false)
	}
	r.notify()
}

// Snapshot returns an immutable point-in-time slice of contiguous segments.
func (r *Registry) Snapshot() []domain.Segment {
	r.mu.RLock()
	defer r.mu.RUnlock()

	segs := make([]domain.Segment, len(r.segments))
	for i, s := range r.segments {
		segs[i] = domain.Segment{
			ID:     i,
			Start:  s.Start,
			End:    s.End.Load(),
			Cursor: s.Cursor.Load(),
		}
	}
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].Start < segs[j].Start
	})
	for i := range segs {
		segs[i].ID = i
	}
	return segs
}

// SplitCount returns the total number of dynamic splits executed.
func (r *Registry) SplitCount() int64 {
	return r.splitCount.Load()
}

// SetDisabled enables or disables dynamic chunk splitting.
func (r *Registry) SetDisabled(disabled bool) {
	r.mu.Lock()
	r.disabled = disabled
	r.mu.Unlock()
}
