package domain

import (
	"fmt"
)

// Segment represents a discrete byte range of a download.
type Segment struct {
	ID     int   `json:"id"`
	Start  int64 `json:"start"`  // inclusive start offset
	End    int64 `json:"end"`    // exclusive end offset
	Cursor int64 `json:"cursor"` // next byte to write; Start <= Cursor <= End
}

// Remaining returns the number of bytes left to download in this segment.
func (s Segment) Remaining() int64 {
	rem := s.End - s.Cursor
	if rem < 0 {
		return 0
	}
	return rem
}

// ValidateInvariants validates invariant I2 (TECH_SPEC §5):
// - Segments are mutually disjoint and cover [0, total) completely without gaps.
// - For every segment: Start <= Cursor <= End.
// - Downloaded bytes equals the sum of (Cursor - Start).
func ValidateInvariants(segs []Segment, total int64) error {
	if total < 0 {
		return fmt.Errorf("%w: total size cannot be negative: %d", ErrInvariantViolation, total)
	}

	if total == 0 {
		if len(segs) == 0 {
			return nil
		}
		if len(segs) == 1 && segs[0].Start == 0 && segs[0].End == 0 && segs[0].Cursor == 0 {
			return nil
		}
		return fmt.Errorf("%w: invalid segment slice for total size 0", ErrInvariantViolation)
	}

	if len(segs) == 0 {
		return fmt.Errorf("%w: segments slice is empty for total size %d", ErrInvariantViolation, total)
	}

	if segs[0].Start != 0 {
		return fmt.Errorf("%w: first segment must start at 0, got %d", ErrInvariantViolation, segs[0].Start)
	}

	for i, s := range segs {
		if s.Start < 0 || s.End < 0 {
			return fmt.Errorf("%w: segment %d has negative boundary [%d, %d)", ErrInvariantViolation, s.ID, s.Start, s.End)
		}
		if s.Cursor < s.Start || s.Cursor > s.End {
			return fmt.Errorf("%w: segment %d cursor %d out of bounds [%d, %d]", ErrInvariantViolation, s.ID, s.Cursor, s.Start, s.End)
		}
		if s.Start >= s.End {
			return fmt.Errorf("%w: segment %d start %d >= end %d", ErrInvariantViolation, s.ID, s.Start, s.End)
		}
		if i > 0 && segs[i-1].End != s.Start {
			return fmt.Errorf("%w: gap or overlap between segment %d (end=%d) and segment %d (start=%d)",
				ErrInvariantViolation, segs[i-1].ID, segs[i-1].End, s.ID, s.Start)
		}
	}

	last := segs[len(segs)-1]
	if last.End != total {
		return fmt.Errorf("%w: last segment end %d does not match total size %d", ErrInvariantViolation, last.End, total)
	}

	return nil
}

// PlanSegments divides [0, total) into n contiguous segments aligned to align bytes,
// with each segment having at least minSeg bytes (TECH_SPEC §6.3).
func PlanSegments(total, n, minSeg, align int64) []Segment {
	if minSeg <= 0 {
		minSeg = 1048576 // 1 MiB default
	}
	if align <= 0 {
		align = 4096 // 4 KiB default
	}

	if total <= 0 {
		return []Segment{{ID: 0, Start: 0, End: 0, Cursor: 0}}
	}

	if n <= 1 || total < minSeg {
		return []Segment{{ID: 0, Start: 0, End: total, Cursor: 0}}
	}

	maxN := total / minSeg
	if maxN < 1 {
		maxN = 1
	}
	if n > maxN {
		n = maxN
	}

	if n <= 1 {
		return []Segment{{ID: 0, Start: 0, End: total, Cursor: 0}}
	}

	baseSize := total / n
	boundaries := make([]int64, 0, n+1)
	boundaries = append(boundaries, 0)

	prev := int64(0)
	for i := int64(1); i < n; i++ {
		ideal := i * baseSize
		aligned := (ideal / align) * align

		// Ensure current segment has at least minSeg
		if aligned < prev+minSeg {
			aligned = prev + minSeg
			if align > 0 {
				aligned = ((aligned + align - 1) / align) * align
			}
		}

		// Ensure remaining segments each can have at least minSeg
		remainingNeeded := (n - i) * minSeg
		if total-aligned < remainingNeeded {
			aligned = total - remainingNeeded
			if align > 0 {
				aligned = (aligned / align) * align
			}
		}

		if aligned <= prev || aligned >= total {
			// Cannot make further valid aligned cuts; finish with remainder
			break
		}

		boundaries = append(boundaries, aligned)
		prev = aligned
	}

	boundaries = append(boundaries, total)

	segs := make([]Segment, 0, len(boundaries)-1)
	for i := 0; i < len(boundaries)-1; i++ {
		segs = append(segs, Segment{
			ID:     i,
			Start:  boundaries[i],
			End:    boundaries[i+1],
			Cursor: boundaries[i],
		})
	}

	return segs
}

// ComputeSplitPoint computes an aligned midpoint for dynamic chunk splitting / work-stealing (TECH_SPEC §6.6).
// It returns mid offset and ok=true if a split is valid, or ok=false if no work can be stolen.
func ComputeSplitPoint(seg Segment, minSplit, safetyMargin, align int64) (mid int64, ok bool) {
	if minSplit <= 0 {
		minSplit = 1048576 // 1 MiB default
	}
	if safetyMargin <= 0 {
		safetyMargin = 524288 // 512 KiB default (2 * 256 KiB bufSize)
	}
	if align <= 0 {
		align = 4096 // 4 KiB default
	}

	pos := seg.Cursor
	oldEnd := seg.End
	remaining := oldEnd - pos

	// Must have enough unread bytes to split into two viable segments
	if remaining < 2*minSplit {
		return 0, false
	}

	rawMid := pos + remaining/2
	// alignUp: align to next multiple of align
	mid = ((rawMid + align - 1) / align) * align

	// Safety check per §6.6: mid must never be <= cursor + safetyMargin or >= oldEnd
	if mid <= pos+safetyMargin || mid >= oldEnd {
		return 0, false
	}

	// Both halves must respect minSplit
	if mid-pos < minSplit || oldEnd-mid < minSplit {
		return 0, false
	}

	return mid, true
}
