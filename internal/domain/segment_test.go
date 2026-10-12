package domain_test

import (
	"arus/internal/domain"
	"errors"
	"math/rand"
	"testing"
	"time"
)

func TestSegmentRemaining(t *testing.T) {
	s := domain.Segment{
		ID:     0,
		Start:  100,
		End:    500,
		Cursor: 300,
	}
	if got := s.Remaining(); got != 200 {
		t.Errorf("Remaining(): got %d, want 200", got)
	}

	// Done segment
	s.Cursor = 500
	if got := s.Remaining(); got != 0 {
		t.Errorf("Remaining(): got %d, want 0", got)
	}

	// Overflow cursor (safety)
	s.Cursor = 600
	if got := s.Remaining(); got != 0 {
		t.Errorf("Remaining(): got %d, want 0", got)
	}
}

func TestValidateInvariants_Valid(t *testing.T) {
	tests := []struct {
		name  string
		segs  []domain.Segment
		total int64
	}{
		{
			name:  "zero total empty segs",
			segs:  []domain.Segment{},
			total: 0,
		},
		{
			name: "zero total single zero seg",
			segs: []domain.Segment{
				{ID: 0, Start: 0, End: 0, Cursor: 0},
			},
			total: 0,
		},
		{
			name: "single segment complete",
			segs: []domain.Segment{
				{ID: 0, Start: 0, End: 1000, Cursor: 500},
			},
			total: 1000,
		},
		{
			name: "multiple contiguous segments",
			segs: []domain.Segment{
				{ID: 0, Start: 0, End: 4096, Cursor: 4096},
				{ID: 1, Start: 4096, End: 8192, Cursor: 6000},
				{ID: 2, Start: 8192, End: 12000, Cursor: 8192},
			},
			total: 12000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := domain.ValidateInvariants(tc.segs, tc.total); err != nil {
				t.Errorf("unexpected invariant error: %v", err)
			}
		})
	}
}

func TestValidateInvariants_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		segs  []domain.Segment
		total int64
	}{
		{
			name:  "negative total",
			segs:  []domain.Segment{},
			total: -1,
		},
		{
			name:  "empty segments for positive total",
			segs:  []domain.Segment{},
			total: 5000,
		},
		{
			name: "first segment does not start at 0",
			segs: []domain.Segment{
				{ID: 0, Start: 10, End: 1000, Cursor: 10},
			},
			total: 1000,
		},
		{
			name: "cursor before start",
			segs: []domain.Segment{
				{ID: 0, Start: 0, End: 1000, Cursor: -5},
			},
			total: 1000,
		},
		{
			name: "cursor after end",
			segs: []domain.Segment{
				{ID: 0, Start: 0, End: 1000, Cursor: 1005},
			},
			total: 1000,
		},
		{
			name: "start >= end in segment",
			segs: []domain.Segment{
				{ID: 0, Start: 100, End: 100, Cursor: 100},
			},
			total: 100,
		},
		{
			name: "gap between segments",
			segs: []domain.Segment{
				{ID: 0, Start: 0, End: 500, Cursor: 0},
				{ID: 1, Start: 600, End: 1000, Cursor: 600},
			},
			total: 1000,
		},
		{
			name: "overlap between segments",
			segs: []domain.Segment{
				{ID: 0, Start: 0, End: 600, Cursor: 0},
				{ID: 1, Start: 500, End: 1000, Cursor: 500},
			},
			total: 1000,
		},
		{
			name: "last segment does not reach total",
			segs: []domain.Segment{
				{ID: 0, Start: 0, End: 800, Cursor: 0},
			},
			total: 1000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := domain.ValidateInvariants(tc.segs, tc.total)
			if err == nil {
				t.Fatalf("expected invariant violation error for %q, got nil", tc.name)
			}
			if !errors.Is(err, domain.ErrInvariantViolation) {
				t.Errorf("expected error wrapping ErrInvariantViolation, got: %v", err)
			}
		})
	}
}

func TestPlanSegments_Basic(t *testing.T) {
	total := int64(10 * 1024 * 1024) // 10 MiB
	segs := domain.PlanSegments(total, 4, 1024*1024, 4096)

	if len(segs) != 4 {
		t.Fatalf("expected 4 segments, got %d", len(segs))
	}

	if err := domain.ValidateInvariants(segs, total); err != nil {
		t.Fatalf("planned segments failed invariant validation: %v", err)
	}

	// Verify alignment of intermediate boundaries
	for i := 0; i < len(segs)-1; i++ {
		if segs[i].End%4096 != 0 {
			t.Errorf("segment %d boundary %d is not 4096 aligned", i, segs[i].End)
		}
	}
}

// Property-based test verifying PlanSegments always satisfies ValidateInvariants across random inputs.
func TestPlanSegments_PropertyInvariant(t *testing.T) {
	//nolint:gosec // testing pseudo-randomness doesn't require crypto/rand
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 200; i++ {
		total := rng.Int63n(100*1024*1024) + 1 // 1 byte to 100 MiB
		n := rng.Int63n(16) + 1                // 1 to 16
		minSeg := (rng.Int63n(4) + 1) * 256 * 1024
		aligns := []int64{512, 1024, 2048, 4096, 8192}
		align := aligns[rng.Intn(len(aligns))]

		segs := domain.PlanSegments(total, n, minSeg, align)
		if err := domain.ValidateInvariants(segs, total); err != nil {
			t.Fatalf("PlanSegments(total=%d, n=%d, minSeg=%d, align=%d) failed invariants: %v",
				total, n, minSeg, align, err)
		}
	}
}

// Fuzz test for PlanSegments
func FuzzPlanSegments(f *testing.F) {
	f.Add(int64(10485760), int64(4), int64(1048576), int64(4096))
	f.Add(int64(500), int64(2), int64(100), int64(64))
	f.Add(int64(0), int64(1), int64(1000), int64(512))

	f.Fuzz(func(t *testing.T, total, n, minSeg, align int64) {
		if total < 0 || total > 1000000000 {
			return
		}
		if n < 0 || n > 64 {
			return
		}
		if minSeg < 0 || minSeg > 50000000 {
			return
		}
		if align < 0 || align > 65536 {
			return
		}

		segs := domain.PlanSegments(total, n, minSeg, align)
		if total > 0 {
			if err := domain.ValidateInvariants(segs, total); err != nil {
				t.Fatalf("PlanSegments invariant violation for total=%d, n=%d, minSeg=%d, align=%d: %v",
					total, n, minSeg, align, err)
			}
		}
	})
}

func TestComputeSplitPoint_PropertySafety(t *testing.T) {
	//nolint:gosec // testing pseudo-randomness doesn't require crypto/rand
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 500; i++ {
		start := rng.Int63n(10 * 1024 * 1024)
		length := rng.Int63n(50 * 1024 * 1024)
		end := start + length
		cursor := start + rng.Int63n(length+1)

		seg := domain.Segment{
			ID:     1,
			Start:  start,
			End:    end,
			Cursor: cursor,
		}

		minSplit := (rng.Int63n(4) + 1) * 512 * 1024
		safetyMargin := (rng.Int63n(3) + 1) * 256 * 1024
		align := int64(4096)

		mid, ok := domain.ComputeSplitPoint(seg, minSplit, safetyMargin, align)
		if ok {
			// Acceptance Criteria: mid must never be <= cursor+safetyMargin or >= end
			if mid <= cursor+safetyMargin {
				t.Fatalf("ComputeSplitPoint produced mid=%d <= cursor+margin (%d + %d)",
					mid, cursor, safetyMargin)
			}
			if mid >= end {
				t.Fatalf("ComputeSplitPoint produced mid=%d >= end (%d)", mid, end)
			}
			if mid%align != 0 {
				t.Fatalf("ComputeSplitPoint produced non-aligned mid=%d", mid)
			}
			if mid-cursor < minSplit {
				t.Fatalf("First half remaining %d < minSplit %d", mid-cursor, minSplit)
			}
			if end-mid < minSplit {
				t.Fatalf("Second half remaining %d < minSplit %d", end-mid, minSplit)
			}
		}
	}
}

// Fuzz test for ComputeSplitPoint verifying safety invariant
func FuzzComputeSplitPoint(f *testing.F) {
	f.Add(int64(0), int64(10485760), int64(0), int64(1048576), int64(524288), int64(4096))
	f.Add(int64(1000), int64(20000000), int64(5000000), int64(2097152), int64(524288), int64(4096))

	f.Fuzz(func(t *testing.T, start, end, cursor, minSplit, safetyMargin, align int64) {
		if start < 0 || end <= start || cursor < start || cursor > end {
			return
		}
		if minSplit <= 0 || safetyMargin <= 0 || align <= 0 {
			return
		}

		seg := domain.Segment{
			ID:     0,
			Start:  start,
			End:    end,
			Cursor: cursor,
		}

		mid, ok := domain.ComputeSplitPoint(seg, minSplit, safetyMargin, align)
		if ok {
			if mid <= cursor+safetyMargin {
				t.Fatalf("mid %d <= cursor+safetyMargin %d", mid, cursor+safetyMargin)
			}
			if mid >= end {
				t.Fatalf("mid %d >= end %d", mid, end)
			}
		}
	})
}
