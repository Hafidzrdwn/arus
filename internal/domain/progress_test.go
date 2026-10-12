package domain_test

import (
	"arus/internal/domain"
	"testing"
	"time"
)

func TestCalculateETA(t *testing.T) {
	tests := []struct {
		name       string
		downloaded int64
		total      int64
		bps        int64
		expected   time.Duration
	}{
		{
			name:       "already completed",
			downloaded: 1000,
			total:      1000,
			bps:        500,
			expected:   0,
		},
		{
			name:       "indeterminate total size (-1)",
			downloaded: 1000,
			total:      -1,
			bps:        500,
			expected:   0,
		},
		{
			name:       "stalled download (bps = 0)",
			downloaded: 500,
			total:      1000,
			bps:        0,
			expected:   -1,
		},
		{
			name:       "negative bps",
			downloaded: 500,
			total:      1000,
			bps:        -100,
			expected:   -1,
		},
		{
			name:       "10 MB remaining at 2 MB/s -> 5s",
			downloaded: 10 * 1024 * 1024,
			total:      20 * 1024 * 1024,
			bps:        2 * 1024 * 1024,
			expected:   5 * time.Second,
		},
		{
			name:       "downloaded exceeds total",
			downloaded: 1500,
			total:      1000,
			bps:        100,
			expected:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := domain.CalculateETA(tc.downloaded, tc.total, tc.bps)
			if got != tc.expected {
				t.Errorf("CalculateETA(%d, %d, %d) = %v, want %v",
					tc.downloaded, tc.total, tc.bps, got, tc.expected)
			}
		})
	}
}

func TestNewProgressSnapshot(t *testing.T) {
	total := int64(100)
	downloaded := int64(50)
	bps := int64(25)

	snap := domain.NewProgressSnapshot(downloaded, total, bps)

	if snap.Downloaded != downloaded {
		t.Errorf("got Downloaded %d, want %d", snap.Downloaded, downloaded)
	}
	if snap.Total != total {
		t.Errorf("got Total %d, want %d", snap.Total, total)
	}
	if snap.Bps != bps {
		t.Errorf("got Bps %d, want %d", snap.Bps, bps)
	}
	if snap.Eta != 2*time.Second {
		t.Errorf("got ETA %v, want 2s", snap.Eta)
	}
	if snap.Percentage != 50.0 {
		t.Errorf("got Percentage %f, want 50.0", snap.Percentage)
	}

	// Capped at 100%
	overSnap := domain.NewProgressSnapshot(150, 100, 10)
	if overSnap.Percentage != 100.0 {
		t.Errorf("got Percentage %f, want 100.0", overSnap.Percentage)
	}
}
