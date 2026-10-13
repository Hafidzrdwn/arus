package engine_test

import (
	"arus/internal/engine"
	"arus/internal/testutil/fakes"
	"testing"
	"time"
)

func TestProgressTracker_EMA_ETA(t *testing.T) {
	fakeClock := fakes.NewFakeClock(time.Now())
	tracker := engine.NewProgressTracker(10*1024*1024, fakeClock) // 10 MiB

	if tracker.Total() != 10*1024*1024 {
		t.Errorf("Total = %d, want 10 MiB", tracker.Total())
	}
	if tracker.Downloaded() != 0 {
		t.Errorf("Downloaded = %d, want 0", tracker.Downloaded())
	}

	tracker.Add(1 * 1024 * 1024) // 1 MiB in 1 second
	fakeClock.Advance(1 * time.Second)
	tracker.Sample()

	speed1s := tracker.Speed1s()
	if speed1s <= 0 {
		t.Errorf("expected positive speed1s, got %d", speed1s)
	}

	snap := tracker.Snapshot()
	if snap.Downloaded != 1*1024*1024 {
		t.Errorf("snap.Downloaded = %d, want 1 MiB", snap.Downloaded)
	}
	if snap.Total != 10*1024*1024 {
		t.Errorf("snap.Total = %d, want 10 MiB", snap.Total)
	}
	if snap.Percentage < 9.9 || snap.Percentage > 10.1 {
		t.Errorf("snap.Percentage = %f, want ~10%%", snap.Percentage)
	}
	if snap.Eta <= 0 {
		t.Errorf("expected positive ETA, got %v", snap.Eta)
	}

	tracker.SetTotal(20 * 1024 * 1024)
	if tracker.Total() != 20*1024*1024 {
		t.Errorf("updated Total = %d, want 20 MiB", tracker.Total())
	}
}
