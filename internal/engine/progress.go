package engine

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ProgressTracker aggregates downloaded bytes and computes Exponential Moving Average (EMA) speeds.
type ProgressTracker struct {
	downloaded atomic.Int64
	total      atomic.Int64
	clock      ports.Clock

	mu             sync.RWMutex
	lastSampleTime time.Time
	lastBytes      int64
	ema1s          float64
	ema5s          float64
	initialized    bool
}

// NewProgressTracker creates an initialized ProgressTracker for total bytes.
func NewProgressTracker(total int64, clock ports.Clock) *ProgressTracker {
	t := &ProgressTracker{
		clock:       clock,
		initialized: true,
	}
	t.total.Store(total)
	t.lastSampleTime = t.now()
	return t
}

func (t *ProgressTracker) now() time.Time {
	if t.clock != nil {
		return t.clock.Now()
	}
	return time.Now()
}

// Add atomically increments the total bytes downloaded.
func (t *ProgressTracker) Add(n int64) {
	t.downloaded.Add(n)
}

// Downloaded returns the current total downloaded byte count.
func (t *ProgressTracker) Downloaded() int64 {
	return t.downloaded.Load()
}

// Total returns the expected total byte count.
func (t *ProgressTracker) Total() int64 {
	return t.total.Load()
}

// SetTotal updates the expected total file size.
func (t *ProgressTracker) SetTotal(total int64) {
	t.total.Store(total)
}

// Sample updates the EMA speed calculations based on current time.
func (t *ProgressTracker) Sample() {
	now := t.now()
	currentBytes := t.downloaded.Load()

	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.initialized {
		t.lastSampleTime = now
		t.lastBytes = currentBytes
		t.initialized = true
		return
	}

	elapsed := now.Sub(t.lastSampleTime).Seconds()
	if elapsed <= 0.001 {
		return
	}

	deltaBytes := float64(currentBytes - t.lastBytes)
	instantRate := deltaBytes / elapsed

	alpha1 := 1.0 - math.Exp(-elapsed/1.0)
	alpha5 := 1.0 - math.Exp(-elapsed/5.0)

	t.ema1s = alpha1*instantRate + (1.0-alpha1)*t.ema1s
	t.ema5s = alpha5*instantRate + (1.0-alpha5)*t.ema5s

	t.lastSampleTime = now
	t.lastBytes = currentBytes
}

// Speed1s returns the 1-second window EMA throughput in bytes per second.
func (t *ProgressTracker) Speed1s() int64 {
	t.Sample()
	t.mu.RLock()
	defer t.mu.RUnlock()
	return int64(math.Round(t.ema1s))
}

// Speed5s returns the 5-second window EMA throughput in bytes per second.
func (t *ProgressTracker) Speed5s() int64 {
	t.Sample()
	t.mu.RLock()
	defer t.mu.RUnlock()
	return int64(math.Round(t.ema5s))
}

// Snapshot returns a point-in-time progress snapshot including ETA and percentage.
func (t *ProgressTracker) Snapshot() domain.ProgressSnapshot {
	t.Sample()
	t.mu.RLock()
	bps := int64(math.Round(t.ema1s))
	t.mu.RUnlock()

	return domain.NewProgressSnapshot(t.downloaded.Load(), t.total.Load(), bps)
}
