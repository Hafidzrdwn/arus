package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// LimitProvider supplies a rate limit in bytes per second.
type LimitProvider interface {
	Limit() int64
}

// StaticLimitProvider is a LimitProvider that returns a constant limit.
type StaticLimitProvider int64

// Limit returns the static rate limit.
func (s StaticLimitProvider) Limit() int64 {
	return int64(s)
}

// Throttle manages bandwidth rate limiting backed by token-bucket rate.Limiter.
type Throttle struct {
	mu      sync.RWMutex
	limiter *rate.Limiter
	bps     atomic.Int64
}

// NewThrottle creates a Throttle configured with a bandwidth limit in bytes per second.
func NewThrottle(bps int64) *Throttle {
	t := &Throttle{}
	t.SetLimit(bps)
	return t
}

// Limit returns the configured rate limit in bytes per second.
func (t *Throttle) Limit() int64 {
	if t == nil {
		return 0
	}
	return t.bps.Load()
}

// SetLimit dynamically updates the bandwidth limit and adjusts burst capacity.
func (t *Throttle) SetLimit(bps int64) {
	if bps < 0 {
		bps = 0
	}
	t.bps.Store(bps)

	t.mu.Lock()
	defer t.mu.Unlock()

	if bps <= 0 {
		t.limiter = nil
		return
	}

	burst := bufferSize

	if t.limiter == nil {
		t.limiter = rate.NewLimiter(rate.Limit(bps), burst)
	} else {
		t.limiter.SetLimit(rate.Limit(bps))
		t.limiter.SetBurst(burst)
	}
}

func (t *Throttle) reserveN(now time.Time, n int) (time.Duration, *rate.Reservation, bool) {
	if t == nil || t.bps.Load() <= 0 {
		return 0, nil, true
	}

	t.mu.RLock()
	lim := t.limiter
	t.mu.RUnlock()

	if lim == nil {
		return 0, nil, true
	}

	if n > lim.Burst() {
		t.mu.Lock()
		lim.SetBurst(n)
		t.mu.Unlock()
	}

	res := lim.ReserveN(now, n)
	if !res.OK() {
		return 0, nil, false
	}
	return res.DelayFrom(now), res, true
}

// LimiterSet coordinates hierarchical global and per-job rate limiters.
type LimiterSet struct {
	global *Throttle
	job    *Throttle
}

// NewLimiterSet bundles global and per-job Throttle instances.
func NewLimiterSet(global, job *Throttle) *LimiterSet {
	return &LimiterSet{
		global: global,
		job:    job,
	}
}

// WaitN coordinates token reservations across global and job throttles with a single sleep.
func (l *LimiterSet) WaitN(ctx context.Context, n int) error {
	if l == nil || n <= 0 {
		return nil
	}

	gActive := l.global != nil && l.global.bps.Load() > 0
	jActive := l.job != nil && l.job.bps.Load() > 0
	if !gActive && !jActive {
		return nil
	}

	now := time.Now()
	var (
		resG *rate.Reservation
		resJ *rate.Reservation
		dG   time.Duration
		dJ   time.Duration
	)

	if gActive {
		var ok bool
		dG, resG, ok = l.global.reserveN(now, n)
		if !ok {
			return errors.New("global rate limiter reservation failed")
		}
	}

	if jActive {
		var ok bool
		dJ, resJ, ok = l.job.reserveN(now, n)
		if !ok {
			if resG != nil {
				resG.CancelAt(now)
			}
			return errors.New("job rate limiter reservation failed")
		}
	}

	delay := dG
	if dJ > delay {
		delay = dJ
	}

	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		cancelNow := time.Now()
		if resG != nil {
			resG.CancelAt(cancelNow)
		}
		if resJ != nil {
			resJ.CancelAt(cancelNow)
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// SetJobLimit dynamically sets the per-job bandwidth limit in bytes per second.
func (l *LimiterSet) SetJobLimit(bps int64) {
	if l != nil && l.job != nil {
		l.job.SetLimit(bps)
	}
}

// SetGlobalLimit dynamically sets the global bandwidth limit in bytes per second.
func (l *LimiterSet) SetGlobalLimit(bps int64) {
	if l != nil && l.global != nil {
		l.global.SetLimit(bps)
	}
}

// JobLimit returns the current per-job bandwidth limit.
func (l *LimiterSet) JobLimit() int64 {
	if l != nil && l.job != nil {
		return l.job.Limit()
	}
	return 0
}

// GlobalLimit returns the current global bandwidth limit.
func (l *LimiterSet) GlobalLimit() int64 {
	if l != nil && l.global != nil {
		return l.global.Limit()
	}
	return 0
}
