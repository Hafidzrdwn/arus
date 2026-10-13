package engine

import (
	"net/url"
	"strings"
	"sync"
	"time"
)

// HostProfiles stores learned operational profiles and connection ceilings per host.
type HostProfiles struct {
	mu           sync.RWMutex
	defaultLimit int
	profiles     map[string]int
}

// NewHostProfiles constructs an initialized HostProfiles tracker.
func NewHostProfiles(defaultPerHost int) *HostProfiles {
	if defaultPerHost <= 0 {
		defaultPerHost = 8
	}
	return &HostProfiles{
		defaultLimit: defaultPerHost,
		profiles:     make(map[string]int),
	}
}

// SafeMaxConns returns the safe connection ceiling for host (default 8, or learned lower).
func (h *HostProfiles) SafeMaxConns(host string) int {
	if h == nil {
		return 8
	}
	h.mu.RLock()
	defer h.mu.RUnlock()

	cleanHost := cleanHostString(host)
	if limit, ok := h.profiles[cleanHost]; ok && limit > 0 {
		return limit
	}
	return h.defaultLimit
}

// Learn429 records that a 429 was received at activeConns, reducing SafeMaxConns for this host.
func (h *HostProfiles) Learn429(host string, activeConns int) {
	if h == nil || activeConns <= 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	cleanHost := cleanHostString(host)
	newLimit := activeConns - 1
	if newLimit < 1 {
		newLimit = 1
	}

	if curr, ok := h.profiles[cleanHost]; !ok || newLimit < curr {
		h.profiles[cleanHost] = newLimit
	}
}

func cleanHostString(host string) string {
	h := strings.TrimSpace(host)
	if strings.Contains(h, "://") {
		if u, err := url.Parse(h); err == nil && u.Host != "" {
			h = u.Host
		}
	}
	if colon := strings.IndexByte(h, ':'); colon != -1 {
		h = h[:colon]
	}
	return strings.ToLower(h)
}

// ConnBudget coordinates global and per-host connection ceilings across concurrent jobs.
type ConnBudget struct {
	mu         sync.Mutex
	maxGlobal  int
	maxPerHost int
	totalInUse int
	hostInUse  map[string]int
}

// NewConnBudget creates a thread-safe connection budget.
func NewConnBudget(maxGlobal, maxPerHost int) *ConnBudget {
	if maxGlobal <= 0 {
		maxGlobal = 64
	}
	if maxPerHost <= 0 {
		maxPerHost = 8
	}
	return &ConnBudget{
		maxGlobal:  maxGlobal,
		maxPerHost: maxPerHost,
		hostInUse:  make(map[string]int),
	}
}

// TryAcquire attempts to allocate additional connections for a host up to target,
// constrained by maxGlobal, maxPerHost, and safeCeiling. Returns the granted count.
func (b *ConnBudget) TryAcquire(host string, current, target, safeCeiling int) int {
	if b == nil {
		return target
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	cleanHost := cleanHostString(host)

	hostCap := b.maxPerHost
	if safeCeiling > 0 && safeCeiling < hostCap {
		hostCap = safeCeiling
	}

	if target <= current {
		diff := current - target
		b.hostInUse[cleanHost] -= diff
		if b.hostInUse[cleanHost] < 0 {
			b.hostInUse[cleanHost] = 0
		}
		b.totalInUse -= diff
		if b.totalInUse < 0 {
			b.totalInUse = 0
		}
		return target
	}

	diff := target - current
	currentHost := b.hostInUse[cleanHost]
	hostAvail := hostCap - currentHost
	if hostAvail < 0 {
		hostAvail = 0
	}

	globalAvail := b.maxGlobal - b.totalInUse
	if globalAvail < 0 {
		globalAvail = 0
	}

	allowedAdd := min(diff, min(hostAvail, globalAvail))
	if allowedAdd < 0 {
		allowedAdd = 0
	}

	b.hostInUse[cleanHost] += allowedAdd
	b.totalInUse += allowedAdd

	return current + allowedAdd
}

// Release releases allocated connections for host when a job terminates or scales down.
func (b *ConnBudget) Release(host string, count int) {
	if b == nil || count <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	cleanHost := cleanHostString(host)
	b.hostInUse[cleanHost] -= count
	if b.hostInUse[cleanHost] < 0 {
		b.hostInUse[cleanHost] = 0
	}
	b.totalInUse -= count
	if b.totalInUse < 0 {
		b.totalInUse = 0
	}
}

// ErrorTracker aggregates connection errors and throttling responses within an observation window.
type ErrorTracker struct {
	mu         sync.Mutex
	resets     int
	timeouts   int
	status429  int
	status503  int
	retryAfter time.Duration
}

// Record429 records an HTTP 429 status code and associated Retry-After duration.
func (e *ErrorTracker) Record429(retryAfter time.Duration) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.status429++
	if retryAfter > e.retryAfter {
		e.retryAfter = retryAfter
	}
}

// Record503 records an HTTP 503 status code and associated Retry-After duration.
func (e *ErrorTracker) Record503(retryAfter time.Duration) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.status503++
	if retryAfter > e.retryAfter {
		e.retryAfter = retryAfter
	}
}

// RecordReset records a connection reset event.
func (e *ErrorTracker) RecordReset() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.resets++
}

// RecordTimeout records a connection or read timeout event.
func (e *ErrorTracker) RecordTimeout() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.timeouts++
}

// Drain captures the accumulated error window and resets internal counters for the next cycle.
func (e *ErrorTracker) Drain() (ErrWindow, time.Duration) {
	if e == nil {
		return ErrWindow{}, 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	w := ErrWindow{
		Resets:    e.resets,
		Timeouts:  e.timeouts,
		Status429: e.status429,
		Status503: e.status503,
	}
	ra := e.retryAfter

	e.resets = 0
	e.timeouts = 0
	e.status429 = 0
	e.status503 = 0
	e.retryAfter = 0

	return w, ra
}
