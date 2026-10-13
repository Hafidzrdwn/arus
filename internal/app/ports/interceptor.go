package ports

import (
	"arus/internal/domain"
	"context"
)

// InterceptorStatus describes the health and runtime state of a browser interceptor.
type InterceptorStatus struct {
	Name    string `json:"name"`
	Running bool   `json:"running"`
	Tier    int    `json:"tier"` // Tier 1: NM, Tier 2: MITM, Tier 3: Protocol/Clipboard
	Err     string `json:"err,omitempty"`
}

// CaptureSink is the driven consumer interface receiving captured downloads from interceptors.
type CaptureSink interface {
	// Submit processes an intercepted download through rules, deduplication, and queues.
	// Returns a domain.CaptureResult indicating whether the capture was accepted or needs confirmation.
	Submit(ctx context.Context, c domain.Capture) (domain.CaptureResult, error)
}

// Interceptor defines driving inbound adapters that capture download requests from web browsers.
type Interceptor interface {
	// Name returns the unique identifier for the interceptor (e.g. "nativemsg", "mitm", "clipboard").
	Name() string

	// Start begins listening for download events and forwards them to sink.
	Start(ctx context.Context, sink CaptureSink) error

	// Stop gracefully ceases listening and restores any system configuration (e.g. proxy settings).
	Stop(ctx context.Context) error

	// Status returns the current operational status.
	Status() InterceptorStatus
}
