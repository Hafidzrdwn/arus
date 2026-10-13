package ports

import (
	"arus/internal/domain"
	"context"
	"io"
	"time"
)

// RangeResponse encapsulates the data returned by an OpenRange request.
type RangeResponse struct {
	// Body contains the ranged stream payload; caller must close when finished.
	Body io.ReadCloser
	// Status is the HTTP response status code (expected 206 for partial content).
	Status int
	// ContentRange contains parsed byte range headers (e.g. bytes 0-1023/10240).
	ContentRange domain.ContentRange
	// RetryAfter is parsed from 429/503 headers when throttling is requested.
	RetryAfter time.Duration
}

// Fetcher defines the outbound driven port for HTTP discovery and byte range downloading.
// Implementations must be safe for concurrent use across multiple goroutines.
type Fetcher interface {
	// Probe sends a non-destructive verification request (Range 0-0) to determine size,
	// range support, and cache validators without fetching the entire body.
	// Returns domain.ProbeResult on success or an error if unreachable or redirected illegally.
	Probe(ctx context.Context, rc domain.RequestContext, url string) (domain.ProbeResult, error)

	// OpenRange opens an HTTP connection for the inclusive byte range [start, endInclusive].
	// The validator string (ETag or Last-Modified) is transmitted via conditional headers (If-Range).
	// If the server returns 200 instead of 206, the caller handles remote-changed detection.
	OpenRange(ctx context.Context, rc domain.RequestContext, url string, start, endInclusive int64, validator string) (RangeResponse, error)
}
