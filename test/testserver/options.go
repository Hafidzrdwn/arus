package testserver

import "time"

// ETagType defines the format of the ETag header.
type ETagType int

const (
	// ETagNone disables the ETag header.
	ETagNone ETagType = iota
	// ETagStrong sets a strong ETag header.
	ETagStrong
	// ETagWeak sets a weak ETag header with W/ prefix.
	ETagWeak
)

type config struct {
	size               int64
	seed               int64
	ranges             bool
	etagType           ETagType
	lastModified       time.Time
	contentDisposition string
	chunkedNoLength    bool
	gzip               bool
	redirectCount      int
	crossHostRedirect  bool
	requireCookieName  string
	requireCookieVal   string
	requireReferer     string
	perConnBPS         int64
	globalBPS          int64
	latency            time.Duration
	dropAfter          int64
	dropProbability    float64
	failFirstK         int
	failStatus         int
	failRetryAfter     time.Duration
	expireAfter        time.Duration
	oneTimeToken       bool
	mutateETagAfter    int64
	maxConns           int
}

func defaultConfig() config {
	return config{
		size:            10 * 1024 * 1024, // 10 MiB default
		seed:            12345,
		ranges:          true,
		etagType:        ETagStrong,
		dropProbability: 1.0,
	}
}

// Option configures Server behavior.
type Option func(*config)

// WithSize sets the payload size in bytes.
func WithSize(n int64) Option {
	return func(c *config) { c.size = n }
}

// WithSeed sets the random seed for deterministic payload generation.
func WithSeed(s int64) Option {
	return func(c *config) { c.seed = s }
}

// WithRanges controls whether the server advertises Accept-Ranges: bytes and processes Range requests.
func WithRanges(enabled bool) Option {
	return func(c *config) { c.ranges = enabled }
}

// WithETag configures the ETag format (ETagStrong, ETagWeak, or ETagNone).
func WithETag(t ETagType) Option {
	return func(c *config) { c.etagType = t }
}

// WithLastModified sets the Last-Modified header on responses.
func WithLastModified(t time.Time) Option {
	return func(c *config) { c.lastModified = t }
}

// WithContentDisposition specifies the attachment filename for Content-Disposition header.
func WithContentDisposition(name string) Option {
	return func(c *config) { c.contentDisposition = name }
}

// WithChunkedNoLength enables chunked transfer encoding and omits Content-Length.
func WithChunkedNoLength() Option {
	return func(c *config) { c.chunkedNoLength = true }
}

// WithGzip compresses the response body with gzip and sets Content-Encoding: gzip.
func WithGzip() Option {
	return func(c *config) { c.gzip = true }
}

// WithRedirects configures a chain of HTTP 302 redirects prior to serving content.
func WithRedirects(n int, crossHost bool) Option {
	return func(c *config) {
		c.redirectCount = n
		c.crossHostRedirect = crossHost
	}
}

// WithRequireCookie enforces that incoming requests carry cookie name=val, otherwise 401.
func WithRequireCookie(name, val string) Option {
	return func(c *config) {
		c.requireCookieName = name
		c.requireCookieVal = val
	}
}

// WithRequireReferer requires Referer header starting with prefix, otherwise 403.
func WithRequireReferer(prefix string) Option {
	return func(c *config) { c.requireReferer = prefix }
}

// WithPerConnBPS throttles transmission on each connection to n bytes per second.
func WithPerConnBPS(n int64) Option {
	return func(c *config) { c.perConnBPS = n }
}

// WithGlobalBPS throttles aggregate transmission across all connections to n bytes per second.
func WithGlobalBPS(n int64) Option {
	return func(c *config) { c.globalBPS = n }
}

// WithLatency injects an artificial delay prior to sending headers and body.
func WithLatency(d time.Duration) Option {
	return func(c *config) { c.latency = d }
}

// WithDropAfter abruptly drops/closes the TCP connection after transmitting bytes.
func WithDropAfter(bytes int64, prob float64) Option {
	return func(c *config) {
		c.dropAfter = bytes
		c.dropProbability = prob
	}
}

// WithFailFirst returns status (e.g. 429 or 503) with Retry-After for the first k requests.
func WithFailFirst(k int, status int, retryAfter time.Duration) Option {
	return func(c *config) {
		c.failFirstK = k
		c.failStatus = status
		c.failRetryAfter = retryAfter
	}
}

// WithExpireAfter causes the server to return 403 Forbidden after d has elapsed since startup.
func WithExpireAfter(d time.Duration) Option {
	return func(c *config) { c.expireAfter = d }
}

// WithOneTimeToken requires a ?token=... query parameter, returning 403 if reused or missing.
func WithOneTimeToken() Option {
	return func(c *config) { c.oneTimeToken = true }
}

// WithMutateETagAfter changes the ETag value after total bytes have been served.
func WithMutateETagAfter(bytes int64) Option {
	return func(c *config) { c.mutateETagAfter = bytes }
}

// WithMaxConns limits concurrent requests, returning 429 when concurrency exceeds n.
func WithMaxConns(n int) Option {
	return func(c *config) { c.maxConns = n }
}
