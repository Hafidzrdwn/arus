package testserver

import (
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// TestingTB is a subset of testing.TB used for server lifecycle management.
type TestingTB interface {
	Cleanup(func())
	Helper()
}

// Server is an HTTP test harness simulating origin servers with fault injection.
type Server struct {
	URL        string
	server     *httptest.Server
	crossHost  *httptest.Server
	cfg        config
	stats      *statsTracker
	globalRate *rate.Limiter
	startTime  time.Time

	mu         sync.Mutex
	usedTokens map[string]bool
	failCount  int
	reqIndex   int
}

// New creates and starts a test HTTP server configured with options.
func New(t TestingTB, opts ...Option) *Server {
	t.Helper()

	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	s := &Server{
		cfg:        cfg,
		stats:      newStatsTracker(),
		startTime:  time.Now(),
		usedTokens: make(map[string]bool),
	}

	if cfg.globalBPS > 0 {
		burst := int(cfg.globalBPS)
		if burst > 32*1024 {
			burst = 32 * 1024
		}
		if burst < 1024 {
			burst = 1024
		}
		s.globalRate = rate.NewLimiter(rate.Limit(cfg.globalBPS), burst)
		s.globalRate.AllowN(time.Now(), burst)
	}

	if cfg.crossHostRedirect {
		s.crossHost = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			target := s.server.URL + "/download"
			http.Redirect(w, r, target, http.StatusFound)
		}))
	}

	s.server = httptest.NewServer(http.HandlerFunc(s.handleHTTP))
	s.URL = s.server.URL

	t.Cleanup(func() {
		s.Close()
	})

	return s
}

// Close gracefully terminates the server and any secondary host instances.
func (s *Server) Close() {
	if s.server != nil {
		s.server.Close()
	}
	if s.crossHost != nil {
		s.crossHost.Close()
	}
}

// Client returns an HTTP client configured to connect to the test server.
func (s *Server) Client() *http.Client {
	if s.server != nil {
		return s.server.Client()
	}
	return http.DefaultClient
}

// Stats returns a thread-safe snapshot of request metrics.
func (s *Server) Stats() Stats {
	return s.stats.snapshot()
}

func (s *Server) handleHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	reqIdx := s.reqIndex
	s.reqIndex++
	s.mu.Unlock()

	s.stats.recordRequest(r)
	defer s.stats.releaseConn()

	if s.cfg.latency > 0 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(s.cfg.latency):
		}
	}

	if s.cfg.maxConns > 0 {
		if s.stats.snapshot().ActiveConns > int64(s.cfg.maxConns) {
			http.Error(w, "too many requests", http.StatusTooManyRequests)
			return
		}
	}

	if s.cfg.expireAfter > 0 && time.Since(s.startTime) >= s.cfg.expireAfter {
		http.Error(w, "link expired", http.StatusForbidden)
		return
	}

	s.mu.Lock()
	if s.cfg.failFirstK > 0 && s.failCount < s.cfg.failFirstK {
		s.failCount++
		status := s.cfg.failStatus
		if status == 0 {
			status = http.StatusServiceUnavailable
		}
		if s.cfg.failRetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(s.cfg.failRetryAfter.Seconds())))
		}
		s.mu.Unlock()
		http.Error(w, "temporary failure", status)
		return
	}
	s.mu.Unlock()

	if s.cfg.oneTimeToken {
		tok := r.URL.Query().Get("token")
		if tok == "" {
			http.Error(w, "missing token", http.StatusForbidden)
			return
		}
		s.mu.Lock()
		if s.usedTokens[tok] {
			s.mu.Unlock()
			http.Error(w, "token already consumed", http.StatusForbidden)
			return
		}
		s.usedTokens[tok] = true
		s.mu.Unlock()
	}

	if s.cfg.requireCookieName != "" {
		cookie, err := r.Cookie(s.cfg.requireCookieName)
		if err != nil || (s.cfg.requireCookieVal != "" && cookie.Value != s.cfg.requireCookieVal) {
			http.Error(w, "unauthorized cookie missing", http.StatusUnauthorized)
			return
		}
	}

	if s.cfg.requireReferer != "" {
		ref := r.Referer()
		if !strings.HasPrefix(ref, s.cfg.requireReferer) {
			http.Error(w, "forbidden invalid referer", http.StatusForbidden)
			return
		}
	}

	if s.cfg.redirectCount > 0 && strings.HasPrefix(r.URL.Path, "/redirect/") {
		stepStr := strings.TrimPrefix(r.URL.Path, "/redirect/")
		step, _ := strconv.Atoi(stepStr)
		if step < s.cfg.redirectCount {
			http.Redirect(w, r, fmt.Sprintf("/redirect/%d", step+1), http.StatusFound)
			return
		}
		if s.cfg.crossHostRedirect && s.crossHost != nil {
			http.Redirect(w, r, s.crossHost.URL+"/cross", http.StatusFound)
			return
		}
		http.Redirect(w, r, "/download", http.StatusFound)
		return
	}
	if s.cfg.redirectCount > 0 && r.URL.Path != "/download" {
		http.Redirect(w, r, "/redirect/1", http.StatusFound)
		return
	}

	etagVal := s.currentETag()
	if etagVal != "" {
		w.Header().Set("ETag", etagVal)
	}
	if !s.cfg.lastModified.IsZero() {
		w.Header().Set("Last-Modified", s.cfg.lastModified.UTC().Format(http.TimeFormat))
	}

	if s.cfg.contentDisposition != "" {
		cd := formatContentDisposition(s.cfg.contentDisposition)
		w.Header().Set("Content-Disposition", cd)
	}

	rangeHeader := r.Header.Get("Range")
	if !s.cfg.ranges {
		w.Header().Set("Accept-Ranges", "none")
		rangeHeader = ""
	} else {
		w.Header().Set("Accept-Ranges", "bytes")
	}

	if s.cfg.gzip {
		w.Header().Set("Content-Encoding", "gzip")
	}

	if rangeHeader != "" && r.Header.Get("If-Range") != "" {
		ifRange := strings.TrimSpace(r.Header.Get("If-Range"))
		if ifRange != etagVal && (s.cfg.lastModified.IsZero() || ifRange != s.cfg.lastModified.UTC().Format(http.TimeFormat)) {
			rangeHeader = ""
		}
	}

	totalSize := s.cfg.size

	connBPS := s.cfg.perConnBPS
	if s.cfg.slowConnIndex >= 0 && reqIdx == s.cfg.slowConnIndex && s.cfg.slowConnBPS > 0 {
		connBPS = s.cfg.slowConnBPS
	}
	if s.cfg.requestThrottle != nil {
		if tbps := s.cfg.requestThrottle(reqIdx, r); tbps > 0 {
			connBPS = tbps
		}
	}

	if rangeHeader == "" || s.cfg.gzip || s.cfg.chunkedNoLength {
		if !s.cfg.chunkedNoLength && !s.cfg.gzip {
			w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		}
		w.WriteHeader(http.StatusOK)
		s.streamPayload(w, r, 0, totalSize, connBPS)
		return
	}

	start, end, ok := parseSingleRange(rangeHeader, totalSize)
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
		http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	rangeLen := end - start + 1
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
	w.Header().Set("Content-Length", strconv.FormatInt(rangeLen, 10))
	w.WriteHeader(http.StatusPartialContent)

	s.streamPayload(w, r, start, rangeLen, connBPS)
}

func (s *Server) streamPayload(w http.ResponseWriter, r *http.Request, start, length, connBPS int64) {
	ctx := r.Context()
	var dest io.Writer = w

	var gzWriter *gzip.Writer
	if s.cfg.gzip {
		gzWriter = gzip.NewWriter(w)
		defer func() { _ = gzWriter.Close() }()
		dest = gzWriter
	}

	chunkSize := int64(16 * 1024)
	var connLimiter *rate.Limiter
	if connBPS > 0 {
		burst := int(chunkSize)
		if int64(burst) > connBPS {
			burst = int(connBPS)
		}
		if burst <= 0 {
			burst = 1024
		}
		connLimiter = rate.NewLimiter(rate.Limit(connBPS), burst)
		connLimiter.AllowN(time.Now(), burst)
		if int64(burst) < chunkSize {
			chunkSize = int64(burst)
		}
	}

	if s.globalRate != nil && int64(s.globalRate.Burst()) < chunkSize {
		chunkSize = int64(s.globalRate.Burst())
	}
	if chunkSize <= 0 {
		chunkSize = 1024
	}

	buf := make([]byte, chunkSize)

	var written int64
	for written < length {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if s.cfg.expireAfter > 0 && time.Since(s.startTime) >= s.cfg.expireAfter {
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
					return
				}
			}
			return
		}

		toRead := chunkSize
		if length-written < toRead {
			toRead = length - written
		}

		FillDeterministic(buf[:toRead], s.cfg.seed, start+written)

		if connLimiter != nil {
			if err := connLimiter.WaitN(ctx, int(toRead)); err != nil {
				return
			}
		}
		if s.globalRate != nil {
			if err := s.globalRate.WaitN(ctx, int(toRead)); err != nil {
				return
			}
		}

		n, err := dest.Write(buf[:toRead])
		if n > 0 {
			written += int64(n)
			s.stats.addBytes(int64(n))
		}

		if flusher, ok := dest.(http.Flusher); ok {
			flusher.Flush()
		} else if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}

		if s.cfg.dropAfter > 0 && written >= s.cfg.dropAfter {
			//nolint:gosec // pseudo-randomness for test fault injection
			if s.cfg.dropProbability >= 1.0 || rand.Float64() <= s.cfg.dropProbability {
				if hj, ok := w.(http.Hijacker); ok {
					if conn, _, err := hj.Hijack(); err == nil {
						_ = conn.Close()
						return
					}
				}
			}
		}

		if err != nil {
			return
		}
	}
}

func (s *Server) currentETag() string {
	if s.cfg.etagType == ETagNone {
		return ""
	}
	base := fmt.Sprintf("%x-%d", s.cfg.seed, s.cfg.size)
	if s.cfg.mutateETagAfter > 0 && s.stats.getBytes() >= s.cfg.mutateETagAfter {
		base += "-mutated"
	}
	if s.cfg.etagType == ETagWeak {
		return fmt.Sprintf("W/\"%s\"", base)
	}
	return fmt.Sprintf("\"%s\"", base)
}

func parseSingleRange(rangeHeader string, totalSize int64) (start, end int64, ok bool) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(rangeHeader, "bytes=")
	if strings.Contains(spec, ",") {
		spec = strings.Split(spec, ",")[0]
	}
	spec = strings.TrimSpace(spec)

	parts := strings.Split(spec, "-")
	if len(parts) != 2 {
		return 0, 0, false
	}

	if parts[0] == "" {
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false
		}
		if suffix > totalSize {
			suffix = totalSize
		}
		return totalSize - suffix, totalSize - 1, true
	}

	var err error
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 || start >= totalSize {
		return 0, 0, false
	}

	if parts[1] == "" {
		return start, totalSize - 1, true
	}

	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || end < start || end >= totalSize {
		return 0, 0, false
	}

	return start, end, true
}

func formatContentDisposition(filename string) string {
	asciiOnly := true
	for _, r := range filename {
		if r > 127 || r == '"' || r == '\\' {
			asciiOnly = false
			break
		}
	}
	if asciiOnly {
		return fmt.Sprintf("attachment; filename=%q", filename)
	}
	return fmt.Sprintf("attachment; filename=%q; filename*=UTF-8''%s", filename, url.PathEscape(filename))
}
