package httpfetch

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"context"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/idna"
)

var _ ports.Fetcher = (*HTTPFetcher)(nil)

// FetcherConfig provides configuration options for constructing an HTTPFetcher.
type FetcherConfig struct {
	Transport    *http.Transport
	StallTimeout time.Duration
	MaxRedirects int
}

// HTTPFetcher implements ports.Fetcher executing probe discovery and ranged streaming.
type HTTPFetcher struct {
	transport    *http.Transport
	stallTimeout time.Duration
	maxRedirects int
}

// NewFetcher creates a new HTTPFetcher instance.
func NewFetcher(cfg FetcherConfig) (*HTTPFetcher, error) {
	tr := cfg.Transport
	if tr == nil {
		var err error
		tr, err = NewTransport(DefaultTransportConfig())
		if err != nil {
			return nil, err
		}
	}

	stallTimeout := cfg.StallTimeout
	if stallTimeout <= 0 {
		stallTimeout = 30 * time.Second
	}

	maxRedirects := cfg.MaxRedirects
	if maxRedirects <= 0 {
		maxRedirects = 10
	}

	return &HTTPFetcher{
		transport:    tr,
		stallTimeout: stallTimeout,
		maxRedirects: maxRedirects,
	}, nil
}

// Probe inspects an origin URL with Range: bytes=0-0 to discover size, range support, and metadata.
func (f *HTTPFetcher) Probe(ctx context.Context, rc domain.RequestContext, targetURL string) (domain.ProbeResult, error) {
	currentURL, err := normalizeURL(targetURL)
	if err != nil {
		return domain.ProbeResult{}, err
	}

	jar, err := BuildCookieJar(rc, currentURL)
	if err != nil {
		return domain.ProbeResult{}, fmt.Errorf("failed to build cookie jar: %w", err)
	}

	client := &http.Client{
		Transport: f.transport,
		Jar:       jar,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	var resp *http.Response
	redirectHops := 0

	for {
		select {
		case <-ctx.Done():
			return domain.ProbeResult{}, ctx.Err()
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, currentURL.String(), nil)
		if err != nil {
			return domain.ProbeResult{}, fmt.Errorf("failed to create probe request: %w", err)
		}

		ApplyRequestContext(req, rc)
		req.Header.Set("Range", "bytes=0-0")
		req.Header.Set("Accept-Encoding", "identity")

		resp, err = client.Do(req)
		if err != nil {
			return domain.ProbeResult{}, fmt.Errorf("probe request failed: %w", err)
		}

		if isRedirect(resp.StatusCode) {
			redirectHops++
			if redirectHops > f.maxRedirects {
				_ = resp.Body.Close()
				return domain.ProbeResult{}, fmt.Errorf("too many redirects (exceeded limit %d)", f.maxRedirects)
			}

			loc := resp.Header.Get("Location")
			_ = resp.Body.Close()
			if loc == "" {
				return domain.ProbeResult{}, fmt.Errorf("redirect response status %d missing Location header", resp.StatusCode)
			}

			locURL, err := url.Parse(loc)
			if err != nil {
				return domain.ProbeResult{}, fmt.Errorf("invalid redirect Location %q: %w", loc, err)
			}

			nextURL := currentURL.ResolveReference(locURL)
			if nextURL.Scheme != "http" && nextURL.Scheme != "https" {
				return domain.ProbeResult{}, fmt.Errorf("rejected redirect to unsupported scheme %q", nextURL.Scheme)
			}

			StripHeadersOnCrossHost(req, currentURL, nextURL)
			currentURL = nextURL
			continue
		}

		break
	}

	defer func() { _ = resp.Body.Close() }()

	var rangeSupport domain.RangeSupport
	var totalSize int64

	switch resp.StatusCode {
	case http.StatusPartialContent:
		rangeSupport = domain.RangeSupportYes
		cr := parseContentRange(resp.Header.Get("Content-Range"))
		totalSize = cr.Total
	case http.StatusOK:
		if strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes") && resp.ContentLength > 0 {
			rangeSupport = domain.RangeSupportUnverified
			totalSize = resp.ContentLength
		} else {
			rangeSupport = domain.RangeSupportNo
			totalSize = resp.ContentLength
		}
	case http.StatusRequestedRangeNotSatisfiable:
		rangeSupport = domain.RangeSupportYes
		totalSize = 0
	default:
		return domain.ProbeResult{}, fmt.Errorf("probe failed with HTTP status %d", resp.StatusCode)
	}

	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	lastModified := strings.TrimSpace(resp.Header.Get("Last-Modified"))
	contentType := strings.TrimSpace(resp.Header.Get("Content-Type"))

	cd := resp.Header.Get("Content-Disposition")
	filename := parseContentDispositionFilename(cd)
	if filename == "" {
		cleanPath := path.Base(currentURL.Path)
		if cleanPath != "." && cleanPath != "/" && cleanPath != "" {
			if unescaped, err := url.PathUnescape(cleanPath); err == nil && unescaped != "" {
				filename = unescaped
			}
		}
	}
	if filename == "" {
		filename = "download"
	}
	filename = domain.SanitizeFileName(filename)

	var warnings []string
	if ce := resp.Header.Get("Content-Encoding"); ce != "" && !strings.EqualFold(ce, "identity") {
		warnings = append(warnings, "encoded_content")
		rangeSupport = domain.RangeSupportNo
	}
	if etag == "" && lastModified == "" {
		warnings = append(warnings, "no_validator")
	}
	if strings.Contains(strings.ToLower(contentType), "text/html") && hasBinaryExtension(currentURL.Path) {
		warnings = append(warnings, "suspected_html")
	}

	return domain.ProbeResult{
		RangeSupport: rangeSupport,
		TotalSize:    totalSize,
		Validators: domain.Validators{
			ETag:         etag,
			LastModified: lastModified,
		},
		FileName:    filename,
		ContentType: contentType,
		FinalURL:    currentURL.String(),
		Warnings:    warnings,
	}, nil
}

// OpenRange opens an HTTP connection for the inclusive byte range [start, endInclusive].
func (f *HTTPFetcher) OpenRange(ctx context.Context, rc domain.RequestContext, targetURL string, start, endInclusive int64, validator string) (ports.RangeResponse, error) {
	currentURL, err := normalizeURL(targetURL)
	if err != nil {
		return ports.RangeResponse{}, err
	}

	jar, err := BuildCookieJar(rc, currentURL)
	if err != nil {
		return ports.RangeResponse{}, fmt.Errorf("failed to build cookie jar: %w", err)
	}

	client := &http.Client{
		Transport: f.transport,
		Jar:       jar,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, currentURL.String(), nil)
	if err != nil {
		return ports.RangeResponse{}, fmt.Errorf("failed to create range request: %w", err)
	}

	ApplyRequestContext(req, rc)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, endInclusive))
	req.Header.Set("Accept-Encoding", "identity")

	if validator != "" {
		req.Header.Set("If-Range", validator)
	}

	//nolint:bodyclose // body ownership transferred to caller via RangeResponse
	resp, err := client.Do(req)
	if err != nil {
		return ports.RangeResponse{}, fmt.Errorf("range request failed: %w", err)
	}

	var retryAfter time.Duration
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
	}

	var cr domain.ContentRange
	if resp.StatusCode == http.StatusPartialContent {
		cr = parseContentRange(resp.Header.Get("Content-Range"))
	}

	wrappedBody := NewStallReader(resp.Body, f.stallTimeout)

	return ports.RangeResponse{
		Body:         wrappedBody,
		Status:       resp.StatusCode,
		ContentRange: cr,
		RetryAfter:   retryAfter,
	}, nil
}

func normalizeURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("unsupported scheme %q (expected http or https)", u.Scheme)
	}

	asciiHost, err := idna.Lookup.ToASCII(u.Hostname())
	if err != nil {
		return nil, fmt.Errorf("failed to convert IDN host %q: %w", u.Hostname(), err)
	}
	if u.Port() != "" {
		u.Host = asciiHost + ":" + u.Port()
	} else {
		u.Host = asciiHost
	}

	return u, nil
}

func isRedirect(status int) bool {
	return status == http.StatusMovedPermanently ||
		status == http.StatusFound ||
		status == http.StatusSeeOther ||
		status == http.StatusTemporaryRedirect ||
		status == http.StatusPermanentRedirect
}

func parseContentRange(header string) domain.ContentRange {
	if header == "" {
		return domain.ContentRange{Total: -1}
	}

	spec := strings.TrimPrefix(header, "bytes ")
	parts := strings.Split(spec, "/")
	if len(parts) != 2 {
		return domain.ContentRange{Total: -1}
	}

	total := int64(-1)
	if parts[1] != "*" {
		if t, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
			total = t
		}
	}

	rangeParts := strings.Split(parts[0], "-")
	if len(rangeParts) != 2 {
		return domain.ContentRange{Total: total}
	}

	start, err1 := strconv.ParseInt(rangeParts[0], 10, 64)
	end, err2 := strconv.ParseInt(rangeParts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return domain.ContentRange{Total: total}
	}

	return domain.ContentRange{
		Start: start,
		End:   end,
		Total: total,
	}
}

func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}

	if secs, err := strconv.Atoi(header); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}

	if t, err := http.ParseTime(header); err == nil {
		diff := time.Until(t)
		if diff > 0 {
			return diff
		}
	}

	return 0
}

func parseContentDispositionFilename(cd string) string {
	if cd == "" {
		return ""
	}

	_, params, err := mime.ParseMediaType(cd)
	if err == nil {
		if fnStar, ok := params["filename*"]; ok && fnStar != "" {
			parts := strings.SplitN(fnStar, "''", 2)
			if len(parts) == 2 {
				if unescaped, err := url.PathUnescape(parts[1]); err == nil && unescaped != "" {
					return unescaped
				}
			}
		}
		if fn, ok := params["filename"]; ok && fn != "" {
			return fn
		}
	}

	if idx := strings.Index(cd, "filename*="); idx != -1 {
		sub := cd[idx+len("filename*="):]
		if semi := strings.IndexByte(sub, ';'); semi != -1 {
			sub = sub[:semi]
		}
		sub = strings.Trim(sub, `"' `)
		parts := strings.SplitN(sub, "''", 2)
		if len(parts) == 2 {
			if unescaped, err := url.PathUnescape(parts[1]); err == nil && unescaped != "" {
				return unescaped
			}
		}
	}

	if idx := strings.Index(cd, "filename="); idx != -1 {
		sub := cd[idx+len("filename="):]
		if semi := strings.IndexByte(sub, ';'); semi != -1 {
			sub = sub[:semi]
		}
		return strings.Trim(sub, `"' `)
	}

	return ""
}

func hasBinaryExtension(pathStr string) bool {
	ext := strings.ToLower(filepath.Ext(pathStr))
	switch ext {
	case ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".iso", ".dmg", ".bin",
		".exe", ".msi", ".apk", ".deb", ".rpm", ".pdf", ".mp4", ".mkv", ".avi", ".mov",
		".mp3", ".wav", ".flac", ".docx", ".xlsx", ".pptx":
		return true
	}
	return false
}
