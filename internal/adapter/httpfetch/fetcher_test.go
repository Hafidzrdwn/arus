package httpfetch_test

import (
	"arus/internal/adapter/httpfetch"
	"arus/internal/domain"
	"arus/test/testserver"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestFetcher_Probe_RangedNormal(t *testing.T) {
	size := int64(2 * 1024 * 1024)
	lastMod := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	s := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithRanges(true),
		testserver.WithETag(testserver.ETagStrong),
		testserver.WithLastModified(lastMod),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	rc := domain.RequestContext{
		UserAgent: "Arus-Downloader/1.0",
	}

	result, err := fetcher.Probe(t.Context(), rc, s.URL+"/archive.tar")
	if err != nil {
		t.Fatalf("Probe error: %v", err)
	}

	if result.RangeSupport != domain.RangeSupportYes {
		t.Errorf("RangeSupport = %v, want RangeSupportYes", result.RangeSupport)
	}
	if result.TotalSize != size {
		t.Errorf("TotalSize = %d, want %d", result.TotalSize, size)
	}
	if result.Validators.ETag == "" {
		t.Errorf("expected non-empty ETag")
	}
	if result.Validators.LastModified == "" {
		t.Errorf("expected non-empty LastModified")
	}
	if result.FileName != "archive.tar" {
		t.Errorf("FileName = %q, want 'archive.tar'", result.FileName)
	}

	stats := s.Stats()
	if stats.TotalRequests < 1 {
		t.Fatalf("expected at least 1 request, got %d", stats.TotalRequests)
	}
	headers := stats.HeadersReceived[0]
	if enc := headers.Get("Accept-Encoding"); enc != "identity" {
		t.Errorf("Accept-Encoding = %q, want 'identity'", enc)
	}
	if rng := headers.Get("Range"); rng != "bytes=0-0" {
		t.Errorf("Range = %q, want 'bytes=0-0'", rng)
	}
}

func TestFetcher_Probe_NoRange(t *testing.T) {
	size := int64(5000)
	s := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithRanges(false),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	result, err := fetcher.Probe(t.Context(), domain.RequestContext{}, s.URL+"/data.bin")
	if err != nil {
		t.Fatalf("Probe error: %v", err)
	}

	if result.RangeSupport != domain.RangeSupportNo {
		t.Errorf("RangeSupport = %v, want RangeSupportNo", result.RangeSupport)
	}
	if result.TotalSize != size {
		t.Errorf("TotalSize = %d, want %d", result.TotalSize, size)
	}
}

func TestFetcher_Probe_EmptyFile_416(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(0),
		testserver.WithRanges(true),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	result, err := fetcher.Probe(t.Context(), domain.RequestContext{}, s.URL+"/empty.txt")
	if err != nil {
		t.Fatalf("Probe error: %v", err)
	}

	if result.RangeSupport != domain.RangeSupportYes {
		t.Errorf("RangeSupport = %v, want RangeSupportYes", result.RangeSupport)
	}
	if result.TotalSize != 0 {
		t.Errorf("TotalSize = %d, want 0", result.TotalSize)
	}
}

func TestFetcher_Probe_Chunked_NoLength(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024),
		testserver.WithChunkedNoLength(),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	result, err := fetcher.Probe(t.Context(), domain.RequestContext{}, s.URL+"/stream.log")
	if err != nil {
		t.Fatalf("Probe error: %v", err)
	}

	if result.RangeSupport != domain.RangeSupportNo {
		t.Errorf("RangeSupport = %v, want RangeSupportNo", result.RangeSupport)
	}
	if result.TotalSize != -1 {
		t.Errorf("TotalSize = %d, want -1 for chunked unknown length", result.TotalSize)
	}
}

func TestFetcher_Probe_GzipWarning(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024),
		testserver.WithGzip(),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	result, err := fetcher.Probe(t.Context(), domain.RequestContext{}, s.URL+"/compressed.data")
	if err != nil {
		t.Fatalf("Probe error: %v", err)
	}

	foundWarning := false
	for _, w := range result.Warnings {
		if w == "encoded_content" {
			foundWarning = true
			break
		}
	}
	if !foundWarning {
		t.Errorf("expected 'encoded_content' warning, got: %v", result.Warnings)
	}
	if result.RangeSupport != domain.RangeSupportNo {
		t.Errorf("RangeSupport = %v, want RangeSupportNo when gzip encoded", result.RangeSupport)
	}
}

func TestFetcher_Probe_ContentDisposition_RFC6266(t *testing.T) {
	filename := "€.zip"
	s := testserver.New(t,
		testserver.WithSize(1024),
		testserver.WithContentDisposition(filename),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	result, err := fetcher.Probe(t.Context(), domain.RequestContext{}, s.URL+"/generic_path")
	if err != nil {
		t.Fatalf("Probe error: %v", err)
	}

	if result.FileName != filename {
		t.Errorf("FileName = %q, want %q", result.FileName, filename)
	}
}

func TestFetcher_Probe_Warnings_NoValidator_And_SuspectedHTML(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(500),
		testserver.WithETag(testserver.ETagNone),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	result, err := fetcher.Probe(t.Context(), domain.RequestContext{}, s.URL+"/installer.exe")
	if err != nil {
		t.Fatalf("Probe error: %v", err)
	}

	foundNoValidator := false
	for _, w := range result.Warnings {
		if w == "no_validator" {
			foundNoValidator = true
			break
		}
	}
	if !foundNoValidator {
		t.Errorf("expected 'no_validator' warning, got: %v", result.Warnings)
	}
}

func TestFetcher_Probe_Redirect_And_CookieScoping(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024),
		testserver.WithRedirects(2, false),
		testserver.WithRequireCookie("sid", "secret99"),
	)

	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	rc := domain.RequestContext{
		Cookies: []domain.Cookie{
			{
				Name:   "sid",
				Value:  "secret99",
				Domain: u.Hostname(),
				Path:   "/",
			},
		},
	}

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	result, err := fetcher.Probe(t.Context(), rc, s.URL)
	if err != nil {
		t.Fatalf("Probe error: %v", err)
	}

	if !strings.HasSuffix(result.FinalURL, "/download") {
		t.Errorf("FinalURL = %q, expected ending in /download", result.FinalURL)
	}

	stats := s.Stats()
	if stats.TotalRequests < 3 {
		t.Errorf("expected >= 3 requests (2 redirects + 1 final), got: %d", stats.TotalRequests)
	}
}

func TestFetcher_ContextCancellation(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024*1024),
		testserver.WithLatency(500*time.Millisecond),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = fetcher.Probe(ctx, domain.RequestContext{}, s.URL)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}
	if elapsed > 150*time.Millisecond {
		t.Errorf("cancellation took %v, want < 150ms", elapsed)
	}
}

func TestFetcher_OpenRange_Success(t *testing.T) {
	size := int64(1024 * 1024)
	s := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithETag(testserver.ETagStrong),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	startOff := int64(100)
	endOff := int64(199)
	rangeLen := endOff - startOff + 1

	resp, err := fetcher.OpenRange(t.Context(), domain.RequestContext{}, s.URL, startOff, endOff, "")
	if err != nil {
		t.Fatalf("OpenRange error: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.Status != http.StatusPartialContent {
		t.Errorf("Status = %d, want 206", resp.Status)
	}
	if resp.ContentRange.Start != startOff || resp.ContentRange.End != endOff || resp.ContentRange.Total != size {
		t.Errorf("ContentRange = %+v, want Start=%d End=%d Total=%d", resp.ContentRange, startOff, endOff, size)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if int64(len(data)) != rangeLen {
		t.Errorf("len(data) = %d, want %d", len(data), rangeLen)
	}
}

func TestFetcher_OpenRange_RetryAfter(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024),
		testserver.WithFailFirst(1, http.StatusTooManyRequests, 5*time.Second),
	)

	fetcher, err := httpfetch.NewFetcher(httpfetch.FetcherConfig{})
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}

	resp, err := fetcher.OpenRange(t.Context(), domain.RequestContext{}, s.URL, 0, 100, "")
	if err != nil {
		t.Fatalf("OpenRange error: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.Status != http.StatusTooManyRequests {
		t.Errorf("Status = %d, want 429", resp.Status)
	}
	if resp.RetryAfter != 5*time.Second {
		t.Errorf("RetryAfter = %v, want 5s", resp.RetryAfter)
	}
}
