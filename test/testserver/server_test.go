package testserver_test

import (
	"arus/test/testserver"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func getReq(t *testing.T, client *http.Client, rawURL string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext error: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do error: %v", err)
	}
	return resp
}

func newReq(t *testing.T, method, rawURL string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, rawURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext error: %v", err)
	}
	return req
}

func TestServer_RangeRequests(t *testing.T) {
	totalSize := int64(1024 * 1024)
	seed := int64(99999)

	s := testserver.New(t,
		testserver.WithSize(totalSize),
		testserver.WithSeed(seed),
		testserver.WithRanges(true),
	)

	client := s.Client()

	t.Run("Probe_Range_0_0", func(t *testing.T) {
		req := newReq(t, http.MethodGet, s.URL)
		req.Header.Set("Range", "bytes=0-0")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Do error: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusPartialContent {
			t.Errorf("status = %d, want 206", resp.StatusCode)
		}
		if cr := resp.Header.Get("Content-Range"); cr != fmt.Sprintf("bytes 0-0/%d", totalSize) {
			t.Errorf("Content-Range = %q, want 'bytes 0-0/%d'", cr, totalSize)
		}
		if cl := resp.Header.Get("Content-Length"); cl != "1" {
			t.Errorf("Content-Length = %q, want '1'", cl)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}
		if len(body) != 1 {
			t.Errorf("len(body) = %d, want 1", len(body))
		}
		expectedByte := testserver.DeterministicByte(seed, 0)
		if body[0] != expectedByte {
			t.Errorf("body[0] = 0x%02X, want 0x%02X", body[0], expectedByte)
		}
	})

	t.Run("Valid_SubRange", func(t *testing.T) {
		start := int64(100)
		end := int64(599)
		length := end - start + 1

		req := newReq(t, http.MethodGet, s.URL)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Do error: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusPartialContent {
			t.Errorf("status = %d, want 206", resp.StatusCode)
		}
		if cr := resp.Header.Get("Content-Range"); cr != fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize) {
			t.Errorf("Content-Range = %q, want 'bytes %d-%d/%d'", cr, start, end, totalSize)
		}
		if cl := resp.Header.Get("Content-Length"); cl != strconv.FormatInt(length, 10) {
			t.Errorf("Content-Length = %q, want %d", cl, length)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("ReadAll error: %v", err)
		}
		if int64(len(body)) != length {
			t.Errorf("len(body) = %d, want %d", len(body), length)
		}

		for i := int64(0); i < length; i++ {
			exp := testserver.DeterministicByte(seed, start+i)
			if body[i] != exp {
				t.Fatalf("byte at offset %d = 0x%02X, want 0x%02X", start+i, body[i], exp)
			}
		}
	})

	t.Run("Suffix_Range", func(t *testing.T) {
		req := newReq(t, http.MethodGet, s.URL)
		req.Header.Set("Range", "bytes=-256")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Do error: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusPartialContent {
			t.Errorf("status = %d, want 206", resp.StatusCode)
		}
		expectedCR := fmt.Sprintf("bytes %d-%d/%d", totalSize-256, totalSize-1, totalSize)
		if cr := resp.Header.Get("Content-Range"); cr != expectedCR {
			t.Errorf("Content-Range = %q, want %q", cr, expectedCR)
		}
	})

	t.Run("OutOfRange_Returns_416", func(t *testing.T) {
		req := newReq(t, http.MethodGet, s.URL)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", totalSize+10, totalSize+50))

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Do error: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Errorf("status = %d, want 416", resp.StatusCode)
		}
		if cr := resp.Header.Get("Content-Range"); cr != fmt.Sprintf("bytes */%d", totalSize) {
			t.Errorf("Content-Range = %q, want 'bytes */%d'", cr, totalSize)
		}
	})
}

func TestServer_IfRange(t *testing.T) {
	totalSize := int64(1024)
	s := testserver.New(t,
		testserver.WithSize(totalSize),
		testserver.WithETag(testserver.ETagStrong),
	)
	client := s.Client()

	req := newReq(t, http.MethodGet, s.URL)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("initial request error: %v", err)
	}
	etag := resp.Header.Get("ETag")
	_ = resp.Body.Close()
	if etag == "" {
		t.Fatal("expected non-empty ETag")
	}

	t.Run("IfRange_Match_Returns_206", func(t *testing.T) {
		r := newReq(t, http.MethodGet, s.URL)
		r.Header.Set("Range", "bytes=0-99")
		r.Header.Set("If-Range", etag)

		res, err := client.Do(r)
		if err != nil {
			t.Fatalf("Do error: %v", err)
		}
		defer func() { _ = res.Body.Close() }()

		if res.StatusCode != http.StatusPartialContent {
			t.Errorf("status = %d, want 206", res.StatusCode)
		}
		if cl := res.Header.Get("Content-Length"); cl != "100" {
			t.Errorf("Content-Length = %q, want 100", cl)
		}
	})

	t.Run("IfRange_Mismatch_Returns_200_Full", func(t *testing.T) {
		r := newReq(t, http.MethodGet, s.URL)
		r.Header.Set("Range", "bytes=0-99")
		r.Header.Set("If-Range", `"different-mismatched-etag"`)

		res, err := client.Do(r)
		if err != nil {
			t.Fatalf("Do error: %v", err)
		}
		defer func() { _ = res.Body.Close() }()

		if res.StatusCode != http.StatusOK {
			t.Errorf("status = %d, want 200 (full content on mismatch)", res.StatusCode)
		}
		if cl := res.Header.Get("Content-Length"); cl != strconv.FormatInt(totalSize, 10) {
			t.Errorf("Content-Length = %q, want %d", cl, totalSize)
		}
	})
}

func TestServer_DeterministicHash_And_Verify(t *testing.T) {
	size := int64(512 * 1024)
	seed := int64(123456789)

	s := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
	)
	client := s.Client()

	resp := getReq(t, client, s.URL)
	defer func() { _ = resp.Body.Close() }()

	if err := testserver.Verify(resp.Body, seed, size); err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
}

func TestServer_DropAfter(t *testing.T) {
	size := int64(512 * 1024)
	dropBytes := int64(64 * 1024)

	s := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithDropAfter(dropBytes, 1.0),
	)
	client := s.Client()

	resp := getReq(t, client, s.URL)
	defer func() { _ = resp.Body.Close() }()

	buf := make([]byte, 128*1024)
	var totalRead int
	var readErr error
	for {
		n, err := resp.Body.Read(buf)
		totalRead += n
		if err != nil {
			readErr = err
			break
		}
	}

	if readErr == nil || readErr == io.EOF {
		t.Fatalf("expected premature connection termination error, got: %v", readErr)
	}
	if int64(totalRead) >= size {
		t.Errorf("expected read bytes < size, got %d", totalRead)
	}
}

func TestServer_Throttling_Accuracy(t *testing.T) {
	t.Run("PerConnBPS", func(t *testing.T) {
		targetBPS := int64(256 * 1024)
		size := int64(256 * 1024)

		s := testserver.New(t,
			testserver.WithSize(size),
			testserver.WithPerConnBPS(targetBPS),
		)
		client := s.Client()

		start := time.Now()
		resp := getReq(t, client, s.URL)
		defer func() { _ = resp.Body.Close() }()

		n, err := io.Copy(io.Discard, resp.Body)
		if err != nil {
			t.Fatalf("Copy error: %v", err)
		}
		elapsed := time.Since(start)

		if n != size {
			t.Errorf("read bytes = %d, want %d", n, size)
		}

		if elapsed < 800*time.Millisecond || elapsed > 1500*time.Millisecond {
			t.Errorf("elapsed = %v, expected ~1.0s (±250ms)", elapsed)
		}
	})

	t.Run("GlobalBPS", func(t *testing.T) {
		targetBPS := int64(256 * 1024)
		size := int64(256 * 1024)

		s := testserver.New(t,
			testserver.WithSize(size),
			testserver.WithGlobalBPS(targetBPS),
		)
		client := s.Client()

		start := time.Now()
		resp := getReq(t, client, s.URL)
		defer func() { _ = resp.Body.Close() }()

		n, err := io.Copy(io.Discard, resp.Body)
		if err != nil {
			t.Fatalf("Copy error: %v", err)
		}
		elapsed := time.Since(start)

		if n != size {
			t.Errorf("read bytes = %d, want %d", n, size)
		}

		if elapsed < 800*time.Millisecond || elapsed > 1500*time.Millisecond {
			t.Errorf("elapsed = %v, expected ~1.0s (±250ms)", elapsed)
		}
	})
}

func TestServer_MaxConns(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024*1024),
		testserver.WithMaxConns(1),
		testserver.WithLatency(100*time.Millisecond),
	)
	client := s.Client()

	var wg sync.WaitGroup
	statuses := make([]int, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, s.URL, nil)
			if err != nil {
				return
			}
			resp, err := client.Do(req)
			if err == nil {
				statuses[idx] = resp.StatusCode
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()

	found429 := false
	for _, st := range statuses {
		if st == http.StatusTooManyRequests {
			found429 = true
			break
		}
	}
	if !found429 {
		t.Errorf("expected at least one 429 Too Many Requests, got statuses: %v", statuses)
	}
}

func TestServer_FailFirst(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024),
		testserver.WithFailFirst(2, http.StatusServiceUnavailable, 3*time.Second),
	)
	client := s.Client()

	resp1 := getReq(t, client, s.URL)
	if resp1.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("req 1 status = %d, want 503", resp1.StatusCode)
	}
	if ra := resp1.Header.Get("Retry-After"); ra != "3" {
		t.Errorf("Retry-After = %q, want '3'", ra)
	}
	_ = resp1.Body.Close()

	resp2 := getReq(t, client, s.URL)
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("req 2 status = %d, want 503", resp2.StatusCode)
	}
	_ = resp2.Body.Close()

	resp3 := getReq(t, client, s.URL)
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("req 3 status = %d, want 200", resp3.StatusCode)
	}
	_ = resp3.Body.Close()
}

func TestServer_ExpireAfter(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024),
		testserver.WithExpireAfter(50*time.Millisecond),
	)
	client := s.Client()

	time.Sleep(70 * time.Millisecond)

	resp := getReq(t, client, s.URL)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403 Forbidden after expiry", resp.StatusCode)
	}
}

func TestServer_OneTimeToken(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024),
		testserver.WithOneTimeToken(),
	)
	client := s.Client()

	resp1 := getReq(t, client, s.URL+"?token=single-use-123")
	if resp1.StatusCode != http.StatusOK {
		t.Errorf("req 1 status = %d, want 200", resp1.StatusCode)
	}
	_ = resp1.Body.Close()

	resp2 := getReq(t, client, s.URL+"?token=single-use-123")
	if resp2.StatusCode != http.StatusForbidden {
		t.Errorf("req 2 status = %d, want 403", resp2.StatusCode)
	}
	_ = resp2.Body.Close()
}

func TestServer_MutateETagAfter(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(2048),
		testserver.WithETag(testserver.ETagStrong),
		testserver.WithMutateETagAfter(1024),
	)
	client := s.Client()

	r1 := getReq(t, client, s.URL)
	etag1 := r1.Header.Get("ETag")
	_ = r1.Body.Close()

	rRange := newReq(t, http.MethodGet, s.URL)
	rRange.Header.Set("Range", "bytes=0-1500")
	resRange, err := client.Do(rRange)
	if err != nil {
		t.Fatalf("client.Do error: %v", err)
	}
	_, _ = io.Copy(io.Discard, resRange.Body)
	_ = resRange.Body.Close()

	r2 := getReq(t, client, s.URL)
	etag2 := r2.Header.Get("ETag")
	_ = r2.Body.Close()

	if !strings.HasSuffix(etag2, "-mutated\"") {
		t.Errorf("expected mutated ETag ending with -mutated\", got: %q", etag2)
	}
	if etag1 == etag2 {
		t.Errorf("etag should have mutated: etag1=%q, etag2=%q", etag1, etag2)
	}
}

func TestServer_ContentDisposition_RFC6266(t *testing.T) {
	name := "Dokumen unduhan 日本語.pdf"
	s := testserver.New(t,
		testserver.WithSize(100),
		testserver.WithContentDisposition(name),
	)
	client := s.Client()

	resp := getReq(t, client, s.URL)
	defer func() { _ = resp.Body.Close() }()

	cd := resp.Header.Get("Content-Disposition")
	if !strings.Contains(cd, "filename*=") {
		t.Errorf("expected RFC 6266 filename*= in Content-Disposition: %q", cd)
	}
	if !strings.Contains(cd, url.PathEscape(name)) {
		t.Errorf("expected percent-encoded filename in Content-Disposition: %q", cd)
	}
}

func TestServer_CookieAndReferer(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(100),
		testserver.WithRequireCookie("sid", "secret123"),
		testserver.WithRequireReferer("https://portal.example.com/"),
	)
	client := s.Client()

	r1 := newReq(t, http.MethodGet, s.URL)
	res1, err := client.Do(r1)
	if err != nil {
		t.Fatalf("client.Do error: %v", err)
	}
	if res1.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 Unauthorized", res1.StatusCode)
	}
	_ = res1.Body.Close()

	r2 := newReq(t, http.MethodGet, s.URL)
	r2.AddCookie(&http.Cookie{Name: "sid", Value: "secret123"})
	res2, err := client.Do(r2)
	if err != nil {
		t.Fatalf("client.Do error: %v", err)
	}
	if res2.StatusCode != http.StatusForbidden {
		t.Errorf("status = %d, want 403 Forbidden", res2.StatusCode)
	}
	_ = res2.Body.Close()

	r3 := newReq(t, http.MethodGet, s.URL)
	r3.AddCookie(&http.Cookie{Name: "sid", Value: "secret123"})
	r3.Header.Set("Referer", "https://portal.example.com/home")
	res3, err := client.Do(r3)
	if err != nil {
		t.Fatalf("client.Do error: %v", err)
	}
	if res3.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 OK", res3.StatusCode)
	}
	_ = res3.Body.Close()
}

func TestServer_GzipAndChunked(t *testing.T) {
	t.Run("Gzip_Encoding", func(t *testing.T) {
		s := testserver.New(t,
			testserver.WithSize(10*1024),
			testserver.WithGzip(),
		)
		tr := &http.Transport{DisableCompression: true}
		c := &http.Client{Transport: tr}

		resp := getReq(t, c, s.URL)
		defer func() { _ = resp.Body.Close() }()

		if ce := resp.Header.Get("Content-Encoding"); ce != "gzip" {
			t.Errorf("Content-Encoding = %q, want 'gzip'", ce)
		}

		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatalf("gzip.NewReader error: %v", err)
		}
		defer func() { _ = gz.Close() }()

		buf := new(bytes.Buffer)
		_, err = io.CopyN(buf, gz, 10*1024)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("decompress error: %v", err)
		}
		if int64(buf.Len()) != 10*1024 {
			t.Errorf("decompressed len = %d, want 10240", buf.Len())
		}
	})

	t.Run("Chunked_No_Length", func(t *testing.T) {
		s := testserver.New(t,
			testserver.WithSize(4096),
			testserver.WithChunkedNoLength(),
		)
		client := s.Client()

		resp := getReq(t, client, s.URL)
		defer func() { _ = resp.Body.Close() }()

		if cl := resp.Header.Get("Content-Length"); cl != "" {
			t.Errorf("expected omitted Content-Length, got: %q", cl)
		}
	})
}

func TestServer_Redirects(t *testing.T) {
	s := testserver.New(t,
		testserver.WithSize(1024),
		testserver.WithRedirects(3, false),
	)
	client := s.Client()

	resp := getReq(t, client, s.URL)
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 after redirect chain", resp.StatusCode)
	}

	stats := s.Stats()
	if stats.TotalRequests < 4 {
		t.Errorf("total requests = %d, want >= 4 (3 redirects + 1 final)", stats.TotalRequests)
	}
}
