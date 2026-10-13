package engine_test

import (
	"arus/internal/adapter/fsx"
	"arus/internal/adapter/httpfetch"
	"arus/internal/domain"
	"arus/internal/engine"
	"arus/test/testserver"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func calcFileSHA256(t *testing.T, path string) string {
	t.Helper()
	//nolint:gosec // test helper
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("os.Open error: %v", err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatalf("io.Copy error: %v", err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func TestIntegration_RequireCookieAndReferer(t *testing.T) {
	const (
		seed = int64(11111)
		size = int64(2 * 1024 * 1024)
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithRequireCookie("session_token", "valid_secret_xyz"),
		testserver.WithRequireReferer("https://portal.example.com/downloads"),
	)

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fsx.New()

	t.Run("FailsWithoutAuthContext", func(t *testing.T) {
		tempDir := t.TempDir()
		partPath := filepath.Join(tempDir, "unauth.arus-part")

		dl := domain.Download{
			ID:       "dl-unauth",
			URL:      ts.URL,
			PartPath: partPath,
			Opts:     domain.Options{MaxConns: 2},
		}

		job := engine.NewJob(engine.JobDeps{
			Fetcher:   fetcher,
			FileStore: store,
		}, dl)

		_, err := job.Run(context.Background())
		if err == nil {
			t.Fatalf("expected error without cookie/referer, got nil")
		}
		if job.State() != domain.StateFailed {
			t.Errorf("job.State = %v, want %v", job.State(), domain.StateFailed)
		}
	})

	t.Run("SucceedsWithValidContext", func(t *testing.T) {
		tempDir := t.TempDir()
		partPath := filepath.Join(tempDir, "auth.arus-part")

		dl := domain.Download{
			ID:       "dl-auth",
			URL:      ts.URL,
			PartPath: partPath,
			Opts:     domain.Options{MaxConns: 2},
			ReqCtx: domain.RequestContext{
				Referer: "https://portal.example.com/downloads/page",
				Cookies: []domain.Cookie{
					{
						Name:   "session_token",
						Value:  "valid_secret_xyz",
						Path:   "/",
						Secure: false,
					},
				},
			},
		}

		job := engine.NewJob(engine.JobDeps{
			Fetcher:   fetcher,
			FileStore: store,
		}, dl)

		res, err := job.Run(context.Background())
		if err != nil {
			t.Fatalf("job.Run failed with valid auth context: %v", err)
		}
		if res.BytesWritten != size {
			t.Errorf("BytesWritten = %d, want %d", res.BytesWritten, size)
		}

		wantHash := testserver.ExpectedSHA256(seed, size)
		gotHash := calcFileSHA256(t, partPath)
		if gotHash != wantHash {
			t.Errorf("hash mismatch:\ngot:  %s\nwant: %s", gotHash, wantHash)
		}
	})
}

func TestIntegration_LinkExpiredAndRefresh(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		seed = int64(77777)
		size = int64(100 * 1024 * 1024)
	)

	ts1 := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithPerConnBPS(4*1024*1024),
		testserver.WithExpireAfter(2*time.Second),
	)

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fsx.New()

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "expired_refresh.arus-part")

	dl := domain.Download{
		ID:       "dl-expired-refresh",
		URL:      ts1.URL + "/download",
		PartPath: partPath,
		Opts: domain.Options{
			MaxConns: 4,
		},
	}

	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: store,
	}, dl)

	_, run1Err := job.Run(context.Background())
	if run1Err == nil {
		t.Fatalf("expected error from expiring server, got nil")
	}

	if job.State() != domain.StateFailed {
		t.Fatalf("expected job state Failed, got %v", job.State())
	}

	var dlErr *domain.DownloadError
	if !errors.As(run1Err, &dlErr) || dlErr.Code != domain.ErrCodeLinkExpired {
		t.Fatalf("expected ErrCodeLinkExpired, got: %v", run1Err)
	}

	downloadedBeforeRefresh := job.Download().Downloaded
	snap1 := job.Snapshot()
	var sumCursors int64
	for _, s := range snap1.Segments {
		sumCursors += s.Cursor - s.Start
	}
	if sumCursors <= 0 && downloadedBeforeRefresh <= 0 {
		t.Fatalf("expected cursor > 0 before refresh, got sumCursors=%d, downloaded=%d", sumCursors, downloadedBeforeRefresh)
	}

	ts2 := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
	)

	newURL := ts2.URL + "/download"
	if err := job.UpdateSource(newURL, domain.RequestContext{}); err != nil {
		t.Fatalf("UpdateSource failed: %v", err)
	}

	if job.State() != domain.StateProbing {
		t.Fatalf("expected state Probing after UpdateSource, got %v", job.State())
	}

	res, run2Err := job.Run(context.Background())
	if run2Err != nil {
		t.Fatalf("job.Run resume failed: %v", run2Err)
	}

	if job.State() != domain.StateCompleted {
		t.Fatalf("expected job state Completed, got %v", job.State())
	}

	if res.BytesWritten != size {
		t.Errorf("res.BytesWritten = %d, want %d", res.BytesWritten, size)
	}

	wantHash := testserver.ExpectedSHA256(seed, size)
	gotHash := calcFileSHA256(t, partPath)
	if gotHash != wantHash {
		t.Errorf("hash mismatch:\ngot:  %s\nwant: %s", gotHash, wantHash)
	}

	b1 := ts1.Stats().TotalBytesSent
	b2 := ts2.Stats().TotalBytesSent
	totalServerBytes := b1 + b2
	maxAllowedBytes := size + int64(len(snap1.Segments))*int64(engine.BufferSize)

	if b1 <= 0 || b2 <= 0 {
		t.Errorf("expected both servers to transfer data, got b1=%d, b2=%d", b1, b2)
	}
	if totalServerBytes > maxAllowedBytes {
		t.Errorf("total bytes transferred (%d) exceeded budget (%d): b1=%d, b2=%d",
			totalServerBytes, maxAllowedBytes, b1, b2)
	}
}

func TestIntegration_UpdateSource_Mismatch(t *testing.T) {
	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fsx.New()
	tempDir := t.TempDir()

	t.Run("SizeMismatch", func(t *testing.T) {
		ts1 := testserver.New(t, testserver.WithSize(10*1024*1024))
		ts2 := testserver.New(t, testserver.WithSize(20*1024*1024))

		dl := domain.Download{
			ID:        "dl-size-mismatch",
			URL:       ts1.URL,
			State:     domain.StateFailed,
			TotalSize: 10 * 1024 * 1024,
			PartPath:  filepath.Join(tempDir, "size_mismatch.arus-part"),
			Opts:      domain.Options{MaxConns: 1},
		}

		job := engine.NewJob(engine.JobDeps{
			Fetcher:   fetcher,
			FileStore: store,
		}, dl)

		err := job.UpdateSource(ts2.URL, domain.RequestContext{})
		if !errors.Is(err, engine.ErrSourceMismatch) {
			t.Fatalf("expected ErrSourceMismatch for differing size, got: %v", err)
		}
	})

	t.Run("ETagMismatch", func(t *testing.T) {
		tsOrig := testserver.New(t,
			testserver.WithSize(5*1024*1024),
			testserver.WithETag(testserver.ETagStrong),
		)

		differServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("ETag", `"completely-different-etag"`)
			w.Header().Set("Content-Range", "bytes 0-0/5242880")
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("x"))
		}))
		defer differServer.Close()

		dl := domain.Download{
			ID:        "dl-etag-mismatch",
			URL:       tsOrig.URL,
			State:     domain.StateFailed,
			TotalSize: 5 * 1024 * 1024,
			Validators: domain.Validators{
				ETag: `"original-etag"`,
			},
			PartPath: filepath.Join(tempDir, "etag_mismatch.arus-part"),
			Opts:     domain.Options{MaxConns: 1},
		}

		job := engine.NewJob(engine.JobDeps{
			Fetcher:   fetcher,
			FileStore: store,
		}, dl)

		err := job.UpdateSource(differServer.URL, domain.RequestContext{})
		if !errors.Is(err, engine.ErrSourceMismatch) {
			t.Fatalf("expected ErrSourceMismatch for differing ETag, got: %v", err)
		}
	})
}

func TestSessionContext_HeaderFiltering(t *testing.T) {
	if !httpfetch.IsHeaderAllowed("User-Agent") {
		t.Errorf("User-Agent should be allowed")
	}
	if !httpfetch.IsHeaderAllowed("Authorization") {
		t.Errorf("Authorization should be allowed")
	}
	if !httpfetch.IsHeaderAllowed("X-Custom-Token") {
		t.Errorf("X-Custom-Token should be allowed")
	}
	if httpfetch.IsHeaderAllowed("Host") {
		t.Errorf("Host should be denied")
	}
	if httpfetch.IsHeaderAllowed("Range") {
		t.Errorf("Range should be denied")
	}
	if httpfetch.IsHeaderAllowed("If-None-Match") {
		t.Errorf("If-* should be denied")
	}
	if httpfetch.IsHeaderAllowed("Sec-Fetch-Dest") {
		t.Errorf("Sec-Fetch-* should be denied")
	}
	if httpfetch.IsHeaderAllowed("Sec-Fetch-Mode") {
		t.Errorf("Sec-Fetch-* should be denied")
	}
}
