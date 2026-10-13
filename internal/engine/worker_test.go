package engine_test

import (
	"arus/internal/adapter/httpfetch"
	"arus/internal/app/ports"
	"arus/internal/domain"
	"arus/internal/engine"
	"arus/internal/testutil/fakes"
	"arus/test/testserver"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func mustNewFetcher(t *testing.T, cfg httpfetch.FetcherConfig) ports.Fetcher {
	t.Helper()
	f, err := httpfetch.NewFetcher(cfg)
	if err != nil {
		t.Fatalf("NewFetcher error: %v", err)
	}
	return f
}

func TestWorker_FullSegment_DeterministicHash(t *testing.T) {
	const (
		seed = int64(12345)
		size = int64(2 * 1024 * 1024)
	)

	ts := testserver.New(t, testserver.WithSize(size), testserver.WithSeed(seed))
	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})

	store := fakes.NewFakeFileStore()
	memFile, err := store.Create("test.part", size, domain.AllocModeSparse)
	if err != nil {
		t.Fatalf("Create error: %v", err)
	}
	defer func() { _ = memFile.Close() }()

	seg := engine.NewSegState("seg-0", 0, size, 0)
	var progress atomic.Int64

	deps := engine.WorkerDeps{
		Fetcher:   fetcher,
		File:      memFile,
		URL:       ts.URL,
		Progress:  &progress,
		Validator: `"v1"`,
	}

	ctx := context.Background()
	if err := engine.RunSegment(ctx, deps, seg); err != nil {
		t.Fatalf("RunSegment failed: %v", err)
	}

	if seg.Cursor.Load() != size {
		t.Errorf("seg.Cursor = %d, want %d", seg.Cursor.Load(), size)
	}
	if progress.Load() != size {
		t.Errorf("progress = %d, want %d", progress.Load(), size)
	}

	rawFile, ok := memFile.(*fakes.MemFile)
	if !ok {
		t.Fatalf("expected *fakes.MemFile")
	}

	data := make([]byte, size)
	if _, err := rawFile.ReadAt(data, 0); err != nil {
		t.Fatalf("ReadAt error: %v", err)
	}

	hasher := sha256.New()
	hasher.Write(data)
	gotHash := hex.EncodeToString(hasher.Sum(nil))
	wantHash := testserver.ExpectedSHA256(seed, size)

	if gotHash != wantHash {
		t.Errorf("hash mismatch:\ngot:  %s\nwant: %s", gotHash, wantHash)
	}
}

func TestWorker_WithDropAfter_ResumeFromCursor(t *testing.T) {
	const (
		seed = int64(9999)
		size = int64(600 * 1024)
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithDropAfter(150*1024, 1.0),
	)

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})

	store := fakes.NewFakeFileStore()
	memFile, err := store.Create("drop.part", size, domain.AllocModeSparse)
	if err != nil {
		t.Fatalf("Create error: %v", err)
	}
	defer func() { _ = memFile.Close() }()

	seg := engine.NewSegState("seg-drop", 0, size, 0)
	var progress atomic.Int64

	deps := engine.WorkerDeps{
		Fetcher:  fetcher,
		File:     memFile,
		URL:      ts.URL,
		Progress: &progress,
		RNG: func() float64 {
			return 0.5
		},
	}

	ctx := context.Background()
	if err := engine.RunSegment(ctx, deps, seg); err != nil {
		t.Fatalf("RunSegment failed with drop-after: %v", err)
	}

	if seg.Cursor.Load() != size {
		t.Errorf("seg.Cursor = %d, want %d", seg.Cursor.Load(), size)
	}

	rawFile := memFile.(*fakes.MemFile)
	data := make([]byte, size)
	if _, err := rawFile.ReadAt(data, 0); err != nil {
		t.Fatalf("ReadAt error: %v", err)
	}

	hasher := sha256.New()
	hasher.Write(data)
	gotHash := hex.EncodeToString(hasher.Sum(nil))
	wantHash := testserver.ExpectedSHA256(seed, size)

	if gotHash != wantHash {
		t.Errorf("hash mismatch after drops:\ngot:  %s\nwant: %s", gotHash, wantHash)
	}
}

func TestWorker_503_Backoff_FakeClock(t *testing.T) {
	const (
		seed = int64(77)
		size = int64(256 * 1024)
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithFailFirst(2, http.StatusServiceUnavailable, 100*time.Millisecond),
	)

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})

	store := fakes.NewFakeFileStore()
	memFile, err := store.Create("503.part", size, domain.AllocModeSparse)
	if err != nil {
		t.Fatalf("Create error: %v", err)
	}
	defer func() { _ = memFile.Close() }()

	seg := engine.NewSegState("seg-503", 0, size, 0)
	fakeClock := fakes.NewFakeClock(time.Now())

	deps := engine.WorkerDeps{
		Fetcher: fetcher,
		File:    memFile,
		URL:     ts.URL,
		Clock:   fakeClock,
		RNG: func() float64 {
			return 0.5
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- engine.RunSegment(ctx, deps, seg)
	}()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("RunSegment failed on 503 recovery: %v", err)
			}
			if seg.Cursor.Load() != size {
				t.Errorf("seg.Cursor = %d, want %d", seg.Cursor.Load(), size)
			}
			return
		case <-ticker.C:
			fakeClock.Advance(200 * time.Millisecond)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for 503 retry")
		}
	}
}

func TestWorker_404_FatalInstant(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, nil)
	}))
	defer server.Close()

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fakes.NewFakeFileStore()
	memFile, _ := store.Create("404.part", 1024, domain.AllocModeSparse)
	defer func() { _ = memFile.Close() }()

	seg := engine.NewSegState("seg-404", 0, 1024, 0)
	deps := engine.WorkerDeps{
		Fetcher: fetcher,
		File:    memFile,
		URL:     server.URL,
	}

	err := engine.RunSegment(context.Background(), deps, seg)
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}

	var de *domain.DownloadError
	if !errors.As(err, &de) {
		t.Fatalf("expected *domain.DownloadError, got: %T (%v)", err, err)
	}
	if de.Code != domain.ErrCodeNotFound {
		t.Errorf("de.Code = %q, want %q", de.Code, domain.ErrCodeNotFound)
	}
	if de.Recoverable {
		t.Errorf("404 error should not be recoverable")
	}
}

func TestWorker_403_LinkExpired_After2Retries(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fakes.NewFakeFileStore()
	memFile, _ := store.Create("403.part", 1024, domain.AllocModeSparse)
	defer func() { _ = memFile.Close() }()

	fakeClock := fakes.NewFakeClock(time.Now())
	seg := engine.NewSegState("seg-403", 0, 1024, 0)
	deps := engine.WorkerDeps{
		Fetcher: fetcher,
		File:    memFile,
		URL:     server.URL,
		Clock:   fakeClock,
		RNG: func() float64 {
			return 0.5
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- engine.RunSegment(context.Background(), deps, seg)
	}()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("expected error on 403, got nil")
			}
			var de *domain.DownloadError
			if !errors.As(err, &de) {
				t.Fatalf("expected *domain.DownloadError, got: %v", err)
			}
			if de.Code != domain.ErrCodeLinkExpired {
				t.Errorf("de.Code = %q, want %q", de.Code, domain.ErrCodeLinkExpired)
			}
			if !de.Recoverable {
				t.Errorf("link_expired error must be recoverable")
			}
			if count := requestCount.Load(); count != 3 {
				t.Errorf("requestCount = %d, want 3 (1 initial + 2 retries)", count)
			}
			return
		case <-ticker.C:
			fakeClock.Advance(2 * time.Second)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for 403 link_expired")
		}
	}
}

func TestWorker_Stall_Recovery(t *testing.T) {
	var attempt atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := attempt.Add(1)
		if att == 1 {
			w.Header().Set("Content-Range", "bytes 0-1023/1024")
			w.Header().Set("Content-Length", "1024")
			w.WriteHeader(http.StatusPartialContent)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			_, _ = w.Write([]byte("some leading bytes"))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(300 * time.Millisecond)
			return
		}

		rangeHdr := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
		parts := strings.Split(rangeHdr, "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)
		rangeLen := end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/1024", start, end))
		w.Header().Set("Content-Length", strconv.FormatInt(rangeLen, 10))
		w.WriteHeader(http.StatusPartialContent)
		fullPayload := bytes.Repeat([]byte("A"), int(rangeLen))
		_, _ = w.Write(fullPayload)
	}))
	defer server.Close()

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{
		StallTimeout: 60 * time.Millisecond,
	})
	store := fakes.NewFakeFileStore()
	memFile, _ := store.Create("stall.part", 1024, domain.AllocModeSparse)
	defer func() { _ = memFile.Close() }()

	seg := engine.NewSegState("seg-stall", 0, 1024, 0)
	deps := engine.WorkerDeps{
		Fetcher: fetcher,
		File:    memFile,
		URL:     server.URL,
		RNG: func() float64 {
			return 0.5
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := engine.RunSegment(ctx, deps, seg); err != nil {
		t.Fatalf("RunSegment failed on stall recovery: %v", err)
	}

	if seg.Cursor.Load() != 1024 {
		t.Errorf("seg.Cursor = %d, want 1024", seg.Cursor.Load())
	}
}

func TestWorker_ContextCancel_FastExit_NoLeak(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-10485759/10485760")
		w.Header().Set("Content-Length", "10485760")
		w.WriteHeader(http.StatusPartialContent)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		buf := make([]byte, 64*1024)
		for {
			if _, err := w.Write(buf); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}))
	defer server.Close()

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fakes.NewFakeFileStore()
	memFile, _ := store.Create("cancel.part", 10*1024*1024, domain.AllocModeSparse)
	defer func() { _ = memFile.Close() }()

	seg := engine.NewSegState("seg-cancel", 0, 10*1024*1024, 0)
	deps := engine.WorkerDeps{
		Fetcher: fetcher,
		File:    memFile,
		URL:     server.URL,
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := engine.RunSegment(ctx, deps, seg)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}

	if elapsed > 300*time.Millisecond {
		t.Errorf("worker cancel exit took too long: %v (want < 300ms)", elapsed)
	}

	written := seg.Cursor.Load()
	if written <= 0 {
		t.Errorf("expected some bytes written before cancel, got %d", written)
	}
}

func TestWorker_Status200_Validation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("whole file content"))
	}))
	defer server.Close()

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fakes.NewFakeFileStore()

	t.Run("Status200_PosGreaterThanZero_WithValidator", func(t *testing.T) {
		memFile, _ := store.Create("200_val.part", 1024, domain.AllocModeSparse)
		defer func() { _ = memFile.Close() }()

		seg := engine.NewSegState("seg-200", 0, 1024, 100)
		deps := engine.WorkerDeps{
			Fetcher:   fetcher,
			File:      memFile,
			URL:       server.URL,
			Validator: `"etag-1"`,
		}

		err := engine.RunSegment(context.Background(), deps, seg)
		var de *domain.DownloadError
		if !errors.As(err, &de) || de.Code != domain.ErrCodeRemoteChanged {
			t.Fatalf("expected ErrCodeRemoteChanged, got: %v", err)
		}
	})

	t.Run("Status200_PosGreaterThanZero_WithoutValidator", func(t *testing.T) {
		memFile, _ := store.Create("200_noval.part", 1024, domain.AllocModeSparse)
		defer func() { _ = memFile.Close() }()

		seg := engine.NewSegState("seg-200", 0, 1024, 100)
		deps := engine.WorkerDeps{
			Fetcher:   fetcher,
			File:      memFile,
			URL:       server.URL,
			Validator: "",
		}

		err := engine.RunSegment(context.Background(), deps, seg)
		var de *domain.DownloadError
		if !errors.As(err, &de) || de.Code != domain.ErrCodeRangeUnsupported {
			t.Fatalf("expected ErrCodeRangeUnsupported, got: %v", err)
		}
	})
}

func TestWorker_DiskFull_Fatal(t *testing.T) {
	ts := testserver.New(t, testserver.WithSize(1024*1024))
	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})

	store := fakes.NewFakeFileStore()
	memFile, _ := store.Create("disk_full.part", 1024*1024, domain.AllocModeSparse)
	defer func() { _ = memFile.Close() }()

	rawFile := memFile.(*fakes.MemFile)
	rawFile.SetWriteErr(syscall.ENOSPC)

	seg := engine.NewSegState("seg-enospc", 0, 1024*1024, 0)
	deps := engine.WorkerDeps{
		Fetcher: fetcher,
		File:    memFile,
		URL:     ts.URL,
	}

	err := engine.RunSegment(context.Background(), deps, seg)
	var de *domain.DownloadError
	if !errors.As(err, &de) || de.Code != domain.ErrCodeDiskFull {
		t.Fatalf("expected ErrCodeDiskFull, got: %v", err)
	}
	if !de.Recoverable {
		t.Errorf("disk_full error must be recoverable")
	}
}
