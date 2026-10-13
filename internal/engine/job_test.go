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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func fileSHA256(t *testing.T, path string) string {
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

func TestJob_256MiB_Deterministic_Connections1_4_8(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 256 MiB test in short mode")
	}

	const (
		seed = int64(12345)
		size = int64(256 * 1024 * 1024) // 256 MiB
	)

	ts := testserver.New(t, testserver.WithSize(size), testserver.WithSeed(seed))
	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{
		MaxRedirects: 10,
	})
	store := fsx.New()

	wantHash := testserver.ExpectedSHA256(seed, size)
	connsList := []int{1, 4, 8}
	hashes := make(map[int]string)

	for _, conns := range connsList {
		t.Run(fmt.Sprintf("Conns=%d", conns), func(t *testing.T) {
			tempDir := t.TempDir()
			partPath := filepath.Join(tempDir, fmt.Sprintf("file_%d.arus-part", conns))

			dl := domain.Download{
				ID:       domain.DownloadID(fmt.Sprintf("dl-256m-%d", conns)),
				URL:      ts.URL,
				PartPath: partPath,
				Opts: domain.Options{
					MaxConns: conns,
				},
			}

			deps := engine.JobDeps{
				Fetcher:   fetcher,
				FileStore: store,
			}

			job := engine.NewJob(deps, dl)
			res, err := job.Run(context.Background())
			if err != nil {
				t.Fatalf("job.Run error with %d conns: %v", conns, err)
			}

			if res.BytesWritten != size {
				t.Errorf("BytesWritten = %d, want %d", res.BytesWritten, size)
			}

			gotHash := fileSHA256(t, partPath)
			if gotHash != wantHash {
				t.Errorf("hash mismatch for %d conns:\ngot:  %s\nwant: %s", conns, gotHash, wantHash)
			}
			hashes[conns] = gotHash
		})
	}

	if hashes[1] != hashes[4] || hashes[4] != hashes[8] {
		t.Errorf("part files differ across connection counts: conns=1:%s conns=4:%s conns=8:%s",
			hashes[1], hashes[4], hashes[8])
	}
}

func BenchmarkJob_Conns(b *testing.B) {
	const (
		size = int64(15 * 1024 * 1024) // 15 MiB
		seed = int64(54321)
		bps  = int64(5 * 1024 * 1024) // 5 MiB/s per connection
	)

	ts := testserver.New(b,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithPerConnBPS(bps),
	)

	fetcher := mustNewFetcher(b, httpfetch.FetcherConfig{})
	store := fsx.New()

	runOnce := func(conns int) (float64, time.Duration) {
		tempDir := b.TempDir()
		partPath := filepath.Join(tempDir, fmt.Sprintf("bench_%d.arus-part", conns))

		dl := domain.Download{
			ID:       domain.DownloadID(fmt.Sprintf("bench-%d", conns)),
			URL:      ts.URL,
			PartPath: partPath,
			Opts: domain.Options{
				MaxConns: conns,
			},
		}

		job := engine.NewJob(engine.JobDeps{
			Fetcher:   fetcher,
			FileStore: store,
		}, dl)

		start := time.Now()
		res, err := job.Run(context.Background())
		dur := time.Since(start)
		if err != nil {
			b.Fatalf("job.Run failed: %v", err)
		}

		throughput := float64(res.BytesWritten) / dur.Seconds() / (1024 * 1024) // MiB/s
		return throughput, dur
	}

	b.ResetTimer()
	b.ReportAllocs()

	var tp1, tp8 float64
	var dur1, dur8 time.Duration

	for b.Loop() {
		tp1, dur1 = runOnce(1)
		tp8, dur8 = runOnce(8)
	}

	ratio := tp8 / tp1
	fmt.Printf("\n[BenchmarkJob_Conns Results]\n")
	fmt.Printf("1 Connection:  %.2f MiB/s (took %v)\n", tp1, dur1)
	fmt.Printf("8 Connections: %.2f MiB/s (took %v)\n", tp8, dur8)
	fmt.Printf("Throughput Ratio (8 Conns / 1 Conn): %.2fx\n", ratio)

	if ratio < 5.0 {
		b.Fatalf("benchmark failed: 8 connections ratio %.2fx is less than required 5x", ratio)
	}
}

func TestJob_NoRange_ChunkedNoLength(t *testing.T) {
	const (
		seed = int64(456)
		size = int64(1024 * 1024) // 1 MiB
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithRanges(false),
		testserver.WithChunkedNoLength(),
	)

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fsx.New()

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "chunked.arus-part")

	dl := domain.Download{
		ID:       "dl-chunked",
		URL:      ts.URL,
		PartPath: partPath,
		Opts: domain.Options{
			MaxConns: 4,
		},
	}

	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: store,
	}, dl)

	res, err := job.Run(context.Background())
	if err != nil {
		t.Fatalf("job.Run error on chunked: %v", err)
	}

	if res.BytesWritten != size {
		t.Errorf("BytesWritten = %d, want %d", res.BytesWritten, size)
	}

	gotHash := fileSHA256(t, partPath)
	wantHash := testserver.ExpectedSHA256(seed, size)
	if gotHash != wantHash {
		t.Errorf("hash mismatch:\ngot:  %s\nwant: %s", gotHash, wantHash)
	}
}

func TestJob_UnverifiedRange_FallbackSingleStream(t *testing.T) {
	const (
		seed = int64(888)
		size = int64(1024 * 1024)
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithRanges(false),
	)

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fsx.New()

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "unverified.arus-part")

	dl := domain.Download{
		ID:           "dl-unverified",
		URL:          ts.URL,
		PartPath:     partPath,
		RangeSupport: domain.RangeSupportUnverified,
		Opts: domain.Options{
			MaxConns: 4,
		},
	}

	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: store,
	}, dl)

	res, err := job.Run(context.Background())
	if err != nil {
		t.Fatalf("job.Run error on unverified fallback: %v", err)
	}

	if res.BytesWritten != size {
		t.Errorf("BytesWritten = %d, want %d", res.BytesWritten, size)
	}

	gotHash := fileSHA256(t, partPath)
	wantHash := testserver.ExpectedSHA256(seed, size)
	if gotHash != wantHash {
		t.Errorf("hash mismatch:\ngot:  %s\nwant: %s", gotHash, wantHash)
	}
}

func TestJob_Pause_SnapshotConsistent_NoGoleak(t *testing.T) {
	const (
		seed = int64(98765)
		size = int64(50 * 1024 * 1024) // 50 MiB
		bps  = int64(10 * 1024 * 1024) // 10 MiB/s
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithPerConnBPS(bps),
	)

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{})
	store := fsx.New()

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "pause.arus-part")

	dl := domain.Download{
		ID:       "dl-pause",
		URL:      ts.URL,
		PartPath: partPath,
		Opts: domain.Options{
			MaxConns: 4,
		},
	}

	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: store,
	}, dl)

	runErrCh := make(chan error, 1)
	go func() {
		_, err := job.Run(context.Background())
		runErrCh <- err
	}()

	time.Sleep(100 * time.Millisecond)

	pausedSegs := job.Pause()
	if len(pausedSegs) == 0 {
		t.Errorf("expected non-empty segments returned from Pause")
	}

	runErr := <-runErrCh
	if !errors.Is(runErr, engine.ErrPaused) {
		t.Errorf("expected ErrPaused from Run(), got: %v", runErr)
	}

	snap := job.Snapshot()
	if snap.State != domain.StatePaused {
		t.Errorf("snap.State = %v, want %v", snap.State, domain.StatePaused)
	}

	if err := domain.ValidateInvariants(snap.Segments, size); err != nil {
		t.Fatalf("ValidateInvariants failed on paused snapshot: %v", err)
	}

	ts.Close()
	goleak.VerifyNone(t)
}
