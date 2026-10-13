package engine_test

import (
	"arus/internal/adapter/fsx"
	"arus/internal/adapter/httpfetch"
	"arus/internal/domain"
	"arus/internal/engine"
	"arus/test/testserver"
	"context"
	"errors"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLimiter_LimitProvider(t *testing.T) {
	var lp engine.LimitProvider = engine.StaticLimitProvider(5 * 1024 * 1024)
	if lp.Limit() != 5*1024*1024 {
		t.Fatalf("expected 5 MiB/s, got %d", lp.Limit())
	}

	thr := engine.NewThrottle(2 * 1024 * 1024)
	var lp2 engine.LimitProvider = thr
	if lp2.Limit() != 2*1024*1024 {
		t.Fatalf("expected 2 MiB/s from Throttle, got %d", lp2.Limit())
	}
}

func TestLimiter_SingleSleep_NoExcessWait(t *testing.T) {
	global := engine.NewThrottle(2 * 1024 * 1024) // 2 MiB/s
	job := engine.NewThrottle(2 * 1024 * 1024)    // 2 MiB/s
	lim := engine.NewLimiterSet(global, job)

	ctx := context.Background()
	// Drain initial tokens
	_ = lim.WaitN(ctx, 2*1024*1024)

	t0 := time.Now()
	// Request 256 KiB at 2 MiB/s -> delay should be ~125 ms, NOT 250 ms
	err := lim.WaitN(ctx, 256*1024)
	duration := time.Since(t0)
	if err != nil {
		t.Fatalf("WaitN failed: %v", err)
	}

	// 256 KiB at 2 MiB/s is 125 ms. With tolerance: 100 ms to 190 ms.
	if duration < 90*time.Millisecond || duration > 220*time.Millisecond {
		t.Fatalf("expected single-sleep delay ~125ms, got %v", duration)
	}
}

func TestLimiter_SingleJob_Accuracy(t *testing.T) {
	const (
		seed = int64(11111)
		size = int64(25 * 1024 * 1024) // 25 MiB
		bps  = int64(2 * 1024 * 1024)  // 2 MiB/s
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
	)

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "accuracy.arus-part")

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: fsx.New(),
	}, domain.Download{
		ID:       "dl-limit-accuracy",
		URL:      ts.URL,
		PartPath: partPath,
		Opts: domain.Options{
			MaxConns:      4,
			SpeedLimitBps: bps,
		},
	})

	testDuration := 4 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	t0 := time.Now()
	_, err := job.Run(ctx)
	actualDuration := time.Since(t0)

	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) && !errors.Is(err, engine.ErrPaused) {
		t.Fatalf("unexpected job error: %v", err)
	}

	downloaded := job.Snapshot().Progress.Downloaded
	expectedBytes := float64(bps) * actualDuration.Seconds()
	diff := math.Abs(float64(downloaded) - expectedBytes)
	ratio := diff / expectedBytes

	if ratio > 0.10 {
		t.Fatalf("rate limit accuracy deviated by %.2f%% (downloaded=%d, expected=%.0f, ratio=%.4f)",
			ratio*100, downloaded, expectedBytes, ratio)
	}
}

func TestLimiter_TwoJobs_Fairness(t *testing.T) {
	const (
		seed      = int64(22222)
		size      = int64(30 * 1024 * 1024) // 30 MiB
		globalBPS = int64(4 * 1024 * 1024)  // 4 MiB/s global limit
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
	)

	globalThrottle := engine.NewThrottle(globalBPS)

	tempDir1 := t.TempDir()
	part1 := filepath.Join(tempDir1, "fair1.arus-part")
	fetcher1 := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job1 := engine.NewJob(engine.JobDeps{
		Fetcher:       fetcher1,
		FileStore:     fsx.New(),
		GlobalLimiter: globalThrottle,
	}, domain.Download{
		ID:       "dl-fair-1",
		URL:      ts.URL,
		PartPath: part1,
		Opts:     domain.Options{MaxConns: 2},
	})

	tempDir2 := t.TempDir()
	part2 := filepath.Join(tempDir2, "fair2.arus-part")
	fetcher2 := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job2 := engine.NewJob(engine.JobDeps{
		Fetcher:       fetcher2,
		FileStore:     fsx.New(),
		GlobalLimiter: globalThrottle,
	}, domain.Download{
		ID:       "dl-fair-2",
		URL:      ts.URL,
		PartPath: part2,
		Opts:     domain.Options{MaxConns: 2},
	})

	testDuration := 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = job1.Run(ctx)
	}()

	go func() {
		defer wg.Done()
		_, _ = job2.Run(ctx)
	}()

	wg.Wait()

	dl1 := float64(job1.Snapshot().Progress.Downloaded)
	dl2 := float64(job2.Snapshot().Progress.Downloaded)
	total := dl1 + dl2

	expectedTotal := float64(globalBPS) * testDuration.Seconds()
	totalRatio := math.Abs(total-expectedTotal) / expectedTotal
	if totalRatio > 0.20 {
		t.Fatalf("total bandwidth deviated by %.2f%%: total=%.0f, expected=%.0f", totalRatio*100, total, expectedTotal)
	}

	expectedPerJob := total / 2.0
	diff1 := math.Abs(dl1-expectedPerJob) / expectedPerJob
	diff2 := math.Abs(dl2-expectedPerJob) / expectedPerJob

	if diff1 > 0.20 || diff2 > 0.20 {
		t.Fatalf("fairness deviated by >20%%: job1=%.0f (%.2f%%), job2=%.0f (%.2f%%), expected=%.0f each",
			dl1, diff1*100, dl2, diff2*100, expectedPerJob)
	}
}

func TestLimiter_RuntimeAdjustment(t *testing.T) {
	const (
		seed     = int64(33333)
		size     = int64(25 * 1024 * 1024) // 25 MiB
		startBPS = int64(1 * 1024 * 1024)  // 1 MiB/s
		boostBPS = int64(4 * 1024 * 1024)  // 4 MiB/s
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
	)

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "runtime.arus-part")

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: fsx.New(),
	}, domain.Download{
		ID:       "dl-runtime-adjust",
		URL:      ts.URL,
		PartPath: partPath,
		Opts: domain.Options{
			MaxConns:      4,
			SpeedLimitBps: startBPS,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := job.Run(ctx)
		errCh <- err
	}()

	time.Sleep(1200 * time.Millisecond)
	bytesPhase1 := float64(job.Snapshot().Progress.Downloaded)
	ratePhase1 := bytesPhase1 / 1.2 // bytes per second

	tBoost := time.Now()
	job.SetSpeedLimit(boostBPS)

	time.Sleep(1500 * time.Millisecond)
	bytesTotal := float64(job.Snapshot().Progress.Downloaded)
	durationPhase2 := time.Since(tBoost).Seconds()
	bytesPhase2 := bytesTotal - bytesPhase1
	ratePhase2 := bytesPhase2 / durationPhase2

	cancel()
	_ = <-errCh

	if ratePhase1 > 1.3*float64(startBPS) {
		t.Fatalf("phase 1 rate %.0f exceeded 1.3 MiB/s limit", ratePhase1)
	}

	if ratePhase2 < 2.5*1024*1024 {
		t.Fatalf("phase 2 rate %.0f did not accelerate to >= 2.5 MiB/s within < 1 s (boosted to 4 MiB/s)", ratePhase2)
	}

	speedup := ratePhase2 / ratePhase1
	if speedup < 2.2 {
		t.Fatalf("runtime speedup ratio %.2f was less than expected 2.2x increase", speedup)
	}
}

func BenchmarkLimiter_Unlimited_ZeroAlloc(b *testing.B) {
	ctx := context.Background()
	lim := engine.NewLimiterSet(nil, engine.NewThrottle(0))

	allocs := testing.AllocsPerRun(1000, func() {
		_ = lim.WaitN(ctx, 256*1024)
	})
	if allocs != 0 {
		b.Fatalf("expected 0 allocs/op, got %f", allocs)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = lim.WaitN(ctx, 256*1024)
	}
}
