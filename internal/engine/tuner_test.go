package engine_test

import (
	"arus/internal/adapter/fsx"
	"arus/internal/adapter/httpfetch"
	"arus/internal/domain"
	"arus/internal/engine"
	"arus/test/testserver"
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestTuner_Decide_SyntheticSequence(t *testing.T) {
	cfg := engine.DefaultTunerConfig()
	t0 := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	state := engine.NewTunerState(cfg, 2)

	state, act := engine.Decide(t0, state, engine.Sample{
		ThroughputEMA: 1000,
		Conns:         2,
	})
	if act.Kind != engine.ActionHold || act.TargetConns != 2 {
		t.Fatalf("first sample: want Hold at 2, got %v at %d", act.Kind, act.TargetConns)
	}

	state, act = engine.Decide(t0.Add(4*time.Second), state, engine.Sample{
		ThroughputEMA: 1100,
		Conns:         2,
	})
	if act.Kind != engine.ActionInc || act.TargetConns != 3 {
		t.Fatalf("increase >8%%: want Inc at 3, got %v at %d", act.Kind, act.TargetConns)
	}

	state, act = engine.Decide(t0.Add(8*time.Second), state, engine.Sample{
		ThroughputEMA: 1150,
		Conns:         3,
	})
	if act.Kind != engine.ActionHold || act.TargetConns != 3 {
		t.Fatalf("plateau <=8%%: want Hold at 3, got %v at %d", act.Kind, act.TargetConns)
	}

	state, act = engine.Decide(t0.Add(12*time.Second), state, engine.Sample{
		ThroughputEMA: 1300,
		Conns:         3,
	})
	if act.Kind != engine.ActionInc || act.TargetConns != 4 {
		t.Fatalf("increase >8%%: want Inc at 4, got %v at %d", act.Kind, act.TargetConns)
	}

	state, act = engine.Decide(t0.Add(16*time.Second), state, engine.Sample{
		ThroughputEMA: 1000,
		Conns:         4,
	})
	if act.Kind != engine.ActionDec || act.TargetConns != 3 {
		t.Fatalf("revert drop >15%%: want Dec at 3, got %v at %d", act.Kind, act.TargetConns)
	}

	state, act = engine.Decide(t0.Add(25*time.Second), state, engine.Sample{
		ThroughputEMA: 2500,
		Conns:         3,
	})
	if act.Kind != engine.ActionHold || act.TargetConns != 3 {
		t.Fatalf("during cooldown: want Hold at 3, got %v at %d", act.Kind, act.TargetConns)
	}

	state, act = engine.Decide(t0.Add(47*time.Second), state, engine.Sample{
		ThroughputEMA: 1200,
		Conns:         3,
	})
	if act.Kind != engine.ActionInc || act.TargetConns != 4 {
		t.Fatalf("after cooldown: want Inc at 4, got %v at %d", act.Kind, act.TargetConns)
	}

	state, act = engine.Decide(t0.Add(51*time.Second), state, engine.Sample{
		ThroughputEMA: 1200,
		Conns:         4,
		ErrWindow: engine.ErrWindow{
			Status429: 1,
		},
		RetryAfter: 2 * time.Second,
	})
	if act.Kind != engine.ActionHalveAndWait || act.TargetConns != 2 || act.Wait != 2*time.Second {
		t.Fatalf("on 429: want HalveAndWait at 2 (wait 2s), got %v at %d (wait %v)", act.Kind, act.TargetConns, act.Wait)
	}

	state, act = engine.Decide(t0.Add(55*time.Second), state, engine.Sample{
		ThroughputEMA: 1200,
		Conns:         2,
		ErrWindow: engine.ErrWindow{
			Status503: 1,
		},
	})
	if act.Kind != engine.ActionHalveAndWait || act.TargetConns != 1 {
		t.Fatalf("on 503: want HalveAndWait at 1, got %v at %d", act.Kind, act.TargetConns)
	}

	_, act = engine.Decide(t0.Add(59*time.Second), state, engine.Sample{
		ThroughputEMA: 1200,
		Conns:         4,
		ErrWindow: engine.ErrWindow{
			Resets:   2,
			Timeouts: 1,
		},
	})
	if act.Kind != engine.ActionHalveAndWait || act.TargetConns != 2 {
		t.Fatalf("on 3 errors: want HalveAndWait at 2, got %v at %d", act.Kind, act.TargetConns)
	}
}

func TestTuner_Decide_BoundsAndHardCap(t *testing.T) {
	cfg := engine.TunerConfig{
		MinConns:         1,
		MaxConnsPerJob:   8,
		HardCap:          6,
		CooldownDuration: 30 * time.Second,
	}
	t0 := time.Now()
	state := engine.NewTunerState(cfg, 6)
	state.LastThroughput = 1000

	state, act := engine.Decide(t0, state, engine.Sample{
		ThroughputEMA: 2000,
		Conns:         6,
	})
	if act.Kind != engine.ActionHold || act.TargetConns != 6 {
		t.Fatalf("at hard cap 6: want Hold at 6, got %v at %d", act.Kind, act.TargetConns)
	}

	state.LastConns = 1
	_, act = engine.Decide(t0.Add(time.Second), state, engine.Sample{
		ThroughputEMA: 2000,
		Conns:         1,
		ErrWindow: engine.ErrWindow{
			Status429: 1,
		},
	})
	if act.TargetConns != 1 {
		t.Fatalf("halve at min 1: want 1, got %d", act.TargetConns)
	}
}

func TestTuner_AntiOscillation(t *testing.T) {
	cfg := engine.DefaultTunerConfig()
	t0 := time.Now()
	state := engine.NewTunerState(cfg, 4)
	state.LastThroughput = 1000

	conns := 4
	changes := 0

	fluctuations := []float64{
		1020, 990, 1040, 980, 1010, 1030, 990, 1000,
		1020, 970, 1010, 1030, 990, 1010, 1000,
	}

	for i, tp := range fluctuations {
		now := t0.Add(time.Duration((i+1)*4) * time.Second)
		var act engine.Action
		state, act = engine.Decide(now, state, engine.Sample{
			ThroughputEMA: tp,
			Conns:         conns,
		})
		if act.TargetConns != conns {
			changes++
			conns = act.TargetConns
		}
	}

	if changes > 6 {
		t.Fatalf("expected <= 6 connection changes in 60s steady state, got %d", changes)
	}
}

func TestHostProfiles(t *testing.T) {
	hp := engine.NewHostProfiles(8)

	if hp.SafeMaxConns("example.com") != 8 {
		t.Fatalf("expected default 8, got %d", hp.SafeMaxConns("example.com"))
	}

	hp.Learn429("http://example.com:8080/download", 5)
	if hp.SafeMaxConns("example.com") != 4 {
		t.Fatalf("expected 4, got %d", hp.SafeMaxConns("example.com"))
	}

	if hp.SafeMaxConns("https://EXAMPLE.COM/") != 4 {
		t.Fatalf("expected case-insensitive 4, got %d", hp.SafeMaxConns("https://EXAMPLE.COM/"))
	}

	hp.Learn429("example.com", 6)
	if hp.SafeMaxConns("example.com") != 4 {
		t.Fatalf("should not increase limit on 429, got %d", hp.SafeMaxConns("example.com"))
	}

	hp.Learn429("example.com", 3)
	if hp.SafeMaxConns("example.com") != 2 {
		t.Fatalf("expected updated lower limit 2, got %d", hp.SafeMaxConns("example.com"))
	}
}

func TestConnBudget(t *testing.T) {
	b := engine.NewConnBudget(10, 4)

	g1 := b.TryAcquire("host1", 0, 4, 8)
	if g1 != 4 {
		t.Fatalf("host1: want 4, got %d", g1)
	}

	g2 := b.TryAcquire("host1", 4, 6, 8)
	if g2 != 4 {
		t.Fatalf("host1 exceed host cap: want 4, got %d", g2)
	}

	g3 := b.TryAcquire("host2", 0, 4, 8)
	if g3 != 4 {
		t.Fatalf("host2: want 4, got %d", g3)
	}

	g4 := b.TryAcquire("host3", 0, 4, 8)
	if g4 != 2 {
		t.Fatalf("host3 global ceiling: want 2, got %d", g4)
	}

	b.Release("host1", 4)
	g5 := b.TryAcquire("host3", 2, 4, 8)
	if g5 != 4 {
		t.Fatalf("host3 after release: want 4, got %d", g5)
	}

	g6 := b.TryAcquire("host4", 0, 4, 2)
	if g6 != 2 {
		t.Fatalf("host4 safeCeiling 2: want 2, got %d", g6)
	}
}

func TestErrorTracker(t *testing.T) {
	et := &engine.ErrorTracker{}

	et.Record429(3 * time.Second)
	et.Record503(5 * time.Second)
	et.RecordReset()
	et.RecordTimeout()

	win, ra := et.Drain()
	if win.Status429 != 1 || win.Status503 != 1 || win.Resets != 1 || win.Timeouts != 1 {
		t.Fatalf("unexpected drained window: %+v", win)
	}
	if ra != 5*time.Second {
		t.Fatalf("expected retry-after 5s, got %v", ra)
	}

	win2, ra2 := et.Drain()
	if win2.Total() != 0 || ra2 != 0 {
		t.Fatalf("second drain must be empty, got %+v %v", win2, ra2)
	}
}

func TestIntegration_TunerPlateau_GlobalBPS(t *testing.T) {
	const (
		seed = int64(12345)
		size = int64(12 * 1024 * 1024)
		bps  = int64(20 * 1024 * 1024)
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithGlobalBPS(bps),
	)

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "plateau.arus-part")

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job := engine.NewJob(engine.JobDeps{
		Fetcher:       fetcher,
		FileStore:     fsx.New(),
		TunerInterval: 50 * time.Millisecond,
		TunerConfig: &engine.TunerConfig{
			MinConns:         1,
			MaxConnsPerJob:   16,
			HardCap:          32,
			CooldownDuration: 30 * time.Second,
		},
	}, domain.Download{
		ID:       "dl-tuner-plateau",
		URL:      ts.URL,
		PartPath: partPath,
		Opts: domain.Options{
			MaxConns: 4,
		},
	})

	res, err := job.Run(context.Background())
	if err != nil {
		t.Fatalf("job.Run failed: %v", err)
	}

	if res.BytesWritten != size {
		t.Fatalf("bytes written mismatch: got %d, want %d", res.BytesWritten, size)
	}

	gotHash := fileSHA256(t, partPath)
	wantHash := testserver.ExpectedSHA256(seed, size)
	if gotHash != wantHash {
		t.Fatalf("hash mismatch:\ngot:  %s\nwant: %s", gotHash, wantHash)
	}

	target := job.TargetConns()
	if target > 8 {
		t.Fatalf("expected tuner to plateau at <= 8 connections under global BPS cap, got %d", target)
	}
}

func TestIntegration_TunerAdapt_ServerMaxConns(t *testing.T) {
	const (
		seed = int64(54321)
		size = int64(12 * 1024 * 1024)
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithMaxConns(4),
		testserver.WithPerConnBPS(2*1024*1024),
	)

	profiles := engine.NewHostProfiles(8)
	tempDir := t.TempDir()
	part1 := filepath.Join(tempDir, "job1.arus-part")

	fetcher1 := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job1 := engine.NewJob(engine.JobDeps{
		Fetcher:       fetcher1,
		FileStore:     fsx.New(),
		HostProfiles:  profiles,
		TunerInterval: 25 * time.Millisecond,
		TunerConfig: &engine.TunerConfig{
			MinConns:         1,
			MaxConnsPerJob:   8,
			HardCap:          16,
			CooldownDuration: 100 * time.Millisecond,
		},
	}, domain.Download{
		ID:       "dl-tuner-adapt-1",
		URL:      ts.URL,
		PartPath: part1,
	})

	res1, err := job1.Run(context.Background())
	if err != nil {
		t.Fatalf("job1.Run failed: %v", err)
	}
	if res1.BytesWritten != size {
		t.Fatalf("job1 bytes written mismatch: got %d, want %d", res1.BytesWritten, size)
	}

	hash1 := fileSHA256(t, part1)
	wantHash := testserver.ExpectedSHA256(seed, size)
	if hash1 != wantHash {
		t.Fatalf("job1 hash mismatch: got %s, want %s", hash1, wantHash)
	}

	learnedMax := profiles.SafeMaxConns(ts.URL)
	if learnedMax > 4 {
		t.Fatalf("expected learned SafeMaxConns <= 4, got %d", learnedMax)
	}

	part2 := filepath.Join(tempDir, "job2.arus-part")
	fetcher2 := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job2 := engine.NewJob(engine.JobDeps{
		Fetcher:       fetcher2,
		FileStore:     fsx.New(),
		HostProfiles:  profiles,
		TunerInterval: 50 * time.Millisecond,
	}, domain.Download{
		ID:       "dl-tuner-adapt-2",
		URL:      ts.URL,
		PartPath: part2,
		Opts: domain.Options{
			MaxConns: 8,
		},
	})

	res2, err := job2.Run(context.Background())
	if err != nil {
		t.Fatalf("job2.Run failed: %v", err)
	}
	if res2.BytesWritten != size {
		t.Fatalf("job2 bytes written mismatch: got %d, want %d", res2.BytesWritten, size)
	}

	hash2 := fileSHA256(t, part2)
	if hash2 != wantHash {
		t.Fatalf("job2 hash mismatch: got %s, want %s", hash2, wantHash)
	}
}
