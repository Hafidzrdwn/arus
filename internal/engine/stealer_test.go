package engine_test

import (
	"arus/internal/adapter/fsx"
	"arus/internal/adapter/httpfetch"
	"arus/internal/app/ports"
	"arus/internal/domain"
	"arus/internal/engine"
	"arus/test/testserver"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

type mockCheckpointer struct {
	mu    sync.Mutex
	dirty int
}

func (m *mockCheckpointer) MarkDirty() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirty++
}

func (m *mockCheckpointer) DirtyCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dirty
}

type recordingFile struct {
	mu         sync.Mutex
	data       []byte
	byteWrites []int32
	totalBytes int64
}

func newRecordingFile(size int64) *recordingFile {
	return &recordingFile{
		data:       make([]byte, size),
		byteWrites: make([]int32, size),
	}
}

func (r *recordingFile) WriteAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := copy(r.data[off:], p)
	for i := 0; i < n; i++ {
		r.byteWrites[off+int64(i)]++
	}
	r.totalBytes += int64(n)
	return n, nil
}

func (r *recordingFile) Sync() error {
	return nil
}

func (r *recordingFile) Close() error {
	return nil
}

type recordingStore struct {
	ports.FileStore
	file *recordingFile
}

func (s *recordingStore) Create(_ string, _ int64, _ domain.AllocMode) (ports.File, error) {
	return s.file, nil
}

func TestRegistry_Unit_OrphanPriorityAndSplitting(t *testing.T) {
	cp := &mockCheckpointer{}
	s0 := engine.NewSegState("seg-0", 0, 4*engine.MinSplit, 0)
	s1 := engine.NewSegState("seg-1", 4*engine.MinSplit, 8*engine.MinSplit, 4*engine.MinSplit)

	reg := engine.NewRegistry([]*engine.SegState{s0, s1}, cp)

	seg0, ok := reg.Claim("w-1")
	if !ok || seg0 != s0 {
		t.Fatalf("expected claim of s0, got %v, ok=%v", seg0, ok)
	}
	if !s0.IsActive() {
		t.Fatal("s0 should be marked active")
	}

	seg1, ok := reg.Claim("w-2")
	if !ok || seg1 != s1 {
		t.Fatalf("expected claim of s1, got %v, ok=%v", seg1, ok)
	}
	if !s1.IsActive() {
		t.Fatal("s1 should be marked active")
	}

	reg.Orphan(s0)
	if s0.IsActive() {
		t.Fatal("s0 should not be active after orphan")
	}
	if cp.DirtyCount() != 1 {
		t.Fatalf("expected 1 dirty notification, got %d", cp.DirtyCount())
	}

	claimedOrphan, ok := reg.Claim("w-3")
	if !ok || claimedOrphan != s0 {
		t.Fatalf("expected orphan s0 to be claimed with priority, got %v", claimedOrphan)
	}

	stolen, ok := reg.Claim("w-idle")
	if !ok || stolen == nil {
		t.Fatal("expected successful steal from active segment")
	}
	if cp.DirtyCount() != 2 {
		t.Fatalf("expected 2 dirty notifications after steal, got %d", cp.DirtyCount())
	}
	if reg.SplitCount() != 1 {
		t.Fatalf("expected 1 split count, got %d", reg.SplitCount())
	}

	snaps := reg.Snapshot()
	if err := domain.ValidateInvariants(snaps, 8*engine.MinSplit); err != nil {
		t.Fatalf("invariant violation on snapshot: %v", err)
	}
}

var (
	baselineOnce     sync.Once
	baselineDuration time.Duration
)

func TestStealer_Speedup_ThrottledConnection(t *testing.T) {
	const (
		seed = int64(98765)
		size = int64(12 * 1024 * 1024)
	)

	wantHash := testserver.ExpectedSHA256(seed, size)

	serverOpts := []testserver.Option{
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithRequestThrottle(func(_ int, r *http.Request) int64 {
			if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
				return 1500 * 1024 // 1.5 MiB/s (slow connection)
			}
			return 40 * 1024 * 1024 // 40 MiB/s (fast connections)
		}),
	}

	baselineOnce.Do(func() {
		tsNoSteal := testserver.New(t, serverOpts...)
		defer tsNoSteal.Close()
		tempDirNoSteal := t.TempDir()
		partNoSteal := filepath.Join(tempDirNoSteal, "file_nosteal.arus-part")

		fetcherNoSteal := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
		jobNoSteal := engine.NewJob(engine.JobDeps{
			Fetcher:   fetcherNoSteal,
			FileStore: fsx.New(),
		}, domain.Download{
			ID:       "dl-nosteal",
			URL:      tsNoSteal.URL,
			PartPath: partNoSteal,
			Opts:     domain.Options{MaxConns: 4},
		})
		jobNoSteal.SetDisableStealing(true)

		t0 := time.Now()
		resNoSteal, err := jobNoSteal.Run(context.Background())
		baselineDuration = time.Since(t0)
		if err != nil {
			t.Fatalf("jobNoSteal.Run failed: %v", err)
		}
		if resNoSteal.BytesWritten != size {
			t.Fatalf("bytes written mismatch: got %d, want %d", resNoSteal.BytesWritten, size)
		}
		hashNoSteal := fileSHA256(t, partNoSteal)
		if hashNoSteal != wantHash {
			t.Fatalf("hash mismatch without stealing: got %s, want %s", hashNoSteal, wantHash)
		}
	})

	durationNoSteal := baselineDuration

	tsSteal := testserver.New(t, serverOpts...)
	tempDirSteal := t.TempDir()
	partSteal := filepath.Join(tempDirSteal, "file_steal.arus-part")

	fetcherSteal := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	jobSteal := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcherSteal,
		FileStore: fsx.New(),
	}, domain.Download{
		ID:       "dl-steal",
		URL:      tsSteal.URL,
		PartPath: partSteal,
		Opts:     domain.Options{MaxConns: 4},
	})

	t1 := time.Now()
	resSteal, err := jobSteal.Run(context.Background())
	durationSteal := time.Since(t1)
	if err != nil {
		t.Fatalf("jobSteal.Run failed: %v", err)
	}
	if resSteal.BytesWritten != size {
		t.Fatalf("bytes written mismatch: got %d, want %d", resSteal.BytesWritten, size)
	}
	hashSteal := fileSHA256(t, partSteal)
	if hashSteal != wantHash {
		t.Fatalf("hash mismatch with stealing: got %s, want %s", hashSteal, wantHash)
	}

	splitCount := jobSteal.SplitCount()
	if splitCount < 1 {
		t.Fatalf("expected split count >= 1, got %d", splitCount)
	}

	speedRatio := float64(durationSteal) / float64(durationNoSteal)
	if speedRatio >= 0.60 {
		t.Fatalf("stealing speed ratio %.2f exceeded threshold 0.60 (steal=%v, no-steal=%v)", speedRatio, durationSteal, durationNoSteal)
	}
}

func TestStealer_WriteAtRecorder_NoOverlap(t *testing.T) {
	const (
		seed = int64(54321)
		size = int64(12 * 1024 * 1024)
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithRequestThrottle(func(_ int, r *http.Request) int64 {
			if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
				return 5 * 1024 * 1024
			}
			return 50 * 1024 * 1024
		}),
	)

	recFile := newRecordingFile(size)
	recStore := &recordingStore{
		FileStore: fsx.New(),
		file:      recFile,
	}
	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})

	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: recStore,
	}, domain.Download{
		ID:       "dl-record",
		URL:      ts.URL,
		PartPath: "dummy.part",
		Opts:     domain.Options{MaxConns: 4},
	})

	res, err := job.Run(context.Background())
	if err != nil {
		t.Fatalf("job.Run failed: %v", err)
	}
	if res.BytesWritten != size {
		t.Fatalf("bytes written mismatch: got %d, want %d", res.BytesWritten, size)
	}

	splitCount := job.SplitCount()
	if splitCount < 1 {
		t.Fatalf("expected split count >= 1, got %d", splitCount)
	}

	for i := int64(0); i < size; i++ {
		if recFile.byteWrites[i] < 1 {
			t.Fatalf("byte at offset %d was never written", i)
		}
	}

	maxAllowedTotal := size + (splitCount * engine.BufferSize)
	if recFile.totalBytes > maxAllowedTotal {
		t.Fatalf("total written bytes %d exceeded max allowed %d (size=%d, splits=%d)",
			recFile.totalBytes, maxAllowedTotal, size, splitCount)
	}

	h := sha256.Sum256(recFile.data)
	actualHash := hex.EncodeToString(h[:])
	wantHash := testserver.ExpectedSHA256(seed, size)
	if actualHash != wantHash {
		t.Fatalf("final hash mismatch: got %s, want %s", actualHash, wantHash)
	}
}

var propCounter atomic.Int64

func TestStealer_PropertyRandomized_InvariantI2(t *testing.T) {
	sizes := []int64{
		6 * 1024 * 1024,
		8 * 1024 * 1024,
		12 * 1024 * 1024,
		16 * 1024 * 1024,
	}

	idx := int(propCounter.Add(1)-1) % len(sizes)
	sz := sizes[idx]
	seed := int64(1000 + idx)
	conns := 2 + (idx % 4)

	ts := testserver.New(t,
		testserver.WithSize(sz),
		testserver.WithSeed(seed),
		testserver.WithRequestThrottle(func(_ int, r *http.Request) int64 {
			if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
				return 4 * 1024 * 1024
			}
			return 50 * 1024 * 1024
		}),
	)

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "file_prop.arus-part")

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: fsx.New(),
	}, domain.Download{
		ID:       domain.DownloadID(fmt.Sprintf("dl-prop-%d", idx)),
		URL:      ts.URL,
		PartPath: partPath,
		Opts:     domain.Options{MaxConns: conns},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var snapErr atomic.Pointer[error]
	var snapCount atomic.Int64
	doneCh := make(chan struct{})

	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-doneCh:
				return
			case <-ticker.C:
				snap := job.Snapshot()
				if len(snap.Segments) > 0 {
					if err := domain.ValidateInvariants(snap.Segments, sz); err != nil {
						snapErr.Store(&err)
						return
					}
					snapCount.Add(1)
				}
			}
		}
	}()

	res, err := job.Run(ctx)
	close(doneCh)

	if err != nil {
		t.Fatalf("job.Run failed: %v", err)
	}
	if res.BytesWritten != sz {
		t.Fatalf("bytes written mismatch: got %d, want %d", res.BytesWritten, sz)
	}

	if pErr := snapErr.Load(); pErr != nil {
		t.Fatalf("invariant I2 violated during snapshot: %v", *pErr)
	}

	if snapCount.Load() == 0 {
		t.Fatal("expected at least one snapshot during download")
	}

	wantHash := testserver.ExpectedSHA256(seed, sz)
	gotHash := fileSHA256(t, partPath)
	if gotHash != wantHash {
		t.Fatalf("hash mismatch: got %s, want %s", gotHash, wantHash)
	}
}

func TestStealer_CancelMidSplit_NoLeak(t *testing.T) {
	const (
		seed = int64(11223)
		size = int64(16 * 1024 * 1024)
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithRequestThrottle(func(_ int, r *http.Request) int64 {
			if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
				return 512 * 1024
			}
			return 20 * 1024 * 1024
		}),
	)

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "file_cancel.arus-part")

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: fsx.New(),
	}, domain.Download{
		ID:       "dl-cancel",
		URL:      ts.URL,
		PartPath: partPath,
		Opts:     domain.Options{MaxConns: 4},
	})

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(25*time.Millisecond, cancel)

	_, err := job.Run(ctx)
	if err == nil {
		t.Fatal("expected error due to cancellation, got nil")
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, engine.ErrPaused) {
		t.Fatalf("unexpected cancellation error: %v", err)
	}

	ts.Close()
	goleak.VerifyNone(t)
}

func TestJob_SetTargetConns_DynamicRetireAndSpawn(t *testing.T) {
	const (
		seed = int64(44556)
		size = int64(12 * 1024 * 1024)
	)

	ts := testserver.New(t,
		testserver.WithSize(size),
		testserver.WithSeed(seed),
		testserver.WithPerConnBPS(4*1024*1024),
	)

	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "file_dynamic.arus-part")

	fetcher := mustNewFetcher(t, httpfetch.FetcherConfig{MaxRedirects: 5})
	job := engine.NewJob(engine.JobDeps{
		Fetcher:   fetcher,
		FileStore: fsx.New(),
	}, domain.Download{
		ID:       "dl-dynamic",
		URL:      ts.URL,
		PartPath: partPath,
		Opts:     domain.Options{MaxConns: 2},
	})

	time.AfterFunc(20*time.Millisecond, func() {
		job.SetTargetConns(4)
	})

	time.AfterFunc(60*time.Millisecond, func() {
		job.SetTargetConns(2)
	})

	res, err := job.Run(context.Background())
	if err != nil {
		t.Fatalf("job.Run failed: %v", err)
	}
	if res.BytesWritten != size {
		t.Fatalf("bytes written mismatch: got %d, want %d", res.BytesWritten, size)
	}

	wantHash := testserver.ExpectedSHA256(seed, size)
	gotHash := fileSHA256(t, partPath)
	if gotHash != wantHash {
		t.Fatalf("hash mismatch: got %s, want %s", gotHash, wantHash)
	}
}
