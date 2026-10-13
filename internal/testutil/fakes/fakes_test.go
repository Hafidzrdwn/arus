package fakes_test

import (
	"arus/internal/domain"
	"arus/internal/testutil/fakes"
	"errors"
	"testing"
	"time"
)

func TestFakeFileStore_MemFile_SparseAndFaults(t *testing.T) {
	fs := fakes.NewFakeFileStore()

	// 1. Create file with 10 MiB sparse size
	f, err := fs.Create("C:/downloads/test.part", 10*1024*1024, domain.AllocModeSparse)
	if err != nil {
		t.Fatalf("Create() error: %v", err)
	}

	memF, ok := f.(*fakes.MemFile)
	if !ok {
		t.Fatalf("expected *fakes.MemFile, got %T", f)
	}

	// Write at offset 128 KiB (crosses 64 KiB boundary)
	payload := []byte("hello world sparse data")
	if n, err := memF.WriteAt(payload, 128*1024); err != nil || n != len(payload) {
		t.Fatalf("WriteAt() n = %d, err = %v", n, err)
	}

	// Read back at offset 128 KiB
	buf := make([]byte, len(payload))
	if n, err := memF.ReadAt(buf, 128*1024); err != nil || n != len(buf) {
		t.Fatalf("ReadAt() n = %d, err = %v", n, err)
	}
	if string(buf) != string(payload) {
		t.Errorf("ReadAt() got %q, want %q", string(buf), string(payload))
	}

	// Read from a sparse hole (should be zero bytes)
	hole := make([]byte, 10)
	if n, err := memF.ReadAt(hole, 0); err != nil || n != len(hole) {
		t.Fatalf("ReadAt() from hole n = %d, err = %v", n, err)
	}
	for i, b := range hole {
		if b != 0 {
			t.Errorf("hole byte at index %d = %d, want 0", i, b)
		}
	}

	// Test fault injection (ENOSPC simulation)
	errNoSpace := errors.New("disk full: ENOSPC")
	memF.SetWriteErr(errNoSpace)
	if _, err := memF.WriteAt([]byte("fail"), 200); !errors.Is(err, errNoSpace) {
		t.Errorf("WriteAt() with injected error got %v, want %v", err, errNoSpace)
	}
	memF.SetWriteErr(nil)

	// Sync error injection
	errSync := errors.New("sync failed")
	memF.SetSyncErr(errSync)
	if err := memF.Sync(); !errors.Is(err, errSync) {
		t.Errorf("Sync() with injected error got %v, want %v", err, errSync)
	}
	memF.SetSyncErr(nil)

	if err := memF.Sync(); err != nil {
		t.Errorf("Sync() unexpected error: %v", err)
	}
	if err := memF.Close(); err != nil {
		t.Errorf("Close() error: %v", err)
	}

	// Test Finalize with Rename conflict policy
	// Create collision file
	if _, err := fs.Create("C:/downloads/file.zip", 100, domain.AllocModeNone); err != nil {
		t.Fatalf("Create collision file error: %v", err)
	}
	if _, err := fs.Create("C:/downloads/file (1).zip", 100, domain.AllocModeNone); err != nil {
		t.Fatalf("Create collision file 2 error: %v", err)
	}

	// Finalize should generate "file (2).zip"
	finalName, err := fs.Finalize("C:/downloads/test.part", "C:/downloads/file.zip", domain.ConflictPolicyRename)
	if err != nil {
		t.Fatalf("Finalize() error: %v", err)
	}
	if finalName != "file (2).zip" {
		t.Errorf("Finalize() finalName = %q, want 'file (2).zip'", finalName)
	}
}

func TestFakeClock_And_Timer(t *testing.T) {
	start := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	clock := fakes.NewFakeClock(start)

	if !clock.Now().Equal(start) {
		t.Errorf("Now() = %v, want %v", clock.Now(), start)
	}

	timer := clock.NewTimer(5 * time.Second)

	// Advance 3 seconds (not yet expired)
	clock.Advance(3 * time.Second)
	select {
	case <-timer.C():
		t.Fatal("timer triggered prematurely")
	default:
	}

	// Advance another 2 seconds (expired)
	clock.Advance(2 * time.Second)
	select {
	case tick := <-timer.C():
		expected := start.Add(5 * time.Second)
		if !tick.Equal(expected) {
			t.Errorf("timer tick = %v, want %v", tick, expected)
		}
	default:
		t.Fatal("timer should have fired after 5s total advance")
	}

	// Test timer reset
	timer.Reset(10 * time.Second)
	clock.Advance(10 * time.Second)
	select {
	case <-timer.C():
	default:
		t.Fatal("reset timer should have fired")
	}

	// Test timer stop
	timer2 := clock.NewTimer(5 * time.Second)
	if !timer2.Stop() {
		t.Error("Stop() should return true for active timer")
	}
	clock.Advance(10 * time.Second)
	select {
	case <-timer2.C():
		t.Fatal("stopped timer should not fire")
	default:
	}
}

func TestFakeEvents(t *testing.T) {
	events := fakes.NewFakeEvents()

	if events.Count() != 0 {
		t.Errorf("initial count = %d, want 0", events.Count())
	}

	ev1 := domain.DownloadAddedEvent{ID: "01JEV001"}
	ev2 := domain.DownloadStateChangedEvent{ID: "01JEV001", State: domain.StateDownloading}

	events.Publish(ev1)
	events.Publish(ev2)

	if events.Count() != 2 {
		t.Errorf("count = %d, want 2", events.Count())
	}

	list := events.Events()
	if len(list) != 2 {
		t.Fatalf("len(list) = %d, want 2", len(list))
	}

	events.Clear()
	if events.Count() != 0 {
		t.Errorf("count after clear = %d, want 0", events.Count())
	}
}

func TestSeqIDGen(t *testing.T) {
	gen := fakes.NewSeqIDGen("TEST")
	id1 := gen.New()
	id2 := gen.New()

	if id1 == id2 {
		t.Errorf("expected distinct IDs, got id1=%q, id2=%q", id1, id2)
	}
	if string(id1) >= string(id2) {
		t.Errorf("expected strictly monotonic IDs: id1=%q, id2=%q", id1, id2)
	}
}
