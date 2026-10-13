package fsx_test

import (
	"arus/internal/adapter/fsx"
	"arus/internal/domain"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStore_SparseAllocation_SpeedAndSize(t *testing.T) {
	tempDir := t.TempDir()
	partFile := filepath.Join(tempDir, "sparse_2gib.part")

	store := fsx.New()
	size := int64(2 * 1024 * 1024 * 1024) // 2 GiB

	start := time.Now()
	f, err := store.Create(partFile, size, domain.AllocModeSparse)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Create sparse file failed: %v", err)
	}
	defer func() { _ = f.Close() }()

	if elapsed > 200*time.Millisecond {
		t.Errorf("sparse allocation took %v, want < 200ms", elapsed)
	}

	fi, err := os.Stat(partFile)
	if err != nil {
		t.Fatalf("Stat error: %v", err)
	}
	if fi.Size() != size {
		t.Errorf("logical size = %d, want %d", fi.Size(), size)
	}

	physical, err := getPhysicalSize(partFile)
	if err != nil {
		t.Logf("could not query physical size: %v", err)
		return
	}

	// If filesystem does not support sparse (e.g. exFAT/FAT32 temp drive), skip physical size check
	if physical >= size {
		t.Skipf("underlying filesystem does not support sparse files (physical %d == logical %d)", physical, size)
	}

	// Sparse file on NTFS/ext4/APFS should allocate negligible disk space (< 64 MiB)
	if physical > 64*1024*1024 {
		t.Errorf("physical disk usage = %d bytes, want ≪ 2 GiB (< 64 MiB)", physical)
	}
}

func TestStore_ConcurrentWriteAt_16Goroutines(t *testing.T) {
	tempDir := t.TempDir()
	partFile := filepath.Join(tempDir, "concurrent.part")

	store := fsx.New()
	const numWorkers = 16
	const chunkLen = 32 * 1024 // 32 KiB per worker
	totalSize := int64(numWorkers * chunkLen)

	f, err := store.Create(partFile, totalSize, domain.AllocModeSparse)
	if err != nil {
		t.Fatalf("Create error: %v", err)
	}
	defer func() { _ = f.Close() }()

	expectedPayloads := make([][]byte, numWorkers)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		workerID := i
		payload := make([]byte, chunkLen)
		for j := range payload {
			payload[j] = byte((workerID*31 + j) % 256)
		}
		expectedPayloads[workerID] = payload

		wg.Add(1)
		go func() {
			defer wg.Done()
			offset := int64(workerID * chunkLen)
			n, werr := f.WriteAt(payload, offset)
			if werr != nil {
				t.Errorf("worker %d WriteAt error: %v", workerID, werr)
			}
			if n != chunkLen {
				t.Errorf("worker %d wrote %d bytes, want %d", workerID, n, chunkLen)
			}
		}()
	}
	wg.Wait()

	if err := f.Sync(); err != nil {
		t.Fatalf("Sync error: %v", err)
	}

	//nolint:gosec // reading test file
	readBack, err := os.ReadFile(partFile)
	if err != nil {
		t.Fatalf("ReadFile error: %v", err)
	}
	if int64(len(readBack)) != totalSize {
		t.Fatalf("readBack len = %d, want %d", len(readBack), totalSize)
	}

	for i := 0; i < numWorkers; i++ {
		offset := i * chunkLen
		slice := readBack[offset : offset+chunkLen]
		if !bytes.Equal(slice, expectedPayloads[i]) {
			t.Fatalf("data mismatch at worker %d segment (offset %d)", i, offset)
		}
	}
}

func TestStore_Finalize_ConflictPolicies(t *testing.T) {
	store := fsx.New()

	t.Run("Overwrite", func(t *testing.T) {
		tempDir := t.TempDir()
		part := filepath.Join(tempDir, "download.part")
		final := filepath.Join(tempDir, "file.txt")

		_ = os.WriteFile(part, []byte("new part content"), 0o600)
		_ = os.WriteFile(final, []byte("old existing content"), 0o600)

		name, err := store.Finalize(part, final, domain.ConflictPolicyOverwrite)
		if err != nil {
			t.Fatalf("Finalize Overwrite error: %v", err)
		}
		if name != "file.txt" {
			t.Errorf("name = %q, want 'file.txt'", name)
		}

		//nolint:gosec // reading test file
		content, _ := os.ReadFile(final)
		if string(content) != "new part content" {
			t.Errorf("overwritten content mismatch: %q", string(content))
		}
	})

	t.Run("Ask_ReturnsConflict", func(t *testing.T) {
		tempDir := t.TempDir()
		part := filepath.Join(tempDir, "download.part")
		final := filepath.Join(tempDir, "file.txt")

		_ = os.WriteFile(part, []byte("part content"), 0o600)
		_ = os.WriteFile(final, []byte("existing"), 0o600)

		_, err := store.Finalize(part, final, domain.ConflictPolicyAsk)
		if !errors.Is(err, fsx.ErrConflict) {
			t.Fatalf("expected ErrConflict, got: %v", err)
		}
	})

	t.Run("Rename_SequentialNumbering", func(t *testing.T) {
		tempDir := t.TempDir()
		part1 := filepath.Join(tempDir, "download1.part")
		part2 := filepath.Join(tempDir, "download2.part")
		final := filepath.Join(tempDir, "doc.pdf")

		_ = os.WriteFile(final, []byte("original"), 0o600)
		_ = os.WriteFile(part1, []byte("part 1"), 0o600)
		_ = os.WriteFile(part2, []byte("part 2"), 0o600)

		name1, err := store.Finalize(part1, final, domain.ConflictPolicyRename)
		if err != nil {
			t.Fatalf("Finalize part1 error: %v", err)
		}
		if name1 != "doc (1).pdf" {
			t.Errorf("name1 = %q, want 'doc (1).pdf'", name1)
		}

		name2, err := store.Finalize(part2, final, domain.ConflictPolicyRename)
		if err != nil {
			t.Fatalf("Finalize part2 error: %v", err)
		}
		if name2 != "doc (2).pdf" {
			t.Errorf("name2 = %q, want 'doc (2).pdf'", name2)
		}
	})
}

func TestStore_Finalize_SecurityGuards(t *testing.T) {
	tempDir := t.TempDir()
	store := fsx.New()

	part := filepath.Join(tempDir, "test.part")
	_ = os.WriteFile(part, []byte("data"), 0o600)

	t.Run("Reject_PathTraversal", func(t *testing.T) {
		traversal := filepath.Join(tempDir, "..", "escaped.txt")
		_, err := store.Finalize(part, traversal, domain.ConflictPolicyOverwrite)
		if err == nil {
			t.Fatal("expected error on path traversal, got nil")
		}
	})

	t.Run("Reject_ReservedName", func(t *testing.T) {
		reserved := filepath.Join(tempDir, "CON.txt")
		_, err := store.Finalize(part, reserved, domain.ConflictPolicyOverwrite)
		if err == nil {
			t.Fatal("expected error on reserved device name, got nil")
		}
	})
}

type mockFSInfoProvider struct {
	fsInfo    domain.FSInfo
	freeSpace int64
}

func (m *mockFSInfoProvider) FSInfo(_ string) (domain.FSInfo, error) {
	return m.fsInfo, nil
}

func (m *mockFSInfoProvider) FreeSpace(_ string) (int64, error) {
	return m.freeSpace, nil
}

func TestStore_FAT32_SizeLimit_Rejection(t *testing.T) {
	tempDir := t.TempDir()
	mock := &mockFSInfoProvider{
		fsInfo: domain.FSInfo{
			FSName:      "FAT32",
			MaxFileSize: 4*1024*1024*1024 - 1,
			FreeBytes:   100 * 1024 * 1024 * 1024,
		},
		freeSpace: 100 * 1024 * 1024 * 1024,
	}

	store := fsx.NewWithFSInfo(mock)
	fiveGiB := int64(5 * 1024 * 1024 * 1024)

	partFile := filepath.Join(tempDir, "large_file.part")
	_, err := store.Create(partFile, fiveGiB, domain.AllocModeSparse)
	if err == nil {
		t.Fatal("expected error when allocating 5 GiB on FAT32, got nil")
	}

	var de *domain.DownloadError
	if !errors.As(err, &de) {
		t.Fatalf("expected *domain.DownloadError, got: %T (%v)", err, err)
	}
	if de.Code != domain.ErrCodeFSLimit {
		t.Errorf("de.Code = %q, want ErrCodeFSLimit", de.Code)
	}
}

func TestStore_FreeSpace_Rejection(t *testing.T) {
	tempDir := t.TempDir()
	mock := &mockFSInfoProvider{
		fsInfo: domain.FSInfo{
			FSName:      "NTFS",
			MaxFileSize: 1 << 60,
			FreeBytes:   1000,
		},
		freeSpace: 1000, // Only 1000 bytes available
	}

	store := fsx.NewWithFSInfo(mock)
	twoKiB := int64(2048) // Needs ~2068 bytes (2048 * 1.01)

	partFile := filepath.Join(tempDir, "cannot_fit.part")
	_, err := store.Create(partFile, twoKiB, domain.AllocModeSparse)
	if err == nil {
		t.Fatal("expected error when disk has insufficient space, got nil")
	}

	var de *domain.DownloadError
	if !errors.As(err, &de) {
		t.Fatalf("expected *domain.DownloadError, got: %T (%v)", err, err)
	}
	if de.Code != domain.ErrCodeDiskFull {
		t.Errorf("de.Code = %q, want ErrCodeDiskFull", de.Code)
	}
}
