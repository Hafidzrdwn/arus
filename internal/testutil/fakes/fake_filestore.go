package fakes

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	_ ports.FileStore = (*FakeFileStore)(nil)
	_ ports.File      = (*MemFile)(nil)
)

const memBlockSize = 64 * 1024

// MemFile is an in-memory, sparse-aware implementation of ports.File with fault injection hooks.
type MemFile struct {
	mu       sync.RWMutex
	path     string
	size     int64
	mode     domain.AllocMode
	closed   bool
	blocks   map[int64][]byte
	writeErr error
	syncErr  error
}

// WriteAt writes p to the sparse in-memory blocks starting at byte offset off.
func (f *MemFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return 0, errors.New("file closed")
	}
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if len(p) == 0 {
		return 0, nil
	}

	totalWritten := 0
	for totalWritten < len(p) {
		currOff := off + int64(totalWritten)
		blockIdx := currOff / memBlockSize
		blockOff := currOff % memBlockSize
		chunkLen := int64(memBlockSize) - blockOff
		remaining := int64(len(p) - totalWritten)
		if remaining < chunkLen {
			chunkLen = remaining
		}

		block, exists := f.blocks[blockIdx]
		if !exists {
			block = make([]byte, memBlockSize)
			f.blocks[blockIdx] = block
		}

		copy(block[blockOff:], p[totalWritten:totalWritten+int(chunkLen)])
		totalWritten += int(chunkLen)
	}

	if off+int64(len(p)) > f.size {
		f.size = off + int64(len(p))
	}

	return totalWritten, nil
}

// ReadAt reads len(p) bytes from the sparse file at byte offset off.
func (f *MemFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return 0, errors.New("file closed")
	}
	if off >= f.size {
		return 0, os.ErrNotExist
	}

	totalRead := 0
	for totalRead < len(p) {
		currOff := off + int64(totalRead)
		if currOff >= f.size {
			break
		}
		blockIdx := currOff / memBlockSize
		blockOff := currOff % memBlockSize
		chunkLen := int64(memBlockSize) - blockOff
		remaining := int64(len(p) - totalRead)
		if remaining < chunkLen {
			chunkLen = remaining
		}
		if currOff+chunkLen > f.size {
			chunkLen = f.size - currOff
		}

		block, exists := f.blocks[blockIdx]
		if exists {
			copy(p[totalRead:totalRead+int(chunkLen)], block[blockOff:])
		} else {
			// Sparse hole: zeros
			for i := 0; i < int(chunkLen); i++ {
				p[totalRead+i] = 0
			}
		}
		totalRead += int(chunkLen)
	}

	return totalRead, nil
}

// Sync flushes data to simulated storage.
func (f *MemFile) Sync() error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.closed {
		return errors.New("file closed")
	}
	if f.syncErr != nil {
		return f.syncErr
	}
	return nil
}

// Close closes the in-memory file.
func (f *MemFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return errors.New("file already closed")
	}
	f.closed = true
	return nil
}

// Size returns the virtual allocated or written size of the file.
func (f *MemFile) Size() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.size
}

// AllocMode returns the allocation mode used when creating the file.
func (f *MemFile) AllocMode() domain.AllocMode {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mode
}

// SetWriteErr injects an error (e.g. ENOSPC) on subsequent WriteAt operations.
func (f *MemFile) SetWriteErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeErr = err
}

// SetSyncErr injects an error on subsequent Sync operations.
func (f *MemFile) SetSyncErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncErr = err
}

// FakeFileStore is an in-memory test double for ports.FileStore.
type FakeFileStore struct {
	mu        sync.RWMutex
	files     map[string]*MemFile
	freeSpace int64
	fsInfo    domain.FSInfo
}

// NewFakeFileStore creates a new in-memory file store with default 100 GiB free space on NTFS.
func NewFakeFileStore() *FakeFileStore {
	return &FakeFileStore{
		files:     make(map[string]*MemFile),
		freeSpace: 100 * 1024 * 1024 * 1024,
		fsInfo: domain.FSInfo{
			FSName:      "NTFS",
			MaxFileSize: 16 * 1024 * 1024 * 1024 * 1024, // 16 TiB
		},
	}
}

// Create creates a new in-memory sparse file.
func (s *FakeFileStore) Create(path string, size int64, mode domain.AllocMode) (ports.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	mf := &MemFile{
		path:   path,
		size:   size,
		mode:   mode,
		blocks: make(map[int64][]byte),
	}
	s.files[filepath.Clean(path)] = mf
	return mf, nil
}

// Open opens an existing in-memory file.
func (s *FakeFileStore) Open(path string) (ports.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cleanPath := filepath.Clean(path)
	mf, exists := s.files[cleanPath]
	if !exists {
		return nil, os.ErrNotExist
	}
	mf.closed = false
	return mf, nil
}

// Finalize moves partPath to finalPath obeying the specified ConflictPolicy.
func (s *FakeFileStore) Finalize(partPath, finalPath string, policy domain.ConflictPolicy) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cleanPart := filepath.Clean(partPath)
	cleanFinal := filepath.Clean(finalPath)

	mf, exists := s.files[cleanPart]
	if !exists {
		return "", os.ErrNotExist
	}

	targetPath := cleanFinal
	if _, collision := s.files[targetPath]; collision {
		switch policy {
		case domain.ConflictPolicyOverwrite:
			// Overwrite in place
		case domain.ConflictPolicyRename:
			// Generate non-colliding name (1), (2), etc.
			dir := filepath.Dir(cleanFinal)
			base := filepath.Base(cleanFinal)
			ext := filepath.Ext(base)
			nameOnly := strings.TrimSuffix(base, ext)

			counter := 1
			for {
				candidateName := fmt.Sprintf("%s (%d)%s", nameOnly, counter, ext)
				candidatePath := filepath.Join(dir, candidateName)
				if _, exists := s.files[candidatePath]; !exists {
					targetPath = candidatePath
					break
				}
				counter++
			}
		case domain.ConflictPolicyAsk:
			return "", errors.New("conflict policy ask requires user intervention")
		default:
			return "", fmt.Errorf("unknown conflict policy: %q", policy)
		}
	}

	delete(s.files, cleanPart)
	mf.path = targetPath
	s.files[targetPath] = mf
	return filepath.Base(targetPath), nil
}

// FreeSpace returns simulated available bytes on the target directory volume.
func (s *FakeFileStore) FreeSpace(_ string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.freeSpace, nil
}

// FSInfo returns filesystem metadata for the volume.
func (s *FakeFileStore) FSInfo(_ string) (domain.FSInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fsInfo, nil
}

// SetFreeSpace updates simulated free space bytes.
func (s *FakeFileStore) SetFreeSpace(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.freeSpace = bytes
}

// SetFSInfo updates simulated filesystem info.
func (s *FakeFileStore) SetFSInfo(info domain.FSInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fsInfo = info
}

// GetFile retrieves an in-memory file by path for test inspection.
func (s *FakeFileStore) GetFile(path string) *MemFile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.files[filepath.Clean(path)]
}
