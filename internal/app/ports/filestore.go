package ports

import (
	"arus/internal/domain"
	"io"
)

// File abstracts a handle to an allocated part file on disk.
// All methods must be safe for concurrent use by multiple worker goroutines.
type File interface {
	io.WriterAt
	io.Closer

	// Sync flushes kernel buffers to stable persistent storage.
	Sync() error
}

// FileStore defines the outbound driven port for filesystem allocation, write access, and finalization.
// Implementations must handle platform-specific filesystem behaviors (NTFS sparse, FAT32 limits).
type FileStore interface {
	// Create creates or truncates a file at path with the given pre-allocation mode and size.
	// Returns a thread-safe File handle ready for concurrent WriteAt calls.
	Create(path string, size int64, mode domain.AllocMode) (File, error)

	// Open opens an existing part file for resuming downloads.
	Open(path string) (File, error)

	// Finalize atomically moves and renames partPath to finalPath according to ConflictPolicy.
	// Returns the determined final filename (e.g. "file (1).zip" on rename collision).
	Finalize(partPath, finalPath string, policy domain.ConflictPolicy) (finalName string, err error)

	// FreeSpace returns available free bytes on the volume hosting dir.
	FreeSpace(dir string) (int64, error)

	// FSInfo queries filesystem properties such as filesystem name (NTFS, FAT32) and maximum file size.
	FSInfo(dir string) (domain.FSInfo, error)
}
