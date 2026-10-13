package fsx

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	_ ports.FileStore = (*Store)(nil)
	_ ports.File      = (*File)(nil)
)

// ErrConflict indicates the destination file already exists and ConflictPolicy is ask.
var ErrConflict = errors.New("destination file already exists")

// FSInfoProvider abstracts filesystem capacity and metadata querying for testing.
type FSInfoProvider interface {
	FreeSpace(dir string) (int64, error)
	FSInfo(dir string) (domain.FSInfo, error)
}

type defaultFSInfoProvider struct{}

func (d *defaultFSInfoProvider) FreeSpace(dir string) (int64, error) {
	return freeSpaceOS(dir)
}

func (d *defaultFSInfoProvider) FSInfo(dir string) (domain.FSInfo, error) {
	return fsInfoOS(dir)
}

// Store implements ports.FileStore providing zero-merge pre-allocated file storage.
type Store struct {
	fsInfoProvider FSInfoProvider
}

// New constructs a FileStore using standard operating system filesystem drivers.
func New() *Store {
	return &Store{
		fsInfoProvider: &defaultFSInfoProvider{},
	}
}

// NewWithFSInfo constructs a FileStore with an injected FSInfoProvider (for testing).
func NewWithFSInfo(provider FSInfoProvider) *Store {
	return &Store{
		fsInfoProvider: provider,
	}
}

// Create creates a file at path with the specified pre-allocation mode and size.
func (s *Store) Create(path string, size int64, mode domain.AllocMode) (ports.File, error) {
	dir := filepath.Dir(path)

	if size > 0 {
		info, err := s.fsInfoProvider.FSInfo(dir)
		if err == nil && info.MaxFileSize > 0 && size > info.MaxFileSize {
			return nil, domain.NewDownloadError(
				domain.ErrCodeFSLimit,
				fmt.Sprintf("file size %d exceeds %s limit of %d bytes", size, info.FSName, info.MaxFileSize),
				false,
			)
		}

		free, err := s.fsInfoProvider.FreeSpace(dir)
		if err == nil && free > 0 {
			required := int64(float64(size) * 1.01)
			if free < required {
				return nil, domain.NewDownloadError(
					domain.ErrCodeDiskFull,
					fmt.Sprintf("insufficient free space on disk: need %d bytes, available %d bytes", required, free),
					true,
				)
			}
		}
	}

	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, classifyError(err)
	}

	extPath := toExtendedPath(path)
	//nolint:gosec // path managed by FileStore
	f, err := os.OpenFile(extPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, classifyError(err)
	}

	if err := allocateFile(f, size, mode); err != nil {
		_ = f.Close()
		_ = os.Remove(extPath)
		return nil, classifyError(err)
	}

	return &File{f: f}, nil
}

// Open opens an existing part file on disk for write resumption.
func (s *Store) Open(path string) (ports.File, error) {
	extPath := toExtendedPath(path)
	//nolint:gosec // path managed by FileStore
	f, err := os.OpenFile(extPath, os.O_RDWR, 0o600)
	if err != nil {
		return nil, classifyError(err)
	}
	return &File{f: f}, nil
}

// Finalize atomically renames partPath to finalPath enforcing conflict policies and path security.
func (s *Store) Finalize(partPath, finalPath string, policy domain.ConflictPolicy) (string, error) {
	destDir := filepath.Dir(partPath)

	evalDest, errDest := filepath.EvalSymlinks(destDir)
	if errDest != nil {
		evalDest = destDir
	}

	evalFinalDir, errFinal := filepath.EvalSymlinks(filepath.Dir(finalPath))
	if errFinal != nil {
		evalFinalDir = filepath.Dir(finalPath)
	}

	rel, err := filepath.Rel(evalDest, evalFinalDir)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("security violation: path traversal detected: %q escapes destination directory %q", finalPath, destDir)
	}

	baseName := filepath.Base(finalPath)
	sanitized := domain.SanitizeFileName(baseName)
	if sanitized != baseName {
		return "", fmt.Errorf("security violation: invalid or reserved filename %q (sanitized: %q)", baseName, sanitized)
	}

	targetPath := finalPath
	exists := fileExists(targetPath)

	if exists {
		switch policy {
		case domain.ConflictPolicyAsk:
			return "", ErrConflict
		case domain.ConflictPolicyOverwrite:
			targetPath = finalPath
		case domain.ConflictPolicyRename:
			ext := filepath.Ext(baseName)
			stem := strings.TrimSuffix(baseName, ext)
			i := 1
			for {
				candidate := filepath.Join(destDir, fmt.Sprintf("%s (%d)%s", stem, i, ext))
				if !fileExists(candidate) {
					targetPath = candidate
					break
				}
				i++
			}
		default:
			return "", fmt.Errorf("unsupported conflict policy %q", policy)
		}
	}

	overwrite := exists && (policy == domain.ConflictPolicyOverwrite)
	if err := atomicRename(partPath, targetPath, overwrite); err != nil {
		return "", classifyError(err)
	}

	return filepath.Base(targetPath), nil
}

// FreeSpace returns available bytes on the volume containing dir.
func (s *Store) FreeSpace(dir string) (int64, error) {
	return s.fsInfoProvider.FreeSpace(dir)
}

// FSInfo returns filesystem metadata for dir.
func (s *Store) FSInfo(dir string) (domain.FSInfo, error) {
	return s.fsInfoProvider.FSInfo(dir)
}

// File represents a thread-safe handle to an open file.
type File struct {
	f *os.File
}

// WriteAt writes p to the file offset off concurrently.
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.f.WriteAt(p, off)
	if err != nil {
		return n, classifyError(err)
	}
	return n, nil
}

// Sync commits current in-memory buffers to stable persistent storage.
func (f *File) Sync() error {
	return f.f.Sync()
}

// Close closes the underlying operating system file handle.
func (f *File) Close() error {
	return f.f.Close()
}

func fileExists(p string) bool {
	extPath := toExtendedPath(p)
	_, err := os.Stat(extPath)
	return err == nil
}
