//go:build linux

package fsx

import (
	"os"

	"arus/internal/domain"

	"golang.org/x/sys/unix"
)

func allocateFile(f *os.File, size int64, mode domain.AllocMode) error {
	if mode == domain.AllocModeNone || size <= 0 {
		return nil
	}

	if mode == domain.AllocModeFull {
		err := unix.Fallocate(int(f.Fd()), 0, 0, size)
		if err == nil {
			return nil
		}
	}

	return f.Truncate(size)
}
