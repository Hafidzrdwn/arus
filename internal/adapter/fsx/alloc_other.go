//go:build !windows && !linux && !darwin

package fsx

import (
	"os"

	"arus/internal/domain"
)

func allocateFile(f *os.File, size int64, mode domain.AllocMode) error {
	if mode == domain.AllocModeNone || size <= 0 {
		return nil
	}
	return f.Truncate(size)
}
