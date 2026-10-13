//go:build darwin

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
		fst := unix.Fstore_t{
			Flags:      unix.F_ALLOCATECONTIG | unix.F_ALLOCATEALL,
			Posmode:    unix.F_PEOFPOSMODE,
			Offset:     0,
			Length:     size,
			Bytesalloc: 0,
		}
		if err := unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &fst); err != nil {
			fst.Flags = unix.F_ALLOCATEALL
			_ = unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &fst)
		}
	}

	return f.Truncate(size)
}
