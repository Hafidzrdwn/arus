//go:build windows

package fsx

import (
	"arus/internal/domain"
	"log/slog"
	"os"

	"golang.org/x/sys/windows"
)

const fsctlSetSparse = 0x000900C4

func allocateFile(f *os.File, size int64, mode domain.AllocMode) error {
	if mode == domain.AllocModeNone || size <= 0 {
		return nil
	}

	if mode == domain.AllocModeSparse {
		var bytesReturned uint32
		err := windows.DeviceIoControl(
			windows.Handle(f.Fd()),
			fsctlSetSparse,
			nil,
			0,
			nil,
			0,
			&bytesReturned,
			nil,
		)
		if err != nil {
			slog.Warn("FSCTL_SET_SPARSE failed; falling back to standard truncate", "err", err)
		}
	}

	return f.Truncate(size)
}
