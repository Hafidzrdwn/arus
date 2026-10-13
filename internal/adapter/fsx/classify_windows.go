//go:build windows

package fsx

import (
	"arus/internal/domain"
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/windows"
)

func classifyError(err error) error {
	if err == nil {
		return nil
	}
	var de *domain.DownloadError
	if errors.As(err, &de) {
		return err
	}

	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case windows.ERROR_DISK_FULL, windows.ERROR_HANDLE_DISK_FULL, syscall.ENOSPC:
			return domain.NewDownloadError(domain.ErrCodeDiskFull, err.Error(), true)
		case windows.ERROR_ACCESS_DENIED, syscall.EACCES, syscall.EPERM:
			return domain.NewDownloadError(domain.ErrCodePermission, err.Error(), false)
		}
	}

	if errors.Is(err, os.ErrPermission) {
		return domain.NewDownloadError(domain.ErrCodePermission, err.Error(), false)
	}

	return err
}
