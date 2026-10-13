//go:build !windows

package fsx

import (
	"errors"
	"os"
	"syscall"

	"arus/internal/domain"
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
		case syscall.ENOSPC:
			return domain.NewDownloadError(domain.ErrCodeDiskFull, err.Error(), true)
		case syscall.EACCES, syscall.EPERM:
			return domain.NewDownloadError(domain.ErrCodePermission, err.Error(), false)
		}
	}

	if errors.Is(err, os.ErrPermission) {
		return domain.NewDownloadError(domain.ErrCodePermission, err.Error(), false)
	}

	return err
}
