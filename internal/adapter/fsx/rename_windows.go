//go:build windows

package fsx

import (
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
)

func toExtendedPath(p string) string {
	if len(p) < 248 && !strings.HasPrefix(p, `\\?\`) {
		return p
	}
	if strings.HasPrefix(p, `\\?\`) {
		return p
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return `\\?\` + abs
}

func atomicRename(oldPath, newPath string, overwrite bool) error {
	oldPtr, err := syscall.UTF16PtrFromString(toExtendedPath(oldPath))
	if err != nil {
		return err
	}
	newPtr, err := syscall.UTF16PtrFromString(toExtendedPath(newPath))
	if err != nil {
		return err
	}

	var flags uint32 = windows.MOVEFILE_COPY_ALLOWED
	if overwrite {
		flags |= windows.MOVEFILE_REPLACE_EXISTING
	}

	return windows.MoveFileEx(oldPtr, newPtr, flags)
}
