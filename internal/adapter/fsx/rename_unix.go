//go:build !windows

package fsx

import (
	"os"
)

func toExtendedPath(p string) string {
	return p
}

func atomicRename(oldPath, newPath string, overwrite bool) error {
	if !overwrite {
		if _, err := os.Stat(newPath); err == nil {
			return os.ErrExist
		}
	}
	return os.Rename(oldPath, newPath)
}
