//go:build !windows

package fsx_test

import (
	"os"
	"syscall"
)

func getPhysicalSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		return int64(stat.Blocks) * 512, nil
	}
	return fi.Size(), nil
}
