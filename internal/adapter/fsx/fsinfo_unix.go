//go:build !windows

package fsx

import (
	"arus/internal/domain"

	"golang.org/x/sys/unix"
)

func freeSpaceOS(dir string) (int64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

func fsInfoOS(dir string) (domain.FSInfo, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(dir, &stat); err != nil {
		return domain.FSInfo{}, err
	}

	freeBytes := int64(stat.Bavail) * int64(stat.Bsize)
	fsName := "unix"
	maxFileSize := int64(1 << 62)

	if stat.Type == 0x4d44 {
		fsName = "FAT32"
		maxFileSize = 4*1024*1024*1024 - 1
	}

	return domain.FSInfo{
		FSName:      fsName,
		MaxFileSize: maxFileSize,
		FreeBytes:   freeBytes,
	}, nil
}
