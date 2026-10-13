//go:build windows

package fsx

import (
	"arus/internal/domain"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
)

func freeSpaceOS(dir string) (int64, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		abs = dir
	}
	dirPtr, err := syscall.UTF16PtrFromString(abs)
	if err != nil {
		return 0, err
	}
	var freeBytesAvailable, totalBytes, totalFreeBytes uint64
	err = windows.GetDiskFreeSpaceEx(
		dirPtr,
		&freeBytesAvailable,
		&totalBytes,
		&totalFreeBytes,
	)
	if err != nil {
		return 0, err
	}
	return int64(freeBytesAvailable), nil
}

func fsInfoOS(dir string) (domain.FSInfo, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		abs = dir
	}
	volRoot := filepath.VolumeName(abs)
	if volRoot != "" {
		volRoot += `\`
	} else {
		volRoot = `C:\`
	}
	volPtr, err := syscall.UTF16PtrFromString(volRoot)
	if err != nil {
		return domain.FSInfo{}, err
	}

	var fsNameBuf [256]uint16
	var maxComponentLen, flags uint32
	err = windows.GetVolumeInformation(
		volPtr,
		nil,
		0,
		nil,
		&maxComponentLen,
		&flags,
		&fsNameBuf[0],
		uint32(len(fsNameBuf)),
	)
	if err != nil {
		return domain.FSInfo{}, err
	}

	fsName := syscall.UTF16ToString(fsNameBuf[:])
	freeBytes, _ := freeSpaceOS(dir)

	maxFileSize := int64(1 << 62)
	if strings.EqualFold(fsName, "FAT32") {
		maxFileSize = 4*1024*1024*1024 - 1
	}

	return domain.FSInfo{
		FSName:      fsName,
		MaxFileSize: maxFileSize,
		FreeBytes:   freeBytes,
	}, nil
}
