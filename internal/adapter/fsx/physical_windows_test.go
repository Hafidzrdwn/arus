//go:build windows

package fsx_test

import (
	"errors"
	"syscall"
	"unsafe"
)

var (
	kernel32              = syscall.NewLazyDLL("kernel32.dll")
	procGetCompressedSize = kernel32.NewProc("GetCompressedFileSizeW")
)

//nolint:gosec // Windows API test helper
func getPhysicalSize(path string) (int64, error) {
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var high uint32
	low, _, errCall := procGetCompressedSize.Call(uintptr(unsafe.Pointer(ptr)), uintptr(unsafe.Pointer(&high)))
	if low == 0xFFFFFFFF && !errors.Is(errCall, syscall.Errno(0)) {
		return 0, errCall
	}
	return int64(high)<<32 | int64(low), nil
}
