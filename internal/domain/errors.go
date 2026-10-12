package domain

import (
	"errors"
	"fmt"
)

var (
	// ErrIllegalTransition is returned when a state transition is not allowed by the state machine.
	ErrIllegalTransition = errors.New("illegal state transition")
	// ErrInvariantViolation is returned when segment invariants are violated.
	ErrInvariantViolation = errors.New("segment invariant violation")
)

// ErrorCode represents categorized download failure codes.
type ErrorCode string

// Download failure error codes defined in TECH_SPEC §4.1.
const (
	// ErrCodeLinkExpired indicates a download link or presigned URL has expired.
	ErrCodeLinkExpired ErrorCode = "link_expired"
	// ErrCodeRemoteChanged indicates server ETag or content changed mid-download.
	ErrCodeRemoteChanged ErrorCode = "remote_changed"
	// ErrCodeDiskFull indicates target storage volume is full.
	ErrCodeDiskFull ErrorCode = "disk_full"
	// ErrCodeFSLimit indicates filesystem limit exceeded (e.g. 4 GiB FAT32 limit).
	ErrCodeFSLimit ErrorCode = "fs_limit"
	// ErrCodePermission indicates access denied to destination folder or file.
	ErrCodePermission ErrorCode = "permission"
	// ErrCodeNotFound indicates origin server returned 404 Not Found.
	ErrCodeNotFound ErrorCode = "not_found"
	// ErrCodeRangeUnsupported indicates origin server does not accept range requests.
	ErrCodeRangeUnsupported ErrorCode = "range_unsupported"
	// ErrCodeChecksumMismatch indicates verified file hash does not match expected checksum.
	ErrCodeChecksumMismatch ErrorCode = "checksum_mismatch"
	// ErrCodeNetwork indicates transient or persistent network failure.
	ErrCodeNetwork ErrorCode = "network"
	// ErrCodeCanceled indicates download was canceled by user or system.
	ErrCodeCanceled ErrorCode = "canceled"
)

// DownloadError provides structured error metadata for downloads.
type DownloadError struct {
	Code        ErrorCode `json:"code"`
	Message     string    `json:"message"`
	Recoverable bool      `json:"recoverable"`
}

// Error formats the DownloadError message.
func (e *DownloadError) Error() string {
	if e == nil {
		return ""
	}
	if e.Recoverable {
		return fmt.Sprintf("[%s] %s (recoverable)", e.Code, e.Message)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// NewDownloadError creates a new DownloadError.
func NewDownloadError(code ErrorCode, message string, recoverable bool) *DownloadError {
	return &DownloadError{
		Code:        code,
		Message:     message,
		Recoverable: recoverable,
	}
}
