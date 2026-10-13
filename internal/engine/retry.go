package engine

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"
)

// Class categorizes error types according to the retry taxonomy in TECH_SPEC §6.11.
type Class int

// Taxonomy error classifications.
const (
	ClassTransient Class = iota
	ClassThrottle
	ClassExpired
	ClassFatal
	ClassIntegrity
)

func (c Class) String() string {
	switch c {
	case ClassTransient:
		return "transient"
	case ClassThrottle:
		return "throttle"
	case ClassExpired:
		return "expired"
	case ClassFatal:
		return "fatal"
	case ClassIntegrity:
		return "integrity"
	default:
		return "unknown"
	}
}

// Classify maps an HTTP response status code or error to its appropriate retry class.
func Classify(err error, status int) Class {
	if status == http.StatusTooManyRequests {
		return ClassThrottle
	}
	if status == http.StatusServiceUnavailable {
		return ClassThrottle
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusGone {
		return ClassExpired
	}
	if status == http.StatusNotFound || status == http.StatusUnavailableForLegalReasons || status == http.StatusNotImplemented {
		return ClassFatal
	}
	if status >= 500 && status <= 599 {
		return ClassTransient
	}

	if err == nil {
		return ClassTransient
	}

	var de *domain.DownloadError
	if errors.As(err, &de) {
		switch de.Code {
		case domain.ErrCodeDiskFull, domain.ErrCodePermission, domain.ErrCodeFSLimit, domain.ErrCodeNotFound:
			return ClassFatal
		case domain.ErrCodeLinkExpired:
			return ClassExpired
		case domain.ErrCodeRemoteChanged, domain.ErrCodeChecksumMismatch:
			return ClassIntegrity
		case domain.ErrCodeNetwork:
			return ClassTransient
		case domain.ErrCodeRangeUnsupported:
			return ClassFatal
		}
	}

	if isDiskFull(err) || errors.Is(err, os.ErrPermission) {
		return ClassFatal
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ClassFatal
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ClassTransient
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return ClassTransient
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "reset") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "stalled") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection refused") {
		return ClassTransient
	}

	return ClassTransient
}

func isDiskFull(err error) bool {
	if err == nil {
		return false
	}
	var de *domain.DownloadError
	if errors.As(err, &de) && de.Code == domain.ErrCodeDiskFull {
		return true
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		if errno == syscall.ENOSPC || errno == 112 || errno == 39 {
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "disk full") || strings.Contains(msg, "no space")
}

// ClassifyDiskError converts underlying OS disk errors into structured domain download errors.
func ClassifyDiskError(err error) error {
	if err == nil {
		return nil
	}
	var de *domain.DownloadError
	if errors.As(err, &de) {
		return err
	}
	if isDiskFull(err) {
		return domain.NewDownloadError(domain.ErrCodeDiskFull, err.Error(), true)
	}
	if errors.Is(err, os.ErrPermission) {
		return domain.NewDownloadError(domain.ErrCodePermission, err.Error(), false)
	}
	return err
}

// CalculateBackoff computes the delay before retry, including exponential backoff and jitter.
func CalculateBackoff(attempt int, retryAfter time.Duration, rng func() float64) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	base := 500 * time.Millisecond
	if attempt > 0 {
		if attempt > 6 {
			base = 30 * time.Second
		} else {
			base *= (1 << attempt)
			if base > 30*time.Second {
				base = 30 * time.Second
			}
		}
	}
	var r float64
	if rng != nil {
		r = rng()
	} else {
		//nolint:gosec // pseudo-randomness is sufficient for backoff jitter
		r = rand.Float64()
	}
	factor := 0.8 + 0.4*r
	return time.Duration(float64(base) * factor)
}

// Sleep pauses the current execution until d expires or ctx is cancelled.
func Sleep(ctx context.Context, clock ports.Clock, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	if clock == nil {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
	timer := clock.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return nil
	}
}
