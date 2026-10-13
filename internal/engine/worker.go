package engine

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// ErrWorkerRetired indicates the worker stopped because it was marked to retire.
var ErrWorkerRetired = errors.New("worker retired")

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, bufferSize)
		return &b
	},
}

const (
	// BufferSize is the default buffer size used for streaming data.
	BufferSize  = 256 * 1024
	bufferSize  = BufferSize
	coalesceMin = 128 * 1024
	maxAttempts = 8
)

// WorkerDeps encapsulates driven adapters and state trackers required to run a segment worker.
type WorkerDeps struct {
	Fetcher        ports.Fetcher
	File           ports.File
	URL            string
	RequestContext domain.RequestContext
	Validator      string
	Clock          ports.Clock
	RNG            func() float64
	Progress       *atomic.Int64
	Retries        *atomic.Int64
	Retire         *atomic.Bool
	Limiter        *LimiterSet
	ErrTracker     *ErrorTracker
}

func recordNetError(tracker *ErrorTracker, err error) {
	if tracker == nil || err == nil {
		return
	}
	var netErr net.Error
	msg := strings.ToLower(err.Error())
	if (errors.As(err, &netErr) && netErr.Timeout()) || strings.Contains(msg, "timeout") {
		tracker.RecordTimeout()
	} else if strings.Contains(msg, "reset") || strings.Contains(msg, "broken pipe") || strings.Contains(msg, "connection refused") || strings.Contains(msg, "unexpected eof") {
		tracker.RecordReset()
	}
}

// RunSegment downloads a discrete byte range from the remote server and writes it directly to disk.
func RunSegment(ctx context.Context, deps WorkerDeps, seg *SegState) error {
	var attempt int
	var expiredRetries int

	bufPtr := bufPool.Get().(*[]byte)
	buf := *bufPtr
	defer bufPool.Put(bufPtr)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if deps.Retire != nil && deps.Retire.Load() {
			return ErrWorkerRetired
		}

		pos := seg.Cursor.Load()
		end := seg.End.Load()
		if pos >= end {
			return nil
		}

		resp, err := deps.Fetcher.OpenRange(ctx, deps.RequestContext, deps.URL, pos, end-1, deps.Validator)
		if err != nil {
			recordNetError(deps.ErrTracker, err)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			class := Classify(err, 0)
			if class == ClassFatal {
				return ClassifyDiskError(err)
			}
			if class == ClassExpired {
				if expiredRetries >= 2 {
					return domain.NewDownloadError(domain.ErrCodeLinkExpired, err.Error(), true)
				}
				expiredRetries++
			}
			attempt++
			if deps.Retries != nil {
				deps.Retries.Add(1)
			}
			if attempt >= maxAttempts {
				return fmt.Errorf("segment %s failed after %d attempts: %w", seg.ID, attempt, err)
			}
			delay := CalculateBackoff(attempt, 0, deps.RNG)
			if serr := Sleep(ctx, deps.Clock, delay); serr != nil {
				return serr
			}
			continue
		}

		if (resp.Status == http.StatusOK || resp.Status == http.StatusPartialContent) &&
			strings.Contains(strings.ToLower(resp.ContentType), "text/html") && (pos > 0 || hasBinaryURL(deps.URL)) {
			_ = resp.Body.Close()
			if expiredRetries >= 2 {
				return domain.NewDownloadError(domain.ErrCodeLinkExpired, "server returned text/html for binary request", true)
			}
			expiredRetries++
			attempt++
			if deps.Retries != nil {
				deps.Retries.Add(1)
			}
			delay := CalculateBackoff(attempt, resp.RetryAfter, deps.RNG)
			if serr := Sleep(ctx, deps.Clock, delay); serr != nil {
				return serr
			}
			continue
		}

		switch {
		case resp.Status == http.StatusOK:
			if pos > 0 {
				_ = resp.Body.Close()
				if deps.Validator != "" {
					return domain.NewDownloadError(domain.ErrCodeRemoteChanged, "server returned 200 for range request with validator", false)
				}
				return domain.NewDownloadError(domain.ErrCodeRangeUnsupported, "server does not support range requests", false)
			}
		case resp.Status == http.StatusPartialContent:
			if resp.ContentRange.Start != pos {
				_ = resp.Body.Close()
				return fmt.Errorf("range mismatch: expected start %d, got %d", pos, resp.ContentRange.Start)
			}
		case resp.Status == http.StatusTooManyRequests:
			if deps.ErrTracker != nil {
				deps.ErrTracker.Record429(resp.RetryAfter)
			}
			_ = resp.Body.Close()
			attempt++
			if deps.Retries != nil {
				deps.Retries.Add(1)
			}
			delay := CalculateBackoff(attempt, resp.RetryAfter, deps.RNG)
			if serr := Sleep(ctx, deps.Clock, delay); serr != nil {
				return serr
			}
			continue
		case resp.Status == http.StatusServiceUnavailable:
			if deps.ErrTracker != nil {
				deps.ErrTracker.Record503(resp.RetryAfter)
			}
			_ = resp.Body.Close()
			attempt++
			if deps.Retries != nil {
				deps.Retries.Add(1)
			}
			delay := CalculateBackoff(attempt, resp.RetryAfter, deps.RNG)
			if serr := Sleep(ctx, deps.Clock, delay); serr != nil {
				return serr
			}
			continue
		case resp.Status == http.StatusNotFound:
			_ = resp.Body.Close()
			return domain.NewDownloadError(domain.ErrCodeNotFound, "server returned 404 Not Found", false)
		case resp.Status == http.StatusForbidden || resp.Status == http.StatusUnauthorized || resp.Status == http.StatusGone:
			_ = resp.Body.Close()
			if expiredRetries >= 2 {
				return domain.NewDownloadError(domain.ErrCodeLinkExpired, fmt.Sprintf("server returned status %d", resp.Status), true)
			}
			expiredRetries++
			attempt++
			if deps.Retries != nil {
				deps.Retries.Add(1)
			}
			delay := CalculateBackoff(attempt, resp.RetryAfter, deps.RNG)
			if serr := Sleep(ctx, deps.Clock, delay); serr != nil {
				return serr
			}
			continue
		default:
			_ = resp.Body.Close()
			class := Classify(nil, resp.Status)
			if class == ClassFatal {
				return domain.NewDownloadError(domain.ErrorCode(strconv.Itoa(resp.Status)), fmt.Sprintf("fatal status %d", resp.Status), false)
			}

			attempt++
			if deps.Retries != nil {
				deps.Retries.Add(1)
			}
			if attempt >= maxAttempts {
				return fmt.Errorf("segment %s failed after %d attempts (status %d)", seg.ID, attempt, resp.Status)
			}
			delay := CalculateBackoff(attempt, resp.RetryAfter, deps.RNG)
			if serr := Sleep(ctx, deps.Clock, delay); serr != nil {
				return serr
			}
			continue
		}

		bytesReadInAttempt := int64(0)
		for {
			if ctx.Err() != nil {
				_ = resp.Body.Close()
				return ctx.Err()
			}
			if deps.Retire != nil && deps.Retire.Load() {
				_ = resp.Body.Close()
				return ErrWorkerRetired
			}

			limit := seg.End.Load()
			if pos >= limit {
				break
			}

			n := min(int64(len(buf)), limit-pos)
			targetRead := min(n, coalesceMin)

			rn, rerr := io.ReadAtLeast(resp.Body, buf[:n], int(targetRead))
			if rn > 0 {
				if deps.Limiter != nil {
					if lerr := deps.Limiter.WaitN(ctx, rn); lerr != nil {
						_ = resp.Body.Close()
						return lerr
					}
				}
				wn, werr := deps.File.WriteAt(buf[:rn], pos)
				if werr != nil {
					_ = resp.Body.Close()
					return ClassifyDiskError(werr)
				}
				pos += int64(wn)
				seg.Cursor.Store(pos)
				if deps.Progress != nil {
					deps.Progress.Add(int64(wn))
				}
				bytesReadInAttempt += int64(wn)
			}

			if deps.Retire != nil && deps.Retire.Load() {
				_ = resp.Body.Close()
				return ErrWorkerRetired
			}

			if pos >= seg.End.Load() {
				break
			}

			if rerr != nil {
				if rerr == io.EOF && seg.End.Load() == math.MaxInt64 {
					seg.End.Store(pos)
				}
				if rerr != io.EOF && deps.ErrTracker != nil {
					recordNetError(deps.ErrTracker, rerr)
				}
				break
			}
		}

		_ = resp.Body.Close()

		if pos >= seg.End.Load() {
			return nil
		}

		if bytesReadInAttempt > 0 {
			attempt = 0
		} else {
			attempt++
			if deps.Retries != nil {
				deps.Retries.Add(1)
			}
		}

		if attempt >= maxAttempts {
			return fmt.Errorf("segment %s failed after %d consecutive attempts without progress", seg.ID, attempt)
		}

		delay := CalculateBackoff(attempt, 0, deps.RNG)
		if serr := Sleep(ctx, deps.Clock, delay); serr != nil {
			return serr
		}
	}
}

func hasBinaryURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	ext := strings.ToLower(filepath.Ext(u.Path))
	switch ext {
	case ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz", ".iso", ".dmg", ".bin",
		".exe", ".msi", ".apk", ".deb", ".rpm", ".pdf", ".mp4", ".mkv", ".avi", ".mov",
		".mp3", ".wav", ".flac", ".docx", ".xlsx", ".pptx":
		return true
	}
	return false
}
