package engine

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
)

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, bufferSize)
		return &b
	},
}

const (
	bufferSize  = 256 * 1024
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

		pos := seg.Cursor.Load()
		end := seg.End.Load()
		if pos >= end {
			return nil
		}

		resp, err := deps.Fetcher.OpenRange(ctx, deps.RequestContext, deps.URL, pos, end-1, deps.Validator)
		if err != nil {
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
			if attempt >= maxAttempts {
				return fmt.Errorf("segment %s failed after %d attempts: %w", seg.ID, attempt, err)
			}
			delay := CalculateBackoff(attempt, 0, deps.RNG)
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

			limit := seg.End.Load()
			if pos >= limit {
				break
			}

			n := min(int64(len(buf)), limit-pos)
			targetRead := min(n, coalesceMin)

			rn, rerr := io.ReadAtLeast(resp.Body, buf[:n], int(targetRead))
			if rn > 0 {
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

			if pos >= seg.End.Load() {
				break
			}

			if rerr != nil {
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
