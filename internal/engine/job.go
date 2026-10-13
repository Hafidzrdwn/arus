package engine

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrPaused indicates the download job was paused by request.
var ErrPaused = errors.New("download paused")

// JobDeps bundles driven ports needed to orchestrate download jobs.
type JobDeps struct {
	Fetcher   ports.Fetcher
	FileStore ports.FileStore
	Clock     ports.Clock
	RNG       func() float64
}

// Result summarizes execution metrics of a download job.
type Result struct {
	BytesWritten int64
	Duration     time.Duration
	RetryCount   int64
}

// JobSnapshot captures a thread-safe, consistent snapshot of a job's progress and state.
type JobSnapshot struct {
	State    domain.State
	Segments []domain.Segment
	Progress domain.ProgressSnapshot
}

// Job orchestrates probing, allocation, segment planning, and worker lifecycles.
type Job struct {
	deps     JobDeps
	download domain.Download

	stateMu    sync.RWMutex
	state      domain.State
	onState    func(domain.State)
	onStateCh  chan domain.State
	cancelFunc context.CancelFunc

	segMu    sync.RWMutex
	segments []*SegState

	progress       *ProgressTracker
	pauseRequested atomic.Bool
	retries        atomic.Int64
	file           ports.File
}

// NewJob creates an initialized download Job for the given download entity.
func NewJob(deps JobDeps, download domain.Download) *Job {
	return &Job{
		deps:      deps,
		download:  download,
		state:     domain.StateQueued,
		progress:  NewProgressTracker(download.TotalSize, deps.Clock),
		onStateCh: make(chan domain.State, 16),
	}
}

// SetOnState registers a synchronous callback for lifecycle state transitions.
func (j *Job) SetOnState(cb func(domain.State)) {
	j.stateMu.Lock()
	defer j.stateMu.Unlock()
	j.onState = cb
}

// OnStateChan returns a channel streaming lifecycle state transitions.
func (j *Job) OnStateChan() <-chan domain.State {
	return j.onStateCh
}

// State returns the current lifecycle state of the job.
func (j *Job) State() domain.State {
	j.stateMu.RLock()
	defer j.stateMu.RUnlock()
	return j.state
}

func (j *Job) setState(st domain.State) {
	j.stateMu.Lock()
	j.state = st
	cb := j.onState
	ch := j.onStateCh
	j.stateMu.Unlock()

	if cb != nil {
		cb(st)
	}
	if ch != nil {
		select {
		case ch <- st:
		default:
		}
	}
}

func (j *Job) now() time.Time {
	if j.deps.Clock != nil {
		return j.deps.Clock.Now()
	}
	return time.Now()
}

func (j *Job) validatorString() string {
	if j.download.Validators.ETag != "" && !strings.HasPrefix(j.download.Validators.ETag, "W/") {
		return j.download.Validators.ETag
	}
	if j.download.Validators.LastModified != "" {
		return j.download.Validators.LastModified
	}
	return j.download.Validators.ETag
}

// Pause requests active workers to halt, syncs data, and transitions state to Paused.
func (j *Job) Pause() []domain.Segment {
	j.pauseRequested.Store(true)
	j.stateMu.Lock()
	cancel := j.cancelFunc
	j.stateMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if j.file != nil {
		_ = j.file.Sync()
	}
	return j.Snapshot().Segments
}

// Snapshot returns a thread-safe point-in-time snapshot of the job.
func (j *Job) Snapshot() JobSnapshot {
	j.stateMu.RLock()
	st := j.state
	j.stateMu.RUnlock()

	j.segMu.RLock()
	segsCopy := make([]domain.Segment, len(j.segments))
	for i, s := range j.segments {
		segsCopy[i] = domain.Segment{
			ID:     i,
			Start:  s.Start,
			End:    s.End.Load(),
			Cursor: s.Cursor.Load(),
		}
	}
	j.segMu.RUnlock()

	return JobSnapshot{
		State:    st,
		Segments: segsCopy,
		Progress: j.progress.Snapshot(),
	}
}

// Run executes the complete download lifecycle: probe, allocate, plan, and stream.
func (j *Job) Run(ctx context.Context) (Result, error) {
	startTime := j.now()

	j.setState(domain.StateProbing)
	probeResult, err := j.deps.Fetcher.Probe(ctx, j.download.ReqCtx, j.download.URL)
	if err != nil {
		j.setState(domain.StateFailed)
		return Result{}, fmt.Errorf("probe failed: %w", err)
	}

	if probeResult.FinalURL != "" {
		j.download.FinalURL = probeResult.FinalURL
	}
	if probeResult.TotalSize > 0 {
		j.download.TotalSize = probeResult.TotalSize
		j.progress.SetTotal(probeResult.TotalSize)
	}
	j.download.RangeSupport = probeResult.RangeSupport
	j.download.Validators = probeResult.Validators
	if j.download.FileName == "" && probeResult.FileName != "" {
		j.download.FileName = probeResult.FileName
	}

	j.setState(domain.StateAllocating)
	allocMode := domain.AllocModeSparse
	if j.download.TotalSize <= 0 || j.download.RangeSupport == domain.RangeSupportNo {
		allocMode = domain.AllocModeNone
	}

	allocSize := max(0, j.download.TotalSize)
	file, err := j.deps.FileStore.Create(j.download.PartPath, allocSize, allocMode)
	if err != nil {
		j.setState(domain.StateFailed)
		return Result{}, fmt.Errorf("file allocation failed: %w", err)
	}
	j.file = file
	defer func() {
		_ = file.Close()
	}()

	isSingleStream := false
	var segs []*SegState

	if j.download.TotalSize <= 0 || j.download.RangeSupport == domain.RangeSupportNo {
		isSingleStream = true
		end := int64(math.MaxInt64)
		if j.download.TotalSize > 0 {
			end = j.download.TotalSize
		}
		segs = []*SegState{
			NewSegState("seg-0", 0, end, 0),
		}
	} else {
		maxConns := j.download.Opts.MaxConns
		if maxConns <= 0 {
			maxConns = 4
		}

		planned := domain.PlanSegments(j.download.TotalSize, int64(maxConns), 1048576, 4096)
		segs = make([]*SegState, len(planned))
		for i, p := range planned {
			segs[i] = NewSegState(fmt.Sprintf("seg-%d", p.ID), p.Start, p.End, p.Cursor)
		}
	}

	j.segMu.Lock()
	j.segments = segs
	j.segMu.Unlock()

	j.setState(domain.StateDownloading)

	workerCtx, workerCancel := context.WithCancel(ctx)
	j.stateMu.Lock()
	j.cancelFunc = workerCancel
	j.stateMu.Unlock()
	defer workerCancel()

	if !isSingleStream && j.download.RangeSupport == domain.RangeSupportUnverified && len(segs) > 1 {
		firstSeg := segs[0]
		firstResp, rerr := j.deps.Fetcher.OpenRange(workerCtx, j.download.ReqCtx, j.download.URL, firstSeg.Cursor.Load(), firstSeg.End.Load()-1, j.validatorString())
		if rerr == nil {
			if firstResp.Status == http.StatusOK {
				isSingleStream = true
				firstSeg.End.Store(j.download.TotalSize)
				j.segMu.Lock()
				j.segments = []*SegState{firstSeg}
				j.segMu.Unlock()

				sErr := j.streamResponseDirect(workerCtx, firstResp.Body, firstSeg)
				_ = firstResp.Body.Close()
				if sErr != nil {
					j.setState(domain.StateFailed)
					return Result{}, sErr
				}
			} else {
				_ = firstResp.Body.Close()
				if firstResp.Status == http.StatusPartialContent {
					j.download.RangeSupport = domain.RangeSupportYes
				}
			}
		}
	}

	if !isSingleStream || j.segments[0].Cursor.Load() < j.segments[0].End.Load() {
		var wg sync.WaitGroup
		errCh := make(chan error, len(j.segments))

		for _, seg := range j.segments {
			wg.Add(1)
			go func(s *SegState) {
				defer wg.Done()
				wDeps := WorkerDeps{
					Fetcher:        j.deps.Fetcher,
					File:           j.file,
					URL:            j.download.URL,
					RequestContext: j.download.ReqCtx,
					Validator:      j.validatorString(),
					Clock:          j.deps.Clock,
					RNG:            j.deps.RNG,
					Progress:       &j.progress.downloaded,
					Retries:        &j.retries,
				}
				if werr := RunSegment(workerCtx, wDeps, s); werr != nil {
					if !errors.Is(werr, context.Canceled) {
						errCh <- werr
					}
					workerCancel()
				}
			}(seg)
		}

		wg.Wait()
		close(errCh)

		if j.pauseRequested.Load() || errors.Is(ctx.Err(), context.Canceled) {
			_ = j.file.Sync()
			j.setState(domain.StatePaused)
			return Result{
				BytesWritten: j.progress.Downloaded(),
				Duration:     j.now().Sub(startTime),
				RetryCount:   j.retries.Load(),
			}, ErrPaused
		}

		var firstErr error
		for werr := range errCh {
			if firstErr == nil {
				firstErr = werr
			}
		}

		if firstErr != nil {
			j.setState(domain.StateFailed)
			return Result{
				BytesWritten: j.progress.Downloaded(),
				Duration:     j.now().Sub(startTime),
				RetryCount:   j.retries.Load(),
			}, firstErr
		}
	}

	if isSingleStream && j.download.TotalSize <= 0 {
		finalWritten := j.segments[0].Cursor.Load()
		j.download.TotalSize = finalWritten
		j.progress.SetTotal(finalWritten)
		j.segments[0].End.Store(finalWritten)
	}

	if err := j.file.Sync(); err != nil {
		j.setState(domain.StateFailed)
		return Result{}, fmt.Errorf("sync failed: %w", err)
	}

	j.setState(domain.StateCompleted)
	return Result{
		BytesWritten: j.progress.Downloaded(),
		Duration:     j.now().Sub(startTime),
		RetryCount:   j.retries.Load(),
	}, nil
}

func (j *Job) streamResponseDirect(ctx context.Context, body io.Reader, seg *SegState) error {
	buf := make([]byte, bufferSize)
	pos := seg.Cursor.Load()
	limit := seg.End.Load()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if pos >= limit {
			break
		}

		n := min(int64(len(buf)), limit-pos)
		rn, rerr := body.Read(buf[:n])
		if rn > 0 {
			wn, werr := j.file.WriteAt(buf[:rn], pos)
			if werr != nil {
				return ClassifyDiskError(werr)
			}
			pos += int64(wn)
			seg.Cursor.Store(pos)
			j.progress.Add(int64(wn))
		}
		if rerr == io.EOF {
			seg.End.Store(pos)
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	return nil
}
