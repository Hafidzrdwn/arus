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
	Fetcher       ports.Fetcher
	FileStore     ports.FileStore
	Clock         ports.Clock
	RNG           func() float64
	GlobalLimiter *Throttle
	HostProfiles  *HostProfiles
	ConnBudget    *ConnBudget
	TunerInterval time.Duration
	TunerConfig   *TunerConfig
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

type workerHandle struct {
	id     string
	retire atomic.Bool
	cancel context.CancelFunc
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
	registry *Registry

	workerMu         sync.Mutex
	workers          []*workerHandle
	workerCounter    atomic.Int64
	workerCtx        context.Context
	workerCancel     context.CancelFunc
	workerWG         sync.WaitGroup
	errCh            chan error
	targetConns      int
	isSingleStream   bool
	done             bool
	stealingDisabled bool

	progress       *ProgressTracker
	pauseRequested atomic.Bool
	retries        atomic.Int64
	file           ports.File
	limiter        *LimiterSet
	errTracker     *ErrorTracker
	budgetConns    int
}

// NewJob creates an initialized download Job for the given download entity.
func NewJob(deps JobDeps, download domain.Download) *Job {
	jobThrottle := NewThrottle(download.Opts.SpeedLimitBps)
	limiterSet := NewLimiterSet(deps.GlobalLimiter, jobThrottle)

	return &Job{
		deps:       deps,
		download:   download,
		state:      domain.StateQueued,
		progress:   NewProgressTracker(download.TotalSize, deps.Clock),
		onStateCh:  make(chan domain.State, 16),
		limiter:    limiterSet,
		errTracker: &ErrorTracker{},
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
	j.cancelWorkers()

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

	var segs []domain.Segment
	if j.registry != nil {
		segs = j.registry.Snapshot()
	} else {
		j.segMu.RLock()
		segs = make([]domain.Segment, len(j.segments))
		for i, s := range j.segments {
			segs[i] = domain.Segment{
				ID:     i,
				Start:  s.Start,
				End:    s.End.Load(),
				Cursor: s.Cursor.Load(),
			}
		}
		j.segMu.RUnlock()
	}

	return JobSnapshot{
		State:    st,
		Segments: segs,
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
		if j.deps.HostProfiles != nil {
			host := cleanHostString(j.download.URL)
			safe := j.deps.HostProfiles.SafeMaxConns(host)
			if safe > 0 && maxConns > safe {
				maxConns = safe
			}
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

	j.registry = NewRegistry(segs, nil)
	if j.stealingDisabled {
		j.registry.SetDisabled(true)
	}

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
				j.registry = NewRegistry([]*SegState{firstSeg}, nil)

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
		numWorkers := len(segs)
		if isSingleStream {
			numWorkers = 1
		} else {
			maxConns := j.download.Opts.MaxConns
			if maxConns <= 0 {
				maxConns = 4
			}
			host := cleanHostString(j.download.URL)
			safeMax := 32
			if j.deps.HostProfiles != nil {
				safeMax = j.deps.HostProfiles.SafeMaxConns(host)
				if safeMax > 0 && maxConns > safeMax {
					maxConns = safeMax
				}
			}
			if numWorkers > maxConns {
				numWorkers = maxConns
			}
			if j.deps.ConnBudget != nil {
				numWorkers = j.deps.ConnBudget.TryAcquire(host, 0, numWorkers, safeMax)
				if numWorkers < 1 {
					numWorkers = 1
				}
			}
			j.workerMu.Lock()
			j.budgetConns = numWorkers
			j.workerMu.Unlock()
		}

		defer func() {
			j.workerMu.Lock()
			bConns := j.budgetConns
			j.budgetConns = 0
			j.workerMu.Unlock()

			if j.deps.ConnBudget != nil && bConns > 0 {
				host := cleanHostString(j.download.URL)
				j.deps.ConnBudget.Release(host, bConns)
			}
		}()

		j.workerMu.Lock()
		j.workerCtx = workerCtx
		j.workerCancel = workerCancel
		j.errCh = make(chan error, 32)
		j.isSingleStream = isSingleStream
		j.targetConns = numWorkers
		j.done = false
		j.workers = make([]*workerHandle, 0, numWorkers)
		for i := 0; i < numWorkers; i++ {
			h := &workerHandle{id: fmt.Sprintf("worker-%d", j.workerCounter.Add(1))}
			j.workers = append(j.workers, h)
			j.workerWG.Add(1)
			go j.runWorker(workerCtx, h)
		}
		j.workerMu.Unlock()

		tunerStopCh := make(chan struct{})
		if !isSingleStream {
			go j.runTuner(workerCtx, tunerStopCh, numWorkers)
		}

		j.workerWG.Wait()
		close(tunerStopCh)

		j.workerMu.Lock()
		j.done = true
		j.workerMu.Unlock()

		if j.pauseRequested.Load() || errors.Is(ctx.Err(), context.Canceled) {
			_ = j.file.Sync()
			j.setState(domain.StatePaused)
			return Result{
				BytesWritten: j.progress.Downloaded(),
				Duration:     j.now().Sub(startTime),
				RetryCount:   j.retries.Load(),
			}, ErrPaused
		}

		close(j.errCh)
		var firstErr error
		for werr := range j.errCh {
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

	if j.download.TotalSize > 0 {
		snap := j.Snapshot()
		if err := domain.ValidateInvariants(snap.Segments, j.download.TotalSize); err != nil {
			j.setState(domain.StateFailed)
			return Result{}, fmt.Errorf("invariant violation: %w", err)
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
			if j.limiter != nil {
				if lerr := j.limiter.WaitN(ctx, rn); lerr != nil {
					return lerr
				}
			}
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

func (j *Job) runWorker(ctx context.Context, h *workerHandle) {
	wCtx, wCancel := context.WithCancel(ctx)
	j.workerMu.Lock()
	h.cancel = wCancel
	j.workerMu.Unlock()
	defer wCancel()
	defer j.workerWG.Done()
	defer j.removeWorker(h)

	for {
		if ctx.Err() != nil || h.retire.Load() {
			return
		}

		seg, ok := j.registry.Claim(h.id)
		if !ok {
			if j.registry.IsComplete() {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-j.registry.NotifyChan():
				continue
			case <-time.After(20 * time.Millisecond):
				continue
			}
		}

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
			Retire:         &h.retire,
			Limiter:        j.limiter,
			ErrTracker:     j.errTracker,
		}

		werr := RunSegment(wCtx, wDeps, seg)
		if werr != nil {
			if errors.Is(werr, ErrWorkerRetired) || h.retire.Load() {
				j.registry.Orphan(seg)
				return
			}
			if !errors.Is(werr, context.Canceled) {
				select {
				case j.errCh <- werr:
				default:
				}
			}
			j.cancelWorkers()
			return
		}

		if h.retire.Load() && !seg.IsComplete() {
			j.registry.Orphan(seg)
			return
		}

		j.registry.Release(seg)
	}
}

func (j *Job) removeWorker(h *workerHandle) {
	j.workerMu.Lock()
	defer j.workerMu.Unlock()
	for i, w := range j.workers {
		if w == h {
			j.workers = append(j.workers[:i], j.workers[i+1:]...)
			break
		}
	}
}

func (j *Job) cancelWorkers() {
	j.workerMu.Lock()
	cancel := j.workerCancel
	j.workerMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// SetTargetConns dynamically adjusts the desired number of worker connections.
func (j *Job) SetTargetConns(n int) {
	if n <= 0 {
		n = 1
	}
	if n > 32 {
		n = 32
	}

	host := cleanHostString(j.download.URL)
	safeMax := 32
	if j.deps.HostProfiles != nil {
		safeMax = j.deps.HostProfiles.SafeMaxConns(host)
		if safeMax > 0 && n > safeMax {
			n = safeMax
		}
	}

	j.workerMu.Lock()
	defer j.workerMu.Unlock()

	if j.deps.ConnBudget != nil {
		n = j.deps.ConnBudget.TryAcquire(host, j.budgetConns, n, safeMax)
		j.budgetConns = n
	}

	j.targetConns = n

	if j.done || j.State() != domain.StateDownloading || j.workerCancel == nil || j.isSingleStream {
		return
	}

	activeCount := 0
	var activeWorkers []*workerHandle
	for _, w := range j.workers {
		if !w.retire.Load() {
			activeCount++
			activeWorkers = append(activeWorkers, w)
		}
	}

	if n > activeCount {
		toSpawn := n - activeCount
		for i := 0; i < toSpawn; i++ {
			h := &workerHandle{id: fmt.Sprintf("worker-%d", j.workerCounter.Add(1))}
			j.workers = append(j.workers, h)
			j.workerWG.Add(1)
			go j.runWorker(j.workerCtx, h)
		}
	} else if n < activeCount {
		toRetire := activeCount - n
		for i := len(activeWorkers) - 1; i >= 0 && toRetire > 0; i-- {
			activeWorkers[i].retire.Store(true)
			if activeWorkers[i].cancel != nil {
				activeWorkers[i].cancel()
			}
			toRetire--
		}
	}
}

// TargetConns returns the desired number of worker connections configured for the job.
func (j *Job) TargetConns() int {
	j.workerMu.Lock()
	defer j.workerMu.Unlock()
	return j.targetConns
}

// ActiveConns returns the number of active, non-retired worker connections.
func (j *Job) ActiveConns() int {
	j.workerMu.Lock()
	defer j.workerMu.Unlock()
	count := 0
	for _, w := range j.workers {
		if !w.retire.Load() {
			count++
		}
	}
	return count
}

// ErrTracker returns the job's ErrorTracker.
func (j *Job) ErrTracker() *ErrorTracker {
	return j.errTracker
}

func (j *Job) runTuner(ctx context.Context, stopCh <-chan struct{}, initialConns int) {
	interval := j.deps.TunerInterval
	if interval <= 0 {
		interval = 4 * time.Second
	}

	cfg := DefaultTunerConfig()
	if j.deps.TunerConfig != nil {
		cfg = *j.deps.TunerConfig
	}
	if j.download.Opts.MaxConns > 0 {
		cfg.MaxConnsPerJob = j.download.Opts.MaxConns
	}

	host := cleanHostString(j.download.URL)
	if j.deps.HostProfiles != nil {
		safe := j.deps.HostProfiles.SafeMaxConns(host)
		if safe > 0 && cfg.MaxConnsPerJob > safe {
			cfg.MaxConnsPerJob = safe
		}
	}

	state := NewTunerState(cfg, initialConns)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
		}

		if ctx.Err() != nil {
			return
		}

		active := j.ActiveConns()
		if active == 0 {
			continue
		}

		if j.deps.HostProfiles != nil {
			safe := j.deps.HostProfiles.SafeMaxConns(host)
			if safe > 0 && state.Config.MaxConnsPerJob > safe {
				state.Config.MaxConnsPerJob = safe
			}
		}

		errWin, retryAfter := j.errTracker.Drain()
		speed := float64(j.progress.Speed1s())

		sample := Sample{
			ThroughputEMA: speed,
			Conns:         active,
			ErrWindow:     errWin,
			RetryAfter:    retryAfter,
		}

		nextState, action := Decide(j.now(), state, sample)
		state = nextState

		if action.Kind == ActionHalveAndWait || errWin.Status429 > 0 {
			if j.deps.HostProfiles != nil {
				j.deps.HostProfiles.Learn429(host, active)
			}
		}

		if action.TargetConns != active {
			j.SetTargetConns(action.TargetConns)
		}

		if action.Wait > 0 {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-time.After(action.Wait):
			}
		}
	}
}

// SetDisableStealing configures whether idle workers attempt to steal segments.
func (j *Job) SetDisableStealing(b bool) {
	j.stealingDisabled = b
	if j.registry != nil {
		j.registry.SetDisabled(b)
	}
}

// SplitCount returns the count of dynamic chunk splits performed by the job.
func (j *Job) SplitCount() int64 {
	if j.registry != nil {
		return j.registry.SplitCount()
	}
	return 0
}

// Registry returns the job's underlying segment registry.
func (j *Job) Registry() *Registry {
	return j.registry
}

// SetSpeedLimit dynamically updates the job's download speed limit in bytes per second.
func (j *Job) SetSpeedLimit(bps int64) {
	if j.limiter != nil {
		j.limiter.SetJobLimit(bps)
	}
}

// Limiter returns the job's LimiterSet.
func (j *Job) Limiter() *LimiterSet {
	return j.limiter
}
