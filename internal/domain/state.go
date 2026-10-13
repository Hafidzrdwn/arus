package domain

import (
	"errors"
	"fmt"
)

// Event is the marker interface for lifecycle state machine events.
type Event interface {
	isEvent()
}

// SlotGranted indicates that the scheduler granted a concurrency slot for the download.
type SlotGranted struct{}

// ProbeOK indicates probe request succeeded, carrying whether this is a resume or new download.
type ProbeOK struct{ Resume bool }

// Allocated indicates pre-allocation and segment initialization succeeded.
type Allocated struct{}

// PauseRequested indicates a pause request was initiated.
type PauseRequested struct{}

// Paused indicates download has successfully paused and checkpoints persisted.
type Paused struct{}

// AllDone indicates all segments have finished downloading.
type AllDone struct{}

// VerifyOK indicates verification, checksum, and file finalization succeeded.
type VerifyOK struct{}

// Fatal indicates an unrecoverable or fatal error occurred.
type Fatal struct{ Err *DownloadError }

// Retry indicates a failed download should be retried from Queued.
type Retry struct{}

// RefreshLink indicates a failed download's link is refreshed.
type RefreshLink struct{}

// Cancel indicates the download is cancelled.
type Cancel struct{}

// Resume indicates a paused download should resume.
type Resume struct{}

func (SlotGranted) isEvent()    {}
func (ProbeOK) isEvent()        {}
func (Allocated) isEvent()      {}
func (PauseRequested) isEvent() {}
func (Paused) isEvent()         {}
func (AllDone) isEvent()        {}
func (VerifyOK) isEvent()       {}
func (Fatal) isEvent()          {}
func (Retry) isEvent()          {}
func (RefreshLink) isEvent()    {}
func (Cancel) isEvent()         {}
func (Resume) isEvent()         {}

// EventSlotGranted is an alias for SlotGranted.
type EventSlotGranted = SlotGranted

// EventProbeOK is an alias for ProbeOK.
type EventProbeOK = ProbeOK

// EventAllocated is an alias for Allocated.
type EventAllocated = Allocated

// EventPauseRequested is an alias for PauseRequested.
type EventPauseRequested = PauseRequested

// EventPaused is an alias for Paused.
type EventPaused = Paused

// EventAllDone is an alias for AllDone.
type EventAllDone = AllDone

// EventVerifyOK is an alias for VerifyOK.
type EventVerifyOK = VerifyOK

// EventFatal is an alias for Fatal.
type EventFatal = Fatal

// EventRetry is an alias for Retry.
type EventRetry = Retry

// EventRefreshLink is an alias for RefreshLink.
type EventRefreshLink = RefreshLink

// EventCancel is an alias for Cancel.
type EventCancel = Cancel

// EventResume is an alias for Resume.
type EventResume = Resume

// Transition transitions the Download state according to TECH_SPEC §5.
// Any illegal transition returns an error wrapping ErrIllegalTransition.
func (d *Download) Transition(ev Event) error {
	if d == nil {
		return errors.New("cannot transition nil download")
	}

	switch d.State {
	case StateQueued:
		switch e := ev.(type) {
		case SlotGranted:
			d.State = StateProbing
			return nil
		case Cancel:
			d.State = StateCancelled
			return nil
		case Fatal:
			d.State = StateFailed
			d.Err = e.Err
			return nil
		}

	case StateProbing:
		switch e := ev.(type) {
		case ProbeOK:
			if e.Resume {
				d.State = StateDownloading
			} else {
				d.State = StateAllocating
			}
			return nil
		case Fatal:
			d.State = StateFailed
			d.Err = e.Err
			return nil
		}

	case StateAllocating:
		switch e := ev.(type) {
		case Allocated:
			d.State = StateDownloading
			return nil
		case Fatal:
			d.State = StateFailed
			d.Err = e.Err
			return nil
		}

	case StateDownloading:
		switch e := ev.(type) {
		case PauseRequested, Paused:
			d.State = StatePaused
			return nil
		case AllDone:
			d.State = StateVerifying
			return nil
		case Fatal:
			d.State = StateFailed
			d.Err = e.Err
			return nil
		}

	case StatePaused:
		switch e := ev.(type) {
		case Resume:
			d.State = StateQueued
			return nil
		case RefreshLink:
			d.State = StateProbing
			d.Err = nil
			return nil
		case Cancel:
			d.State = StateCancelled
			return nil
		case Fatal:
			d.State = StateFailed
			d.Err = e.Err
			return nil
		}

	case StateVerifying:
		switch e := ev.(type) {
		case VerifyOK:
			d.State = StateCompleted
			return nil
		case Fatal:
			d.State = StateFailed
			d.Err = e.Err
			return nil
		}

	case StateFailed:
		switch ev.(type) {
		case Retry:
			d.State = StateQueued
			d.Err = nil
			return nil
		case RefreshLink:
			d.State = StateProbing
			d.Err = nil
			return nil
		case Cancel:
			d.State = StateCancelled
			return nil
		}

	case StateCompleted, StateCancelled:
		return fmt.Errorf("%w: terminal state %s cannot transition on event %T", ErrIllegalTransition, d.State, ev)
	}

	return fmt.Errorf("%w: illegal transition from %s on event %T", ErrIllegalTransition, d.State, ev)
}
