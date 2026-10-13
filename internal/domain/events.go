package domain

import "time"

// DownloadAddedEvent is published when a new download is queued or created.
type DownloadAddedEvent struct {
	ID       DownloadID `json:"id"`
	Download Download   `json:"download"`
}

func (DownloadAddedEvent) isEvent() {}

// DownloadStateChangedEvent is published whenever a download transitions between states.
type DownloadStateChangedEvent struct {
	ID    DownloadID     `json:"id"`
	State State          `json:"state"`
	Err   *DownloadError `json:"err,omitempty"`
}

func (DownloadStateChangedEvent) isEvent() {}

// DownloadProgressEvent is published periodically with download throughput metrics.
type DownloadProgressEvent struct {
	ID         DownloadID    `json:"id"`
	Downloaded int64         `json:"downloaded"`
	Total      int64         `json:"total"`
	Bps        int64         `json:"bps"`
	Eta        time.Duration `json:"eta"`
}

func (DownloadProgressEvent) isEvent() {}

// DownloadCompletedEvent is published when a download successfully verifies and finalizes.
type DownloadCompletedEvent struct {
	ID        DownloadID `json:"id"`
	FinalPath string     `json:"final_path"`
}

func (DownloadCompletedEvent) isEvent() {}

// CapturePendingEvent is published when an intercepted download requires user confirmation.
type CapturePendingEvent struct {
	CaptureID string  `json:"capture_id"`
	Capture   Capture `json:"capture"`
}

func (CapturePendingEvent) isEvent() {}

// InterceptorStatusEvent is published when an interceptor changes active status.
type InterceptorStatusEvent struct {
	Name    string `json:"name"`
	Running bool   `json:"running"`
	Err     string `json:"err,omitempty"`
}

func (InterceptorStatusEvent) isEvent() {}
