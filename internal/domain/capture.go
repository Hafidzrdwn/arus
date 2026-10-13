package domain

import (
	"net/http"
	"time"
)

// Capture represents a download interception request from browser or local hooks.
type Capture struct {
	ID         string        `json:"id"`
	URL        string        `json:"url"`
	FinalURL   string        `json:"final_url"`
	Method     string        `json:"method"`
	Headers    http.Header   `json:"headers"`
	Cookies    []Cookie      `json:"cookies"`
	RawCookie  string        `json:"raw_cookie"`
	FileName   string        `json:"file_name"`
	MimeType   string        `json:"mime_type"`
	TotalSize  int64         `json:"total_size"`
	Origin     CaptureOrigin `json:"origin"`
	TabID      int           `json:"tab_id"`
	CapturedAt time.Time     `json:"captured_at"`
}

// CaptureResult represents the decision taken on an intercepted capture.
type CaptureResult struct {
	Accepted   bool       `json:"accepted"`
	DownloadID DownloadID `json:"download_id,omitempty"`
	Reason     string     `json:"reason,omitempty"`
	AskUser    bool       `json:"ask_user,omitempty"`
}
