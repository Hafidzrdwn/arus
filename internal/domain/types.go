package domain

import (
	"net/http"
	"time"
)

// DownloadID is the unique identifier for a download (ULID string).
type DownloadID string

// State represents the lifecycle state of a download.
type State string

// Download lifecycle states specified in TECH_SPEC §4.1 and §5.
const (
	StateQueued      State = "queued"
	StateProbing     State = "probing"
	StateAllocating  State = "allocating"
	StateDownloading State = "downloading"
	StatePaused      State = "paused"
	StateVerifying   State = "verifying" // finalize + checksum + rename (zero merge)
	StateCompleted   State = "completed"
	StateFailed      State = "failed"
	StateCancelled   State = "cancelled"
)

// RangeSupport represents server byte-range support.
type RangeSupport int

// Server byte-range support values.
const (
	RangeSupportNo         RangeSupport = 0
	RangeSupportYes        RangeSupport = 1
	RangeSupportUnverified RangeSupport = 2
)

// ConflictPolicy defines resolution when destination file already exists.
type ConflictPolicy string

// File collision resolution policies.
const (
	ConflictPolicyRename    ConflictPolicy = "rename"
	ConflictPolicyOverwrite ConflictPolicy = "overwrite"
	ConflictPolicyAsk       ConflictPolicy = "ask"
)

// CaptureOrigin indicates where the download request originated from.
type CaptureOrigin string

// Download capture origin types.
const (
	CaptureOriginExtension CaptureOrigin = "extension"
	CaptureOriginMITM      CaptureOrigin = "mitm"
	CaptureOriginProtocol  CaptureOrigin = "protocol"
	CaptureOriginClipboard CaptureOrigin = "clipboard"
	CaptureOriginManual    CaptureOrigin = "manual"
)

// Validators contains cache and validation headers from the server.
type Validators struct {
	ETag         string `json:"etag"`
	LastModified string `json:"last_modified"`
}

// Options contains configurable settings for a download job.
type Options struct {
	MaxConns         int            `json:"max_conns"`
	SpeedLimitBps    int64          `json:"speed_limit_bps"`
	ChecksumAlgo     string         `json:"checksum_algo"`
	ChecksumExpected string         `json:"checksum_expected"`
	ConflictPolicy   ConflictPolicy `json:"conflict_policy"`
	Category         string         `json:"category"`
	QueueID          string         `json:"queue_id"`
	Priority         int            `json:"priority"`
}

// Cookie represents a structured browser cookie.
type Cookie struct {
	Name    string    `json:"name"`
	Value   string    `json:"value"`
	Domain  string    `json:"domain"`
	Path    string    `json:"path"`
	Secure  bool      `json:"secure"`
	Expires time.Time `json:"expires"`
}

// RequestContext holds captured browser session parameters.
type RequestContext struct {
	Method          string         `json:"method"`
	Headers         http.Header    `json:"headers"`
	Referer         string         `json:"referer"`
	UserAgent       string         `json:"user_agent"`
	Cookies         []Cookie       `json:"cookies"`
	RawCookie       string         `json:"raw_cookie"`
	CookieScopeHost string         `json:"cookie_scope_host"`
	Body            []byte         `json:"body"`
	Origin          CaptureOrigin  `json:"origin"`
	CapturedAt      time.Time      `json:"captured_at"`
	Jar             http.CookieJar `json:"-"`
}

// Download is the central aggregate entity representing a download item.
type Download struct {
	ID           DownloadID     `json:"id"`
	URL          string         `json:"url"`
	FinalURL     string         `json:"final_url"`
	FileName     string         `json:"file_name"`
	DestDir      string         `json:"dest_dir"`
	PartPath     string         `json:"part_path"`
	TotalSize    int64          `json:"total_size"`
	Downloaded   int64          `json:"downloaded"`
	State        State          `json:"state"`
	Err          *DownloadError `json:"err,omitempty"`
	RangeSupport RangeSupport   `json:"range_support"`
	Validators   Validators     `json:"validators"`
	Opts         Options        `json:"opts"`
	ReqCtx       RequestContext `json:"req_ctx"`
	CreatedAt    time.Time      `json:"created_at"`
	StartedAt    time.Time      `json:"started_at"`
	CompletedAt  time.Time      `json:"completed_at"`
}

// ContentRange represents parsed HTTP Content-Range information.
type ContentRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	Total int64 `json:"total"`
}

// ProbeResult holds the outcome of probing an origin URL.
type ProbeResult struct {
	RangeSupport RangeSupport   `json:"range_support"`
	TotalSize    int64          `json:"total_size"`
	Validators   Validators     `json:"validators"`
	FileName     string         `json:"file_name"`
	ContentType  string         `json:"content_type"`
	FinalURL     string         `json:"final_url"`
	Warnings     []string       `json:"warnings"`
	Jar          http.CookieJar `json:"-"`
}
