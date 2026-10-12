package domain

import "time"

// ProgressSnapshot represents an immutable point-in-time snapshot of download progress.
type ProgressSnapshot struct {
	Downloaded int64         `json:"downloaded"`
	Total      int64         `json:"total"`
	Bps        int64         `json:"bps"`
	Eta        time.Duration `json:"eta"`
	Percentage float64       `json:"percentage"`
}

// CalculateETA computes the estimated time remaining based on downloaded bytes,
// total expected bytes, and current throughput in bytes-per-second.
// Returns:
// - 0 when total <= 0 or downloaded >= total (already done or indeterminate completed)
// - -1 when bps <= 0 (stalled or speed indeterminate)
// - positive duration in seconds otherwise
func CalculateETA(downloaded, total, bps int64) time.Duration {
	if total <= 0 || downloaded >= total {
		return 0
	}
	if bps <= 0 {
		return -1
	}
	remaining := total - downloaded
	seconds := remaining / bps
	return time.Duration(seconds) * time.Second
}

// NewProgressSnapshot constructs a ProgressSnapshot with computed ETA and percentage.
func NewProgressSnapshot(downloaded, total, bps int64) ProgressSnapshot {
	var pct float64
	if total > 0 {
		pct = (float64(downloaded) / float64(total)) * 100.0
		if pct > 100.0 {
			pct = 100.0
		}
	}
	return ProgressSnapshot{
		Downloaded: downloaded,
		Total:      total,
		Bps:        bps,
		Eta:        CalculateETA(downloaded, total, bps),
		Percentage: pct,
	}
}
