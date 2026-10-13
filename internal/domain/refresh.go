package domain

import (
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
)

// MatchRefresh scores how well an intercepted capture matches a download job needing refresh.
func MatchRefresh(job Download, capture Capture) (int, bool) {
	var score int

	if job.TotalSize > 0 && capture.TotalSize > 0 {
		if job.TotalSize != capture.TotalSize {
			return 0, false
		}
		score += 40
	}

	capETag := getHeader(capture.Headers, "ETag")
	if capETag == "" {
		capETag = getHeader(capture.Headers, "If-None-Match")
	}
	if capETag == "" {
		capETag = getHeader(capture.Headers, "If-Range")
	}

	cleanJobETag := strings.Trim(strings.TrimPrefix(job.Validators.ETag, "W/"), `"`)
	cleanCapETag := strings.Trim(strings.TrimPrefix(capETag, "W/"), `"`)
	if cleanJobETag != "" && cleanCapETag != "" {
		if cleanJobETag != cleanCapETag {
			return 0, false
		}
		score += 30
	}

	jobHost := hostFromRawURL(job.URL)
	if jobHost == "" && job.FinalURL != "" {
		jobHost = hostFromRawURL(job.FinalURL)
	}
	capHost := hostFromRawURL(capture.URL)
	if capHost == "" && capture.FinalURL != "" {
		capHost = hostFromRawURL(capture.FinalURL)
	}

	if jobHost != "" && capHost != "" {
		switch {
		case jobHost == capHost:
			score += 20
		case strings.HasSuffix(jobHost, "."+capHost) || strings.HasSuffix(capHost, "."+jobHost):
			score += 15
		default:
			jobParts := strings.Split(jobHost, ".")
			capParts := strings.Split(capHost, ".")
			if len(jobParts) >= 2 && len(capParts) >= 2 {
				if jobParts[len(jobParts)-1] == capParts[len(capParts)-1] &&
					jobParts[len(jobParts)-2] == capParts[len(capParts)-2] {
					score += 10
				}
			}
		}
	}

	jobName := strings.ToLower(filepath.Base(job.FileName))
	capName := strings.ToLower(filepath.Base(capture.FileName))
	if jobName != "" && capName != "" {
		if jobName == capName {
			score += 20
		} else if ext := filepath.Ext(jobName); ext != "" && ext == filepath.Ext(capName) {
			score += 5
		}
	}

	if score >= 50 {
		return score, true
	}
	return score, false
}

func hostFromRawURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

func getHeader(h http.Header, name string) string {
	if h == nil {
		return ""
	}
	if val := h.Get(name); val != "" {
		return val
	}
	lowerName := strings.ToLower(name)
	for k, v := range h {
		if strings.ToLower(k) == lowerName && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}
