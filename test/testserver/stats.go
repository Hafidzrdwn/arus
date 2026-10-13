package testserver

import (
	"net/http"
	"sync"
)

// Stats captures observability metrics and history of requests handled by Server.
type Stats struct {
	TotalRequests   int64
	ActiveConns     int64
	MaxConnsSeen    int64
	TotalBytesSent  int64
	RangesRequested []string
	HeadersReceived []http.Header
}

type statsTracker struct {
	mu              sync.Mutex
	totalRequests   int64
	activeConns     int64
	maxConnsSeen    int64
	totalBytesSent  int64
	rangesRequested []string
	headersReceived []http.Header
}

func newStatsTracker() *statsTracker {
	return &statsTracker{}
}

func (s *statsTracker) recordRequest(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalRequests++
	s.activeConns++
	if s.activeConns > s.maxConnsSeen {
		s.maxConnsSeen = s.activeConns
	}

	if rng := r.Header.Get("Range"); rng != "" {
		s.rangesRequested = append(s.rangesRequested, rng)
	}

	s.headersReceived = append(s.headersReceived, r.Header.Clone())
}

func (s *statsTracker) releaseConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeConns > 0 {
		s.activeConns--
	}
}

func (s *statsTracker) addBytes(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalBytesSent += n
}

func (s *statsTracker) getBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalBytesSent
}

func (s *statsTracker) snapshot() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	ranges := make([]string, len(s.rangesRequested))
	copy(ranges, s.rangesRequested)

	headers := make([]http.Header, len(s.headersReceived))
	for i, h := range s.headersReceived {
		headers[i] = h.Clone()
	}

	return Stats{
		TotalRequests:   s.totalRequests,
		ActiveConns:     s.activeConns,
		MaxConnsSeen:    s.maxConnsSeen,
		TotalBytesSent:  s.totalBytesSent,
		RangesRequested: ranges,
		HeadersReceived: headers,
	}
}
