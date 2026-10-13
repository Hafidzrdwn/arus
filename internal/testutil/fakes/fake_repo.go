// Package fakes provides in-memory implementations of outbound hexagonal ports for unit and integration testing.
package fakes

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"context"
	"sort"
	"strings"
	"sync"
)

var _ ports.DownloadRepository = (*FakeRepo)(nil)

// FakeRepo is an in-memory, thread-safe implementation of ports.DownloadRepository.
type FakeRepo struct {
	mu        sync.RWMutex
	downloads map[domain.DownloadID]domain.Download
	segments  map[domain.DownloadID][]domain.Segment
}

// NewFakeRepo instantiates a clean FakeRepo.
func NewFakeRepo() *FakeRepo {
	return &FakeRepo{
		downloads: make(map[domain.DownloadID]domain.Download),
		segments:  make(map[domain.DownloadID][]domain.Segment),
	}
}

// Create persists a new download record. Returns ErrAlreadyExists if the ID is duplicated.
func (r *FakeRepo) Create(_ context.Context, d domain.Download) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.downloads[d.ID]; exists {
		return ports.ErrAlreadyExists
	}
	r.downloads[d.ID] = cloneDownload(d)
	return nil
}

// Get retrieves a download record by ID. Returns ErrNotFound if it does not exist.
func (r *FakeRepo) Get(_ context.Context, id domain.DownloadID) (domain.Download, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	d, exists := r.downloads[id]
	if !exists {
		return domain.Download{}, ports.ErrNotFound
	}
	return cloneDownload(d), nil
}

// List queries download records matching the criteria in ListFilter.
func (r *FakeRepo) List(_ context.Context, f ports.ListFilter) ([]domain.Download, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var results []domain.Download

	for _, d := range r.downloads {
		// Filter by States
		if len(f.States) > 0 {
			stateMatched := false
			for _, st := range f.States {
				if d.State == st {
					stateMatched = true
					break
				}
			}
			if !stateMatched {
				continue
			}
		}

		// Filter by Category
		if f.Category != "" && d.Opts.Category != f.Category {
			continue
		}

		// Filter by QueueID
		if f.QueueID != "" && d.Opts.QueueID != f.QueueID {
			continue
		}

		// Filter by Search (substring matching on URL or FileName)
		if f.Search != "" {
			if !strings.Contains(d.URL, f.Search) && !strings.Contains(d.FileName, f.Search) {
				continue
			}
		}

		results = append(results, cloneDownload(d))
	}

	// Sort deterministically: descending CreatedAt, then ascending ID
	sort.Slice(results, func(i, j int) bool {
		if results[i].CreatedAt.Equal(results[j].CreatedAt) {
			return results[i].ID < results[j].ID
		}
		return results[i].CreatedAt.After(results[j].CreatedAt)
	})

	// Pagination: Offset and Limit
	if f.Offset > 0 {
		if f.Offset >= len(results) {
			return []domain.Download{}, nil
		}
		results = results[f.Offset:]
	}

	if f.Limit > 0 && len(results) > f.Limit {
		results = results[:f.Limit]
	}

	return results, nil
}

// UpdateState atomically updates the State and Err fields of an existing download.
func (r *FakeRepo) UpdateState(_ context.Context, id domain.DownloadID, st domain.State, e *domain.DownloadError) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	d, exists := r.downloads[id]
	if !exists {
		return ports.ErrNotFound
	}

	d.State = st
	if e != nil {
		errCopy := *e
		d.Err = &errCopy
	} else {
		d.Err = nil
	}

	r.downloads[id] = d
	return nil
}

// ReplaceSegments atomically updates the segments and downloaded count for a download.
func (r *FakeRepo) ReplaceSegments(_ context.Context, id domain.DownloadID, segs []domain.Segment, downloaded int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	d, exists := r.downloads[id]
	if !exists {
		return ports.ErrNotFound
	}

	clonedSegs := make([]domain.Segment, len(segs))
	copy(clonedSegs, segs)

	r.segments[id] = clonedSegs
	d.Downloaded = downloaded
	r.downloads[id] = d
	return nil
}

// Segments loads all active segments recorded for a download.
func (r *FakeRepo) Segments(_ context.Context, id domain.DownloadID) ([]domain.Segment, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if _, exists := r.downloads[id]; !exists {
		return nil, ports.ErrNotFound
	}

	src := r.segments[id]
	res := make([]domain.Segment, len(src))
	copy(res, src)

	sort.Slice(res, func(i, j int) bool {
		return res[i].ID < res[j].ID
	})

	return res, nil
}

// PurgeSecrets nullifies sensitive RequestContext fields upon completion or cancellation.
func (r *FakeRepo) PurgeSecrets(_ context.Context, id domain.DownloadID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	d, exists := r.downloads[id]
	if !exists {
		return ports.ErrNotFound
	}

	d.ReqCtx.Cookies = nil
	d.ReqCtx.RawCookie = ""
	d.ReqCtx.Body = nil
	d.ReqCtx.Headers = nil

	r.downloads[id] = d
	return nil
}

// Delete removes the download and its associated segments.
func (r *FakeRepo) Delete(_ context.Context, id domain.DownloadID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.downloads[id]; !exists {
		return ports.ErrNotFound
	}

	delete(r.downloads, id)
	delete(r.segments, id)
	return nil
}

func cloneDownload(d domain.Download) domain.Download {
	copied := d
	if d.Err != nil {
		errCopy := *d.Err
		copied.Err = &errCopy
	}
	if d.ReqCtx.Headers != nil {
		copied.ReqCtx.Headers = d.ReqCtx.Headers.Clone()
	}
	if d.ReqCtx.Cookies != nil {
		copied.ReqCtx.Cookies = make([]domain.Cookie, len(d.ReqCtx.Cookies))
		copy(copied.ReqCtx.Cookies, d.ReqCtx.Cookies)
	}
	if d.ReqCtx.Body != nil {
		copied.ReqCtx.Body = make([]byte, len(d.ReqCtx.Body))
		copy(copied.ReqCtx.Body, d.ReqCtx.Body)
	}
	return copied
}
