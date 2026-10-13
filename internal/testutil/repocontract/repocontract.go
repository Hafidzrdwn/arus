// Package repocontract provides a reusable compliance test suite for implementations of ports.DownloadRepository.
package repocontract

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// RepoContractTest runs a comprehensive suite of contract tests against any ports.DownloadRepository implementation.
//
//nolint:revive // function name RepoContractTest requested by specification
func RepoContractTest(t *testing.T, factory func() ports.DownloadRepository) {
	t.Helper()

	t.Run("Create_Get_Success", func(t *testing.T) {
		repo := factory()
		ctx := context.Background()

		dl := newSampleDownload("01JCREATE00000000000000001", "https://example.com/test1.zip", "test1.zip")
		if err := repo.Create(ctx, dl); err != nil {
			t.Fatalf("Create() unexpected error: %v", err)
		}

		got, err := repo.Get(ctx, dl.ID)
		if err != nil {
			t.Fatalf("Get() unexpected error: %v", err)
		}
		if got.ID != dl.ID {
			t.Errorf("Get() ID = %q, want %q", got.ID, dl.ID)
		}
		if got.URL != dl.URL {
			t.Errorf("Get() URL = %q, want %q", got.URL, dl.URL)
		}
		if got.State != dl.State {
			t.Errorf("Get() State = %q, want %q", got.State, dl.State)
		}
		if got.TotalSize != dl.TotalSize {
			t.Errorf("Get() TotalSize = %d, want %d", got.TotalSize, dl.TotalSize)
		}
	})

	t.Run("Create_Duplicate_ErrAlreadyExists", func(t *testing.T) {
		repo := factory()
		ctx := context.Background()

		dl := newSampleDownload("01JDUP00000000000000000001", "https://example.com/dup.zip", "dup.zip")
		if err := repo.Create(ctx, dl); err != nil {
			t.Fatalf("first Create() unexpected error: %v", err)
		}

		err := repo.Create(ctx, dl)
		if !errors.Is(err, ports.ErrAlreadyExists) {
			t.Fatalf("second Create() err = %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("UpdateState_Success", func(t *testing.T) {
		repo := factory()
		ctx := context.Background()

		dl := newSampleDownload("01JSTATE000000000000000001", "https://example.com/state.zip", "state.zip")
		if err := repo.Create(ctx, dl); err != nil {
			t.Fatalf("Create() error: %v", err)
		}

		dlErr := &domain.DownloadError{
			Code:        domain.ErrCodeNetwork,
			Message:     "connection reset",
			Recoverable: true,
		}

		if err := repo.UpdateState(ctx, dl.ID, domain.StateFailed, dlErr); err != nil {
			t.Fatalf("UpdateState() error: %v", err)
		}

		got, err := repo.Get(ctx, dl.ID)
		if err != nil {
			t.Fatalf("Get() error: %v", err)
		}
		if got.State != domain.StateFailed {
			t.Errorf("State = %v, want %v", got.State, domain.StateFailed)
		}
		if got.Err == nil || got.Err.Code != domain.ErrCodeNetwork {
			t.Errorf("Err = %+v, want code %v", got.Err, domain.ErrCodeNetwork)
		}
	})

	t.Run("Delete_Success", func(t *testing.T) {
		repo := factory()
		ctx := context.Background()

		dl := newSampleDownload("01JDEL000000000000000000001", "https://example.com/del.zip", "del.zip")
		if err := repo.Create(ctx, dl); err != nil {
			t.Fatalf("Create() error: %v", err)
		}

		if err := repo.Delete(ctx, dl.ID); err != nil {
			t.Fatalf("Delete() error: %v", err)
		}

		_, err := repo.Get(ctx, dl.ID)
		if !errors.Is(err, ports.ErrNotFound) {
			t.Fatalf("Get() after Delete err = %v, want ErrNotFound", err)
		}
	})

	t.Run("NotFound_Operations", func(t *testing.T) {
		repo := factory()
		ctx := context.Background()
		nonExistent := domain.DownloadID("01JNONEXISTENT000000000001")

		if _, err := repo.Get(ctx, nonExistent); !errors.Is(err, ports.ErrNotFound) {
			t.Errorf("Get(nonExistent) err = %v, want ErrNotFound", err)
		}
		if err := repo.UpdateState(ctx, nonExistent, domain.StateDownloading, nil); !errors.Is(err, ports.ErrNotFound) {
			t.Errorf("UpdateState(nonExistent) err = %v, want ErrNotFound", err)
		}
		if err := repo.ReplaceSegments(ctx, nonExistent, nil, 0); !errors.Is(err, ports.ErrNotFound) {
			t.Errorf("ReplaceSegments(nonExistent) err = %v, want ErrNotFound", err)
		}
		if err := repo.PurgeSecrets(ctx, nonExistent); !errors.Is(err, ports.ErrNotFound) {
			t.Errorf("PurgeSecrets(nonExistent) err = %v, want ErrNotFound", err)
		}
		if err := repo.Delete(ctx, nonExistent); !errors.Is(err, ports.ErrNotFound) {
			t.Errorf("Delete(nonExistent) err = %v, want ErrNotFound", err)
		}
	})

	t.Run("ReplaceSegments_And_Query", func(t *testing.T) {
		repo := factory()
		ctx := context.Background()

		dl := newSampleDownload("01JSEGS0000000000000000001", "https://example.com/segs.zip", "segs.zip")
		if err := repo.Create(ctx, dl); err != nil {
			t.Fatalf("Create() error: %v", err)
		}

		initSegs := []domain.Segment{
			{ID: 0, Start: 0, End: 500, Cursor: 250},
			{ID: 1, Start: 500, End: 1000, Cursor: 500},
		}
		if err := repo.ReplaceSegments(ctx, dl.ID, initSegs, 250); err != nil {
			t.Fatalf("ReplaceSegments(init) error: %v", err)
		}

		gotSegs, err := repo.Segments(ctx, dl.ID)
		if err != nil {
			t.Fatalf("Segments() error: %v", err)
		}
		if len(gotSegs) != 2 {
			t.Fatalf("len(gotSegs) = %d, want 2", len(gotSegs))
		}

		updatedDl, err := repo.Get(ctx, dl.ID)
		if err != nil {
			t.Fatalf("Get() error: %v", err)
		}
		if updatedDl.Downloaded != 250 {
			t.Errorf("Downloaded = %d, want 250", updatedDl.Downloaded)
		}

		// Atomically replace with 3 new segments
		newSegs := []domain.Segment{
			{ID: 0, Start: 0, End: 250, Cursor: 250},
			{ID: 1, Start: 250, End: 500, Cursor: 300},
			{ID: 2, Start: 500, End: 1000, Cursor: 600},
		}
		if err := repo.ReplaceSegments(ctx, dl.ID, newSegs, 400); err != nil {
			t.Fatalf("ReplaceSegments(newSegs) error: %v", err)
		}

		gotSegs2, err := repo.Segments(ctx, dl.ID)
		if err != nil {
			t.Fatalf("Segments() error: %v", err)
		}
		if len(gotSegs2) != 3 {
			t.Fatalf("len(gotSegs2) = %d, want 3", len(gotSegs2))
		}
		if gotSegs2[1].Cursor != 300 {
			t.Errorf("gotSegs2[1].Cursor = %d, want 300", gotSegs2[1].Cursor)
		}

		updatedDl2, err := repo.Get(ctx, dl.ID)
		if err != nil {
			t.Fatalf("Get() error: %v", err)
		}
		if updatedDl2.Downloaded != 400 {
			t.Errorf("Downloaded = %d, want 400", updatedDl2.Downloaded)
		}
	})

	t.Run("PurgeSecrets_Success", func(t *testing.T) {
		repo := factory()
		ctx := context.Background()

		dl := newSampleDownload("01JPURGE000000000000000001", "https://example.com/secret.zip", "secret.zip")
		dl.ReqCtx.Cookies = []domain.Cookie{{Name: "session", Value: "secret_val"}}
		dl.ReqCtx.RawCookie = "session=secret_val"
		dl.ReqCtx.Body = []byte("sensitive-payload")
		dl.ReqCtx.Headers = http.Header{"Authorization": []string{"Bearer token123"}}

		if err := repo.Create(ctx, dl); err != nil {
			t.Fatalf("Create() error: %v", err)
		}

		if err := repo.PurgeSecrets(ctx, dl.ID); err != nil {
			t.Fatalf("PurgeSecrets() error: %v", err)
		}

		got, err := repo.Get(ctx, dl.ID)
		if err != nil {
			t.Fatalf("Get() error: %v", err)
		}
		if len(got.ReqCtx.Cookies) != 0 {
			t.Errorf("expected empty cookies, got %v", got.ReqCtx.Cookies)
		}
		if got.ReqCtx.RawCookie != "" {
			t.Errorf("expected empty RawCookie, got %q", got.ReqCtx.RawCookie)
		}
		if len(got.ReqCtx.Body) != 0 {
			t.Errorf("expected empty Body, got %v", got.ReqCtx.Body)
		}
	})

	t.Run("List_Filter_And_Pagination", func(t *testing.T) {
		repo := factory()
		ctx := context.Background()

		dls := []domain.Download{
			{
				ID:        "01JLIST00000000000000000001",
				URL:       "https://files.org/archive_one.tar",
				FileName:  "archive_one.tar",
				State:     domain.StateQueued,
				Opts:      domain.Options{Category: "archives", QueueID: "queue-main"},
				CreatedAt: time.Now().Add(-30 * time.Minute),
			},
			{
				ID:        "01JLIST00000000000000000002",
				URL:       "https://files.org/movie_two.mp4",
				FileName:  "movie_two.mp4",
				State:     domain.StateDownloading,
				Opts:      domain.Options{Category: "video", QueueID: "queue-main"},
				CreatedAt: time.Now().Add(-20 * time.Minute),
			},
			{
				ID:        "01JLIST00000000000000000003",
				URL:       "https://media.net/video_three.mp4",
				FileName:  "video_three.mp4",
				State:     domain.StateCompleted,
				Opts:      domain.Options{Category: "video", QueueID: "queue-alt"},
				CreatedAt: time.Now().Add(-10 * time.Minute),
			},
		}

		for _, item := range dls {
			if err := repo.Create(ctx, item); err != nil {
				t.Fatalf("Create(%s) error: %v", item.ID, err)
			}
		}

		// Filter by state
		res, err := repo.List(ctx, ports.ListFilter{States: []domain.State{domain.StateQueued, domain.StateDownloading}})
		if err != nil {
			t.Fatalf("List(states) error: %v", err)
		}
		if len(res) != 2 {
			t.Errorf("List(states) len = %d, want 2", len(res))
		}

		// Filter by Category
		res, err = repo.List(ctx, ports.ListFilter{Category: "video"})
		if err != nil {
			t.Fatalf("List(category) error: %v", err)
		}
		if len(res) != 2 {
			t.Errorf("List(category) len = %d, want 2", len(res))
		}

		// Filter by QueueID
		res, err = repo.List(ctx, ports.ListFilter{QueueID: "queue-alt"})
		if err != nil {
			t.Fatalf("List(queue) error: %v", err)
		}
		if len(res) != 1 {
			t.Fatalf("List(queue) len = %d, want 1", len(res))
		}
		if res[0].ID != "01JLIST00000000000000000003" {
			t.Errorf("List(queue) ID = %q, want 01JLIST00000000000000000003", res[0].ID)
		}

		// Filter by Search (substring matching URL or FileName)
		res, err = repo.List(ctx, ports.ListFilter{Search: "media.net"})
		if err != nil {
			t.Fatalf("List(search url) error: %v", err)
		}
		if len(res) != 1 {
			t.Errorf("List(search) len = %d, want 1", len(res))
		}

		res, err = repo.List(ctx, ports.ListFilter{Search: "movie_two"})
		if err != nil {
			t.Fatalf("List(search filename) error: %v", err)
		}
		if len(res) != 1 {
			t.Errorf("List(search) len = %d, want 1", len(res))
		}

		// Pagination: limit 1, offset 1
		res, err = repo.List(ctx, ports.ListFilter{Limit: 1, Offset: 1})
		if err != nil {
			t.Fatalf("List(pagination) error: %v", err)
		}
		if len(res) != 1 {
			t.Errorf("List(pagination) len = %d, want 1", len(res))
		}
	})
}

func newSampleDownload(id domain.DownloadID, url, name string) domain.Download {
	return domain.Download{
		ID:           id,
		URL:          url,
		FileName:     name,
		DestDir:      "C:/Downloads",
		PartPath:     "C:/Downloads/" + name + ".arus-part",
		TotalSize:    1000,
		Downloaded:   0,
		State:        domain.StateQueued,
		RangeSupport: domain.RangeSupportYes,
		CreatedAt:    time.Now(),
	}
}
