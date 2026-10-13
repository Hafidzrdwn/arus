package ports

import (
	"arus/internal/domain"
	"context"
	"errors"
)

var (
	// ErrNotFound is returned when a requested download or entity does not exist.
	ErrNotFound = errors.New("entity not found")
	// ErrAlreadyExists is returned when attempting to insert an entity with an existing ID.
	ErrAlreadyExists = errors.New("entity already exists")
)

// ListFilter specifies filtering, pagination, and search criteria for querying downloads.
type ListFilter struct {
	// States restricts results to downloads matching any of the specified states.
	States []domain.State
	// Category filters downloads by category identifier (exact match).
	Category string
	// QueueID filters downloads by assigned queue identifier (exact match).
	QueueID string
	// Search provides substring matching on URL or FileName.
	Search string
	// Offset specifies pagination offset (0-indexed).
	Offset int
	// Limit specifies maximum number of items to return (0 means unlimited or default).
	Limit int
}

// DownloadRepository defines the outbound driven port for persisting downloads and segment checkpoints.
// All implementations must be safe for concurrent access across multiple goroutines.
type DownloadRepository interface {
	// Create persists a new download record. Returns ErrAlreadyExists if the ID is duplicated.
	Create(ctx context.Context, d domain.Download) error

	// Get retrieves a download record by ID. Returns ErrNotFound if it does not exist.
	Get(ctx context.Context, id domain.DownloadID) (domain.Download, error)

	// Update updates persisted mutable fields of an existing download record.
	// Returns ErrNotFound if the record does not exist.
	Update(ctx context.Context, d domain.Download) error

	// List queries download records matching the criteria in ListFilter.
	List(ctx context.Context, f ListFilter) ([]domain.Download, error)

	// UpdateState atomically updates the State and Err fields of an existing download.
	// Returns ErrNotFound if the download does not exist.
	UpdateState(ctx context.Context, id domain.DownloadID, st domain.State, e *domain.DownloadError) error

	// ReplaceSegments atomically deletes existing segments for the download, inserts new segs,
	// and updates total downloaded bytes within a single transaction.
	// Returns ErrNotFound if the download does not exist.
	ReplaceSegments(ctx context.Context, id domain.DownloadID, segs []domain.Segment, downloaded int64) error

	// Segments loads all active segments recorded for a download.
	Segments(ctx context.Context, id domain.DownloadID) ([]domain.Segment, error)

	// PurgeSecrets nullifies sensitive RequestContext (cookies, tokens) upon completion or cancellation.
	PurgeSecrets(ctx context.Context, id domain.DownloadID) error

	// Delete removes the download and its associated segments.
	Delete(ctx context.Context, id domain.DownloadID) error
}
