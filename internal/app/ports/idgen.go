package ports

import "arus/internal/domain"

// IDGenerator creates unique identifiers for domain entities.
type IDGenerator interface {
	// New returns a newly generated unique DownloadID.
	New() domain.DownloadID
}
