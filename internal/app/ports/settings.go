package ports

import "context"

// Settings defines the outbound driven port for reading and updating application configuration.
// Implementations must be safe for concurrent access.
type Settings interface {
	// Get retrieves the raw string value for key. Returns ErrNotFound if key does not exist.
	Get(ctx context.Context, key string) (string, error)
	// Set stores or updates key with the given string value.
	Set(ctx context.Context, key, val string) error
	// GetInt retrieves key parsed as an integer, returning defaultVal if key is not found.
	GetInt(ctx context.Context, key string, defaultVal int) (int, error)
	// SetInt stores an integer setting under key.
	SetInt(ctx context.Context, key string, val int) error
	// GetBool retrieves key parsed as a boolean, returning defaultVal if key is not found.
	GetBool(ctx context.Context, key string, defaultVal bool) (bool, error)
	// SetBool stores a boolean setting under key.
	SetBool(ctx context.Context, key string, val bool) error
	// GetString retrieves key, returning defaultVal if key is not found.
	GetString(ctx context.Context, key string, defaultVal string) (string, error)
}
