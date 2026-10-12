package observability

import (
	"io"
	"log/slog"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

// LogConfig holds configuration options for logger construction.
type LogConfig struct {
	// FilePath is the destination path for the JSON log file.
	FilePath string
	// Level determines the minimum log level (e.g. slog.LevelInfo, slog.LevelDebug).
	Level slog.Level
	// DevMode, when true, sends human-readable text logs to stderr instead of JSON to file.
	DevMode bool
}

// NewLogger constructs an *slog.Logger based on the provided configuration.
// If DevMode is enabled, logs are formatted as text and directed to os.Stderr.
// Otherwise, logs are formatted as JSON and written to FilePath using lumberjack
// with 10 MiB rotation and up to 5 retained backup files. If FilePath is empty
// in non-dev mode, logs default to os.Stdout.
func NewLogger(cfg LogConfig) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: cfg.Level,
	}

	if cfg.DevMode {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}

	var w io.Writer
	if cfg.FilePath != "" {
		w = &lumberjack.Logger{
			Filename:   cfg.FilePath,
			MaxSize:    10, // 10 MiB
			MaxBackups: 5,  // retain 5 backup files
			LocalTime:  true,
			Compress:   true,
		}
	} else {
		w = os.Stdout
	}

	return slog.New(slog.NewJSONHandler(w, opts))
}
