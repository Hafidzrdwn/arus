package engine_test

import (
	"arus/internal/domain"
	"arus/internal/engine"
	"context"
	"errors"
	"io"
	"net/http"
	"syscall"
	"testing"
	"time"
)

func TestRetry_Classify(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		want   engine.Class
	}{
		{
			name:   "Status 429 Throttle",
			status: http.StatusTooManyRequests,
			want:   engine.ClassThrottle,
		},
		{
			name:   "Status 503 Throttle",
			status: http.StatusServiceUnavailable,
			want:   engine.ClassThrottle,
		},
		{
			name:   "Status 401 Expired",
			status: http.StatusUnauthorized,
			want:   engine.ClassExpired,
		},
		{
			name:   "Status 403 Expired",
			status: http.StatusForbidden,
			want:   engine.ClassExpired,
		},
		{
			name:   "Status 404 Fatal",
			status: http.StatusNotFound,
			want:   engine.ClassFatal,
		},
		{
			name:   "Status 500 Transient",
			status: http.StatusInternalServerError,
			want:   engine.ClassTransient,
		},
		{
			name: "ENOSPC Fatal",
			err:  syscall.ENOSPC,
			want: engine.ClassFatal,
		},
		{
			name: "DiskFull DownloadError Fatal",
			err:  domain.NewDownloadError(domain.ErrCodeDiskFull, "full", true),
			want: engine.ClassFatal,
		},
		{
			name: "Permission DownloadError Fatal",
			err:  domain.NewDownloadError(domain.ErrCodePermission, "denied", false),
			want: engine.ClassFatal,
		},
		{
			name: "RemoteChanged Integrity",
			err:  domain.NewDownloadError(domain.ErrCodeRemoteChanged, "changed", false),
			want: engine.ClassIntegrity,
		},
		{
			name: "EOF Transient",
			err:  io.EOF,
			want: engine.ClassTransient,
		},
		{
			name: "UnexpectedEOF Transient",
			err:  io.ErrUnexpectedEOF,
			want: engine.ClassTransient,
		},
		{
			name: "Connection Reset Transient",
			err:  errors.New("connection reset by peer"),
			want: engine.ClassTransient,
		},
		{
			name: "Stall Transient",
			err:  errors.New("stream stalled: no bytes received"),
			want: engine.ClassTransient,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := engine.Classify(tt.err, tt.status)
			if got != tt.want {
				t.Errorf("Classify() = %v (%s), want %v (%s)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestRetry_CalculateBackoff(t *testing.T) {
	t.Run("Respects Retry-After", func(t *testing.T) {
		delay := engine.CalculateBackoff(0, 5*time.Second, nil)
		if delay != 5*time.Second {
			t.Errorf("delay = %v, want 5s", delay)
		}
	})

	t.Run("Exponential Increase with Jitter Bounds", func(t *testing.T) {
		baseAttempts := []time.Duration{
			500 * time.Millisecond,
			1000 * time.Millisecond,
			2000 * time.Millisecond,
			4000 * time.Millisecond,
			8000 * time.Millisecond,
			16000 * time.Millisecond,
			30000 * time.Millisecond,
		}

		for attempt, base := range baseAttempts {
			minDelay := engine.CalculateBackoff(attempt, 0, func() float64 { return 0.0 })
			expectedMin := time.Duration(float64(base) * 0.8)
			if minDelay != expectedMin {
				t.Errorf("attempt %d minDelay = %v, want %v", attempt, minDelay, expectedMin)
			}

			midDelay := engine.CalculateBackoff(attempt, 0, func() float64 { return 0.5 })
			if midDelay != base {
				t.Errorf("attempt %d midDelay = %v, want %v", attempt, midDelay, base)
			}

			maxDelay := engine.CalculateBackoff(attempt, 0, func() float64 { return 1.0 })
			expectedMax := time.Duration(float64(base) * 1.2)
			if maxDelay != expectedMax {
				t.Errorf("attempt %d maxDelay = %v, want %v", attempt, maxDelay, expectedMax)
			}
		}
	})
}

func TestRetry_Sleep_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := engine.Sleep(ctx, nil, 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}
