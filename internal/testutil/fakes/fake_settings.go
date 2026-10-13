package fakes

import (
	"arus/internal/app/ports"
	"context"
	"strconv"
	"sync"
)

var _ ports.Settings = (*FakeSettings)(nil)

// FakeSettings is an in-memory, thread-safe implementation of ports.Settings.
type FakeSettings struct {
	mu     sync.RWMutex
	values map[string]string
}

// NewFakeSettings instantiates a new FakeSettings storage.
func NewFakeSettings() *FakeSettings {
	return &FakeSettings{
		values: make(map[string]string),
	}
}

// Get retrieves raw string value for key.
func (s *FakeSettings) Get(_ context.Context, key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.values[key]
	if !ok {
		return "", ports.ErrNotFound
	}
	return val, nil
}

// Set stores raw string setting.
func (s *FakeSettings) Set(_ context.Context, key, val string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.values[key] = val
	return nil
}

// GetInt retrieves key parsed as integer, or defaultVal if not found.
func (s *FakeSettings) GetInt(ctx context.Context, key string, defaultVal int) (int, error) {
	val, err := s.Get(ctx, key)
	if err != nil {
		return defaultVal, nil
	}
	parsed, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal, err
	}
	return parsed, nil
}

// SetInt stores an integer setting under key.
func (s *FakeSettings) SetInt(ctx context.Context, key string, val int) error {
	return s.Set(ctx, key, strconv.Itoa(val))
}

// GetBool retrieves key parsed as bool, or defaultVal if not found.
func (s *FakeSettings) GetBool(ctx context.Context, key string, defaultVal bool) (bool, error) {
	val, err := s.Get(ctx, key)
	if err != nil {
		return defaultVal, nil
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return defaultVal, err
	}
	return parsed, nil
}

// SetBool stores a bool setting under key.
func (s *FakeSettings) SetBool(ctx context.Context, key string, val bool) error {
	return s.Set(ctx, key, strconv.FormatBool(val))
}

// GetString retrieves key, or defaultVal if not found.
func (s *FakeSettings) GetString(ctx context.Context, key string, defaultVal string) (string, error) {
	val, err := s.Get(ctx, key)
	if err != nil {
		return defaultVal, nil
	}
	return val, nil
}
