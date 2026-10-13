package fakes

import (
	"arus/internal/app/ports"
	"errors"
	"sync"
)

var _ ports.SecretBox = (*FakeSecretBox)(nil)

// FakeSecretBox provides an in-memory test double for ports.SecretBox.
// It applies a deterministic reversible XOR transformation.
type FakeSecretBox struct {
	mu      sync.Mutex
	failErr error
	key     byte
}

// NewFakeSecretBox instantiates a FakeSecretBox with a default mask key.
func NewFakeSecretBox() *FakeSecretBox {
	return &FakeSecretBox{
		key: 0x5A,
	}
}

// Seal encrypts plain data by XORing bytes with the key.
func (b *FakeSecretBox) Seal(plain []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.failErr != nil {
		return nil, b.failErr
	}
	if plain == nil {
		return nil, nil
	}

	out := make([]byte, len(plain))
	for i, v := range plain {
		out[i] = v ^ b.key
	}
	return out, nil
}

// Open decrypts cipher data by XORing bytes with the key.
func (b *FakeSecretBox) Open(cipher []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.failErr != nil {
		return nil, b.failErr
	}
	if cipher == nil {
		return nil, nil
	}

	out := make([]byte, len(cipher))
	for i, v := range cipher {
		out[i] = v ^ b.key
	}
	return out, nil
}

// SetError injects an error into Seal and Open for testing failure paths.
func (b *FakeSecretBox) SetError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failErr = err
}

// ErrCipherCorrupt is a sentinel error for corrupt test payloads.
var ErrCipherCorrupt = errors.New("cipher payload corrupted")
