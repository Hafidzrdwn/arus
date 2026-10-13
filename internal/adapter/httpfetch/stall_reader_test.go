package httpfetch_test

import (
	"arus/internal/adapter/httpfetch"
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

type blockReader struct {
	mu      sync.Mutex
	unblock chan struct{}
	closed  bool
}

func newBlockReader() *blockReader {
	return &blockReader{
		unblock: make(chan struct{}),
	}
}

func (b *blockReader) Read(_ []byte) (int, error) {
	<-b.unblock
	return 0, io.EOF
}

func (b *blockReader) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		close(b.unblock)
	}
	return nil
}

func TestStallReader_NormalRead(t *testing.T) {
	payload := []byte("hello world, stream without stall")
	rc := io.NopCloser(bytes.NewReader(payload))

	sr := httpfetch.NewStallReader(rc, 200*time.Millisecond)
	defer func() { _ = sr.Close() }()

	buf := make([]byte, len(payload))
	n, err := io.ReadFull(sr, buf)
	if err != nil {
		t.Fatalf("ReadFull error: %v", err)
	}
	if n != len(payload) {
		t.Errorf("read bytes = %d, want %d", n, len(payload))
	}
	if !bytes.Equal(buf, payload) {
		t.Errorf("got %q, want %q", buf, payload)
	}
}

func TestStallReader_StallTimeoutTriggered(t *testing.T) {
	br := newBlockReader()
	timeout := 50 * time.Millisecond
	sr := httpfetch.NewStallReader(br, timeout)
	defer func() { _ = sr.Close() }()

	start := time.Now()
	buf := make([]byte, 10)
	_, err := sr.Read(buf)
	elapsed := time.Since(start)

	if !errors.Is(err, httpfetch.ErrStallTimeout) {
		t.Fatalf("expected ErrStallTimeout, got: %v", err)
	}

	if elapsed < 40*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Errorf("elapsed = %v, expected ~%v", elapsed, timeout)
	}
}

func TestStallReader_CloseStopsTimer(t *testing.T) {
	br := newBlockReader()
	sr := httpfetch.NewStallReader(br, 100*time.Millisecond)

	if err := sr.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	buf := make([]byte, 10)
	_, err := sr.Read(buf)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("expected io.ErrClosedPipe after close, got: %v", err)
	}
}
