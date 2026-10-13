package testserver

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// DeterministicByte returns a pseudorandom byte calculated from seed and offset.
func DeterministicByte(seed, offset int64) byte {
	x := uint64(seed) ^ (uint64(offset) * 0x9E3779B97F4A7C15)
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return byte(x)
}

// FillDeterministic fills buf with deterministic pseudorandom bytes starting at offset.
func FillDeterministic(buf []byte, seed, offset int64) {
	for i := range buf {
		buf[i] = DeterministicByte(seed, offset+int64(i))
	}
}

// ExpectedSHA256 returns the hex-encoded SHA-256 digest of size bytes generated with seed.
func ExpectedSHA256(seed, size int64) string {
	h := sha256.New()
	const chunkSize = 64 * 1024
	buf := make([]byte, chunkSize)

	var curr int64
	for curr < size {
		n := int64(chunkSize)
		if size-curr < n {
			n = size - curr
		}
		FillDeterministic(buf[:n], seed, curr)
		h.Write(buf[:n])
		curr += n
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Verify reads up to size bytes from r and validates that every byte matches
// the deterministic generator for the given seed, returning an error on mismatch or truncation.
func Verify(r io.Reader, seed, size int64) error {
	const chunkSize = 64 * 1024
	buf := make([]byte, chunkSize)
	expBuf := make([]byte, chunkSize)

	var totalRead int64
	for totalRead < size {
		needed := int64(chunkSize)
		if size-totalRead < needed {
			needed = size - totalRead
		}

		n, err := io.ReadFull(r, buf[:needed])
		if n > 0 {
			FillDeterministic(expBuf[:n], seed, totalRead)
			for i := 0; i < n; i++ {
				if buf[i] != expBuf[i] {
					return fmt.Errorf("byte mismatch at offset %d: got 0x%02X, expected 0x%02X",
						totalRead+int64(i), buf[i], expBuf[i])
				}
			}
			totalRead += int64(n)
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if totalRead < size {
					return fmt.Errorf("stream truncated: read %d bytes, expected %d", totalRead, size)
				}
				break
			}
			return fmt.Errorf("read error at offset %d: %w", totalRead, err)
		}
	}

	extra := make([]byte, 1)
	if n, _ := r.Read(extra); n > 0 {
		return fmt.Errorf("stream contained unexpected extra bytes beyond size %d", size)
	}

	return nil
}
