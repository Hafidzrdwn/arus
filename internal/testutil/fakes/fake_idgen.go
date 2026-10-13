package fakes

import (
	"arus/internal/app/ports"
	"arus/internal/domain"
	"fmt"
	"sync"
)

var _ ports.IDGenerator = (*SeqIDGen)(nil)

// SeqIDGen provides monotonic, predictable DownloadIDs for deterministic testing.
type SeqIDGen struct {
	mu     sync.Mutex
	prefix string
	seq    int64
}

// NewSeqIDGen creates a sequential ID generator with an optional prefix.
// If prefix is empty, "01JTESTID" is used.
func NewSeqIDGen(prefix string) *SeqIDGen {
	if prefix == "" {
		prefix = "01JTESTID"
	}
	return &SeqIDGen{
		prefix: prefix,
	}
}

// New generates the next sequential domain.DownloadID.
func (g *SeqIDGen) New() domain.DownloadID {
	g.mu.Lock()
	g.seq++
	val := g.seq
	g.mu.Unlock()

	return domain.DownloadID(fmt.Sprintf("%s%017d", g.prefix, val))
}
