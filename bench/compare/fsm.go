package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
)

// stateFSM is the shared application state machine for every composition
// (profiles.md measurement rules): a fixed-size slot buffer. Each applied
// normal entry stamps its index into slot (index % cells) with a
// deterministic fill; a snapshot is an 8-byte applied-index header plus the
// whole buffer; restore validates every live slot's index, slot congruence,
// and fill (end-to-end snapshot integrity, RFC §6.4, §11 P6). Command payloads
// are opaque: archive-class profiles stress consensus and storage, not FSM
// compute, and the FSM work is identical across compositions.
//
// The mutex guards worker application against bench-side reads (applied
// index, checksum); it is not part of the measured consensus path.
type stateFSM struct {
	mu      sync.Mutex
	cells   int
	buf     []byte // cells * 16 bytes: 8-byte index + 8-byte fill per slot
	applied uint64
}

const cellBytes = 16

func newStateFSM(stateBytes int) *stateFSM {
	n := stateBytes / cellBytes
	if n < 1 {
		n = 1
	}
	return &stateFSM{cells: n, buf: make([]byte, n*cellBytes)}
}

func (f *stateFSM) applyEntry(index uint64) {
	if index == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	slot := int(index % uint64(f.cells))
	off := slot * cellBytes
	binary.LittleEndian.PutUint64(f.buf[off:], index)
	fill := byte(index)
	for i := off + 8; i < off+cellBytes; i++ {
		f.buf[i] = fill
	}
	if index > f.applied {
		f.applied = index
	}
}

// snapshot returns the header+buffer image. The caller owns the copy.
func (f *stateFSM) snapshot() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]byte, 8+len(f.buf))
	binary.LittleEndian.PutUint64(out, f.applied)
	copy(out[8:], f.buf)
	return out
}

// restore validates and installs a snapshot image. expectIndex, when
// non-zero, is cross-checked against the header (ours/etcd know the manifest
// index; HashiCorp's Restore API does not expose it and passes zero).
func (f *stateFSM) restore(data []byte, expectIndex uint64) error {
	if len(data) != 8+len(f.buf) {
		return fmt.Errorf("snapshot size %d, want %d", len(data), 8+len(f.buf))
	}
	index := binary.LittleEndian.Uint64(data)
	if expectIndex != 0 && index != expectIndex {
		return fmt.Errorf("snapshot header index %d != manifest %d", index, expectIndex)
	}
	for slot := 0; slot < f.cells; slot++ {
		off := 8 + slot*cellBytes
		idx := binary.LittleEndian.Uint64(data[off:])
		if idx == 0 {
			continue
		}
		if idx > index || idx%uint64(f.cells) != uint64(slot) {
			return fmt.Errorf("slot %d: index %d invalid at snapshot %d", slot, idx, index)
		}
		for i := off + 8; i < off+cellBytes; i++ {
			if data[i] != byte(idx) {
				return fmt.Errorf("slot %d: fill mismatch (integrity)", slot)
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	copy(f.buf, data[8:])
	f.applied = index
	return nil
}

// advance bumps the applied watermark for entries that carry no application
// command (no-ops, configuration): identical treatment across compositions.
func (f *stateFSM) advance(index uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index > f.applied {
		f.applied = index
	}
}

// bufferCRC hashes only the application state buffer (not the applied
// watermark): a restart legitimately advances applied via the new-leader
// no-op, but the restored+replayed state contents must be identical (P7).
func (f *stateFSM) bufferCRC() uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return crc32.ChecksumIEEE(f.buf)
}

func (f *stateFSM) appliedIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied
}

// checksum binds the buffer contents and the applied index; two nodes at the
// same applied index with equal checksums have identical state (P6 verify).
func (f *stateFSM) checksum() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(crc32.ChecksumIEEE(f.buf))<<32 | (f.applied & 0xffffffff)
}
