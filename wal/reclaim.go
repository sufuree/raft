package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	raft "github.com/sufuree/raft"
)

// ErrNoSpace reports that a write or admission would exhaust the device or
// its recovery reserve (§8). An actual failed write/sync is terminal (§6.2).
var ErrNoSpace = errors.New("wal: insufficient space")

// readCheckpoint returns the newest valid checkpoint from the superblock's
// dual slots. A torn slot write fails its CRC and falls back to the other
// slot; no valid slots means recovery from the region start (v1 compatible).
func readCheckpoint(sb []byte, size int64) (cp int64, seq uint64, slotB bool, err error) {
	cp = superSize
	sawValid := false
	for _, off := range []int{slotAOff, slotBOff} {
		s := sb[off : off+slotSize]
		if binary.BigEndian.Uint32(s[0:4]) != slotMagic {
			continue
		}
		if crc32.ChecksumIEEE(s[0:24]) != binary.BigEndian.Uint32(s[24:28]) {
			continue
		}
		sawValid = true
		sq := binary.BigEndian.Uint64(s[4:12])
		c := int64(binary.BigEndian.Uint64(s[12:20]))
		// A superseded slot may point past a compacted EOF; skip it. The
		// protocol flips the checkpoint before truncating, so the newest
		// in-range slot is always the authoritative one.
		if c < superSize || c > size {
			continue
		}
		if sq >= seq {
			cp, seq, slotB = c, sq, off == slotBOff
		}
	}
	if sawValid && cp == superSize && seq == 0 {
		// Valid slots exist but none is in range: the substrate shrank below
		// every published checkpoint, which the protocol never does.
		return 0, 0, false, fmt.Errorf("%w: no in-range checkpoint", ErrCorrupt)
	}
	return cp, seq, slotB, nil
}

// flipCheckpoint atomically publishes a new recovery checkpoint. The slot
// sequence is independent of record sequences so consecutive flips (snapshot
// publication, then compaction) remain ordered. Caller holds w.mu.
func (w *WAL) flipCheckpoint(cp int64) error {
	w.slotSeq++
	var slot [slotSize]byte
	binary.BigEndian.PutUint32(slot[0:4], slotMagic)
	binary.BigEndian.PutUint64(slot[4:12], w.slotSeq)
	binary.BigEndian.PutUint64(slot[12:20], uint64(cp))
	binary.BigEndian.PutUint32(slot[24:28], crc32.ChecksumIEEE(slot[0:24]))
	off := slotAOff
	if w.slotFlip {
		off = slotBOff
	}
	w.slotFlip = !w.slotFlip
	if _, err := w.dev.WriteAt(slot[:], int64(off)); err != nil {
		return err
	}
	if err := w.dev.Sync(); err != nil {
		return err
	}
	w.checkpoint = cp
	return nil
}

// maybeCompact reclaims the dead prefix in place once it is at least as large
// as the live region (§6.3 reclamation). Crash safety at every boundary:
//
//   - The copy destination [superSize, superSize+live) lies entirely inside
//     the dead prefix (live <= dead), so the live source is untouched until
//     the checkpoint flips.
//   - A crash before the flip recovers from the old checkpoint (source intact).
//   - A crash after the flip recovers from the copy; the stale tail is recognized
//     by sequence regression under valid CRCs and ignored.
//   - Truncation is optional shrinkage; raw-block substrates no-op it.
//
// Caller holds w.mu.
func (w *WAL) maybeCompact() error {
	dead := w.checkpoint - superSize
	live := w.writeOff - w.checkpoint
	if dead < w.opts.MinReclaimBytes || dead < live || live <= 0 {
		return nil
	}
	buf := make([]byte, live)
	if _, err := w.dev.ReadAt(buf, w.checkpoint); err != nil {
		return err
	}
	if _, err := w.dev.WriteAt(buf, superSize); err != nil {
		return err
	}
	if err := w.dev.Sync(); err != nil {
		return err
	}
	if err := w.flipCheckpoint(superSize); err != nil {
		return err
	}
	w.writeOff = superSize + live
	if err := w.dev.Truncate(w.writeOff); err != nil {
		return err
	}
	return w.dev.Sync()
}

// Preflight reports whether admitting additionalBytes would exhaust the
// device budget while preserving the recovery reserve (§8): space for one
// maximum batch (hard-state updates and snapshot publication) must remain.
// Admission preflight rejects proposals; it never substitutes for the
// terminal failure of an actual write (§6.2).
func (w *WAL) Preflight(additionalBytes int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.opts.MaxDeviceBytes <= 0 {
		return nil // growable substrate: the filesystem's own ENOSPC governs
	}
	need := w.writeOff + int64(additionalBytes) + int64(w.opts.MaxBatchBytes)
	if need > w.opts.MaxDeviceBytes {
		return fmt.Errorf("%w: need %d, budget %d", ErrNoSpace, need, w.opts.MaxDeviceBytes)
	}
	return nil
}

// spaceCheck refuses a write that would exceed the configured device budget.
// Caller holds w.mu.
func (w *WAL) spaceCheck(off int64, recSize int) error {
	if w.opts.MaxDeviceBytes > 0 && off+int64(recSize) > w.opts.MaxDeviceBytes {
		return fmt.Errorf("%w: record at %d+%d exceeds budget %d", ErrNoSpace, off, recSize, w.opts.MaxDeviceBytes)
	}
	return nil
}

// writeSnapChunks streams snapshot data as chunk records before the manifest
// (§6.4). Chunks are durable before the manifest references them; interrupted
// staging leaves orphan chunks that recovery ignores. Caller holds w.mu.
func (w *WAL) writeSnapChunks(index raft.Index, data []byte) (int64, error) {
	start := w.writeOff
	lim := w.opts.MaxBatchBytes
	if int64(lim) > w.opts.SegmentSize {
		lim = int(w.opts.SegmentSize)
	}
	chunk := lim - headerSize - crcSize - 16 // index+offset header
	if chunk <= 0 {
		chunk = 1
	}
	first := true
	for off := 0; off < len(data); off += chunk {
		end := off + chunk
		if end > len(data) {
			end = len(data)
		}
		payload := make([]byte, 16+(end-off))
		binary.BigEndian.PutUint64(payload[0:8], uint64(index))
		binary.BigEndian.PutUint64(payload[8:16], uint64(off))
		copy(payload[16:], data[off:end])
		if err := w.appendRecord(typeSnapData, payload); err != nil {
			return 0, err
		}
		if first {
			// The first chunk may have been segment-aligned by appendRecord.
			start = w.writeOff - int64(headerSize+len(payload)+crcSize)
			first = false
		}
	}
	return start, w.dev.Sync()
}

// stageSnapChunk accumulates a recovered chunk. Only the newest publication's
// chunks are kept; a gap discards staging (the manifest will then fail
// recovery, which is correct: a referenced chunk is missing, §6.3).
func (w *WAL) stageSnapChunk(payload []byte) error {
	if len(payload) < 16 {
		return fmt.Errorf("%w: short snapshot chunk", ErrCorrupt)
	}
	index := raft.Index(binary.BigEndian.Uint64(payload[0:8]))
	off := binary.BigEndian.Uint64(payload[8:16])
	if w.snapStage == nil {
		w.snapStage = make(map[raft.Index][]byte)
	}
	cur, ok := w.snapStage[index]
	if !ok {
		// A newer publication supersedes any interrupted staging.
		for k := range w.snapStage {
			delete(w.snapStage, k)
		}
	}
	if uint64(len(cur)) != off {
		delete(w.snapStage, index) // gap: discard, never assemble partially
		return nil
	}
	if len(cur)+(len(payload)-16) > w.opts.MaxSnapshotBytes {
		delete(w.snapStage, index)
		return nil
	}
	w.snapStage[index] = append(cur, payload[16:]...)
	return nil
}
