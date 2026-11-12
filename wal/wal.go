// Package wal implements the reference segmented write-ahead log (RFC-0001
// §6.3): versioned records, lengths, checksums, batch boundaries, and sequence
// numbers over a file or raw block-device substrate. Each batch is one atomic
// record; recovery exposes the previous complete batch or the next complete
// batch, never a partially applied suffix replacement.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	raft "github.com/sufuree/raft"
)

const (
	superMagic  = 0x5257414c // "RWAL"
	recordMagic = 0x52424154 // "RBAT"
	// formatVersion 3 persists SegmentSize in the superblock so recovery uses
	// the exact grid the store was written with. Earlier versions did not
	// persist it; reopening them with a different SegmentSize would silently
	// skip live records, so they are rejected with an explicit upgrade error
	// rather than reinterpreted (spec §7).
	formatVersion = 3
	superSize     = 4096
	// segSizeOff holds the persisted SegmentSize (8 bytes), clear of the
	// identity/config fields and the checkpoint slots (512, 768).
	segSizeOff = 1024
	headerSize = 17 // magic(4)+len(4)+seq(8)+type(1)
	crcSize    = 4

	typeBatch    = 1
	typeApplied  = 2
	typeSnapData = 3 // streamed snapshot chunk (§6.4)

	// Checkpoint slots live in the superblock's reserved space. Two slots
	// with CRC and monotonic sequence make the checkpoint flip atomic: a torn
	// slot write falls back to the previous valid slot.
	slotAOff  = 512
	slotBOff  = 768
	slotSize  = 28         // magic(4)+seq(8)+checkpoint(8)+crc(4)
	slotMagic = 0x52434b50 // "RCKP"
)

// ErrCorrupt reports corruption that requires recovery intervention (§6.3).
var ErrCorrupt = errors.New("wal: corruption requires recovery intervention")

// Options configures a WAL.
type Options struct {
	SegmentSize   int64
	MaxBatchBytes int
	ClusterID     uint64
	MemberID      uint64
	Generation    uint64
	InitialConfig raft.Configuration
	// MaxSnapshotBytes bounds a reassembled streamed snapshot (§6.4, §8).
	MaxSnapshotBytes int
	// MaxDeviceBytes caps total device growth for growable substrates; zero
	// means the substrate's own size governs (fixed for raw block devices).
	MaxDeviceBytes int64
	// MinReclaimBytes is the dead-prefix threshold before compaction runs.
	MinReclaimBytes int64
}

func (o *Options) setDefaults() {
	if o.SegmentSize <= 0 {
		o.SegmentSize = 1 << 20
	}
	if o.MaxBatchBytes <= 0 {
		o.MaxBatchBytes = 8 << 20
	}
	if o.SegmentSize < superSize {
		o.SegmentSize = superSize
	}
	// A batch must fit a segment: the record check rejects any record larger
	// than SegmentSize, so a MaxBatchBytes above SegmentSize would let the
	// driver/leader build a batch (e.g. a coalesced catch-up append) that the
	// WAL then rejects, terminally failing the node (§6.2). Keep the segment at
	// least as large as the largest allowed batch (§8 limit chain).
	if o.SegmentSize < int64(o.MaxBatchBytes) {
		o.SegmentSize = int64(o.MaxBatchBytes)
	}
	if o.MaxSnapshotBytes <= 0 {
		o.MaxSnapshotBytes = 64 << 20
	}
	if o.MinReclaimBytes <= 0 {
		o.MinReclaimBytes = 1 << 20
	}
}

// WAL is a durable raft.Storage over a Device.
type WAL struct {
	mu   sync.Mutex
	dev  Device
	opts Options

	hs      raft.HardState
	snap    raft.SnapshotMeta
	ents    []raft.Entry
	applied raft.Index

	writeOff   int64
	checkpoint int64
	seq        uint64
	slotSeq    uint64
	slotFlip   bool
	closed     bool

	// snapStage holds streamed snapshot chunks for the snapshot currently
	// being published; a manifest references them (§6.4).
	snapStage map[raft.Index][]byte
}

// Open recovers or initializes a WAL on dev. A fresh device is initialized only
// when explicitly allowed by a matching identity; corruption stops recovery.
func Open(dev Device, opts Options) (*WAL, error) {
	opts.setDefaults()
	w := &WAL{dev: dev, opts: opts}
	if err := w.recover(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) lastIndex() raft.Index {
	if len(w.ents) > 0 {
		return w.ents[len(w.ents)-1].Index
	}
	return w.snap.Index
}

// recover reads the superblock and replays valid batch records.
func (w *WAL) recover() error {
	size, err := w.dev.Size()
	if err != nil {
		return err
	}
	sb := make([]byte, superSize)
	if size >= superSize {
		if _, err := w.dev.ReadAt(sb, 0); err != nil && !errors.Is(err, errEOF) {
			return err
		}
	}
	fresh := isZero(sb)
	if fresh {
		return w.initSuperblock()
	}
	if binary.BigEndian.Uint32(sb[0:4]) != superMagic {
		return fmt.Errorf("%w: bad superblock magic", ErrCorrupt)
	}
	ver := binary.BigEndian.Uint32(sb[4:8])
	if ver != formatVersion {
		return fmt.Errorf("%w: unsupported version %d (format upgrade required)", ErrCorrupt, ver)
	}
	cluster := binary.BigEndian.Uint64(sb[8:16])
	member := binary.BigEndian.Uint64(sb[16:24])
	gen := binary.BigEndian.Uint64(sb[24:32])
	if w.opts.ClusterID != 0 && cluster != w.opts.ClusterID {
		return fmt.Errorf("%w: cluster identity mismatch", ErrCorrupt)
	}
	if w.opts.MemberID != 0 && member != w.opts.MemberID {
		return fmt.Errorf("%w: member identity mismatch", ErrCorrupt)
	}
	if w.opts.Generation != 0 && gen < w.opts.Generation {
		return fmt.Errorf("%w: storage generation regressed", ErrCorrupt)
	}
	cfgLen := binary.BigEndian.Uint32(sb[32:36])
	// The config region must end before checkpoint slot A (512); the slots and
	// the persisted grid are written independently and must not overlap it.
	if int(cfgLen) > slotAOff-36 {
		return fmt.Errorf("%w: bad config length", ErrCorrupt)
	}
	cfg, err := raft.DecodeConfig(sb[36 : 36+cfgLen])
	if err != nil {
		return fmt.Errorf("%w: bad initial config", ErrCorrupt)
	}
	w.opts.InitialConfig = cfg
	w.opts.ClusterID = cluster
	w.opts.MemberID = member
	w.opts.Generation = gen
	// The persisted segment grid is authoritative: recovery must scan with the
	// exact SegmentSize the store was written with, not the caller's option
	// (spec §7 — silent reinterpretation is forbidden).
	storedSeg := int64(binary.BigEndian.Uint64(sb[segSizeOff : segSizeOff+8]))
	if storedSeg < superSize {
		return fmt.Errorf("%w: bad persisted segment size %d", ErrCorrupt, storedSeg)
	}
	w.opts.SegmentSize = storedSeg
	// The persisted grid is authoritative; a batch must still fit a segment, so
	// clamp the caller's batch limit to it (§8 limit chain). A driver configured
	// with a larger batch fails stop on write rather than silently losing data.
	if int64(w.opts.MaxBatchBytes) > storedSeg {
		w.opts.MaxBatchBytes = int(storedSeg)
	}

	// Recovery starts at the published checkpoint; earlier records are dead
	// space reclaimed by compaction (§6.3).
	cp, slotSeq, slotB, err := readCheckpoint(sb, size)
	if err != nil {
		return err
	}
	w.checkpoint = cp
	w.slotSeq = slotSeq
	// Alternate slots so a flip never overwrites the newest valid checkpoint.
	w.slotFlip = !slotB

	// Scan records. writeOff tracks the end of the last valid record; the
	// scan cursor may skip zero padding beyond it (fixed regions are always
	// zero-padded to their constant size).
	off := cp
	lastEnd := cp
	lastSeq := uint64(0)
	for off < size {
		// Skip zero padding to the next segment boundary.
		var magic [4]byte
		if _, err := w.dev.ReadAt(magic[:], off); err != nil {
			break
		}
		if binary.BigEndian.Uint32(magic[:]) == 0 {
			off = w.nextSegment(off)
			continue
		}
		if binary.BigEndian.Uint32(magic[:]) != recordMagic {
			return fmt.Errorf("%w: bad record magic at %d", ErrCorrupt, off)
		}
		rec, n, err := w.readRecord(off, size)
		if err != nil {
			// Torn final record: disposable only if nothing live follows.
			if off+int64(n) < size && w.hasValidRecordAfter(off+int64(n), size, lastSeq) {
				return fmt.Errorf("%w: torn record followed by valid data", ErrCorrupt)
			}
			break
		}
		if rec.seq <= lastSeq && lastSeq != 0 {
			// Stale tail: a compaction copied the live region forward, leaving
			// older records behind. A valid CRC covers the sequence, so a
			// regression under valid checksums marks dead space, not corruption.
			break
		}
		lastSeq = rec.seq
		if err := w.applyRecord(rec); err != nil {
			return err
		}
		off += int64(n)
		lastEnd = off
	}
	w.writeOff = lastEnd
	w.seq = lastSeq
	// Interrupted snapshot staging never becomes a snapshot (§6.4): chunks
	// without a manifest are orphans and are discarded.
	w.snapStage = nil
	// Dispose of any torn tail (no-op on fixed raw-block substrates).
	if err := w.dev.Truncate(lastEnd); err != nil {
		return err
	}
	if err := w.dev.Sync(); err != nil {
		return err
	}
	// Commit metadata must never reference missing entries (§6.1).
	if w.hs.Commit > w.lastIndex() {
		return fmt.Errorf("%w: commit %d beyond recoverable log %d", ErrCorrupt, w.hs.Commit, w.lastIndex())
	}
	return nil
}

func (w *WAL) nextSegment(off int64) int64 {
	rel := off - superSize
	return superSize + (rel/w.opts.SegmentSize+1)*w.opts.SegmentSize
}

// segmentStart rounds off down to its segment boundary. Checkpoints must be
// segment-aligned: compaction copies the live region to the region start,
// and internal padding only stays consistent with the scan grid when the
// copy preserves alignment (§6.3).
func (w *WAL) segmentStart(off int64) int64 {
	rel := off - superSize
	return superSize + (rel/w.opts.SegmentSize)*w.opts.SegmentSize
}

func (w *WAL) hasValidRecordAfter(off, size int64, lastSeq uint64) bool {
	for off < size {
		var magic [4]byte
		if _, err := w.dev.ReadAt(magic[:], off); err != nil {
			return false
		}
		m := binary.BigEndian.Uint32(magic[:])
		if m == 0 {
			off = w.nextSegment(off)
			continue
		}
		if m != recordMagic {
			return false
		}
		rec, _, err := w.readRecord(off, size)
		if err != nil {
			return false
		}
		// Stale-tail records are dead space, not live data.
		return rec.seq > lastSeq || lastSeq == 0
	}
	return false
}

type record struct {
	seq     uint64
	typ     byte
	payload []byte
}

func (w *WAL) readRecord(off, size int64) (record, int, error) {
	if off+headerSize > size {
		return record{}, 0, errEOF
	}
	hdr := make([]byte, headerSize)
	if _, err := w.dev.ReadAt(hdr, off); err != nil {
		return record{}, 0, err
	}
	plen := binary.BigEndian.Uint32(hdr[4:8])
	seq := binary.BigEndian.Uint64(hdr[8:16])
	typ := hdr[16]
	total := headerSize + int(plen) + crcSize
	if off+int64(total) > size {
		return record{}, total, errEOF
	}
	body := make([]byte, int(plen)+crcSize)
	if _, err := w.dev.ReadAt(body, off+headerSize); err != nil {
		return record{}, total, err
	}
	payload := body[:plen]
	storedCRC := binary.BigEndian.Uint32(body[plen:])
	buf := append([]byte{}, hdr[8:16]...)
	buf = append(buf, typ)
	buf = append(buf, payload...)
	if crc32.ChecksumIEEE(buf) != storedCRC {
		return record{}, total, ErrCorrupt
	}
	return record{seq: seq, typ: typ, payload: payload}, total, nil
}

func (w *WAL) applyRecord(r record) error {
	switch r.typ {
	case typeBatch:
		b, err := decodeBatch(r.payload)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		return w.applyBatch(b)
	case typeApplied:
		if len(r.payload) < 8 {
			return fmt.Errorf("%w: short applied record", ErrCorrupt)
		}
		idx := raft.Index(binary.BigEndian.Uint64(r.payload))
		if idx > w.applied {
			w.applied = idx
		}
	case typeSnapData:
		return w.stageSnapChunk(r.payload)
	default:
		return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, r.typ)
	}
	return nil
}

func (w *WAL) applyBatch(b raft.PersistBatch) error {
	if b.Snapshot != nil {
		w.snap = *b.Snapshot
		w.snap.Config = b.Snapshot.Config.Clone()
		if b.SnapshotStream != nil {
			// The manifest references streamed chunks; they must be complete
			// and intact, else recovery requires intervention (§6.3, §6.4).
			data, ok := w.snapStage[b.Snapshot.Index]
			if !ok || uint64(len(data)) != b.SnapshotStream.Size ||
				crc32.ChecksumIEEE(data) != b.SnapshotStream.CRC {
				return fmt.Errorf("%w: snapshot manifest references missing or corrupt data at %d", ErrCorrupt, b.Snapshot.Index)
			}
			w.snap.Data = data
			delete(w.snapStage, b.Snapshot.Index)
		}
		// Retain the suffix above the snapshot: committed entries above the
		// snapshot index must survive publication (§6.1, §6.4).
		var keep []raft.Entry
		for _, e := range w.ents {
			if e.Index > b.Snapshot.Index {
				keep = append(keep, e)
			}
		}
		w.ents = keep
	}
	if b.TruncateAt > 0 {
		var keep []raft.Entry
		for _, e := range w.ents {
			if e.Index < b.TruncateAt {
				keep = append(keep, e)
			}
		}
		w.ents = keep
	}
	for _, e := range b.Entries {
		w.appendEntry(e)
	}
	if b.HardState != nil {
		if b.HardState.Term != 0 || b.HardState.Vote != 0 {
			w.hs.Term = b.HardState.Term
			w.hs.Vote = b.HardState.Vote
		}
		if b.HardState.Commit > w.hs.Commit {
			w.hs.Commit = b.HardState.Commit
		}
	}
	if b.Commit != nil && *b.Commit > w.hs.Commit {
		w.hs.Commit = *b.Commit
	}
	return nil
}

func (w *WAL) appendEntry(e raft.Entry) {
	if e.Index <= w.snap.Index {
		return
	}
	if len(w.ents) > 0 {
		last := w.ents[len(w.ents)-1].Index
		if e.Index <= last {
			off := e.Index - w.snap.Index - 1
			if off < raft.Index(len(w.ents)) {
				w.ents[off] = e
			}
			return
		}
	}
	w.ents = append(w.ents, e)
}

func (w *WAL) initSuperblock() error {
	sb := make([]byte, superSize)
	binary.BigEndian.PutUint32(sb[0:4], superMagic)
	binary.BigEndian.PutUint32(sb[4:8], formatVersion)
	binary.BigEndian.PutUint64(sb[8:16], w.opts.ClusterID)
	binary.BigEndian.PutUint64(sb[16:24], w.opts.MemberID)
	binary.BigEndian.PutUint64(sb[24:32], w.opts.Generation)
	cfg := raft.EncodeConfig(w.opts.InitialConfig)
	if len(cfg) > slotAOff-36 {
		return fmt.Errorf("wal: initial config %d bytes exceeds superblock budget", len(cfg))
	}
	binary.BigEndian.PutUint32(sb[32:36], uint32(len(cfg)))
	copy(sb[36:], cfg)
	// Persist the segment grid so recovery never reinterprets it (spec §7).
	binary.BigEndian.PutUint64(sb[segSizeOff:segSizeOff+8], uint64(w.opts.SegmentSize))
	if _, err := w.dev.WriteAt(sb, 0); err != nil {
		return err
	}
	if err := w.dev.Sync(); err != nil {
		return err
	}
	w.writeOff = superSize
	w.seq = 0
	return nil
}

// appendRecord writes one checksummed record at the write cursor, advancing
// it. The caller synchronizes; a successful Sync is the durable receipt.
// Caller holds w.mu.
func (w *WAL) appendRecord(typ byte, payload []byte) error {
	recSize := headerSize + len(payload) + crcSize
	if recSize > w.opts.MaxBatchBytes || int64(recSize) > w.opts.SegmentSize {
		return fmt.Errorf("wal: record type %d %d bytes exceeds budget (batch %d, segment %d)", typ, recSize, w.opts.MaxBatchBytes, w.opts.SegmentSize)
	}
	off := w.writeOff
	rel := off - superSize
	if rel%w.opts.SegmentSize+int64(recSize) > w.opts.SegmentSize {
		off = w.nextSegment(off)
	}
	if err := w.spaceCheck(off, recSize); err != nil {
		return err
	}
	w.seq++
	buf := make([]byte, recSize)
	binary.BigEndian.PutUint32(buf[0:4], recordMagic)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint64(buf[8:16], w.seq)
	buf[16] = typ
	copy(buf[headerSize:], payload)
	crcBuf := append([]byte{}, buf[8:16]...)
	crcBuf = append(crcBuf, typ)
	crcBuf = append(crcBuf, payload...)
	binary.BigEndian.PutUint32(buf[headerSize+len(payload):], crc32.ChecksumIEEE(crcBuf))
	if _, err := w.dev.WriteAt(buf, off); err != nil {
		return err
	}
	w.writeOff = off + int64(recSize)
	return nil
}

// Save implements raft.Storage: one atomic, synchronized batch record.
// Snapshot publications are self-contained (hard state and applied watermark
// included) so the recovery checkpoint can discard all older records (§6.1,
// §6.3, §6.4). Large snapshots stream as chunk records before the manifest.
func (w *WAL) Save(b raft.PersistBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("wal: closed")
	}
	if b.Snapshot != nil {
		// Carry the current hard state into the publication record: older
		// term/vote records become reclaimable once the checkpoint advances.
		hs := w.hs
		if b.HardState != nil {
			if b.HardState.Term != 0 || b.HardState.Vote != 0 {
				hs.Term, hs.Vote = b.HardState.Term, b.HardState.Vote
			}
			if b.HardState.Commit > hs.Commit {
				hs.Commit = b.HardState.Commit
			}
		}
		if b.Commit != nil && *b.Commit > hs.Commit {
			hs.Commit = *b.Commit
		}
		b.HardState = &hs
	}
	manifest := b
	var postManifest []raft.Entry
	if b.Snapshot != nil {
		// The publication must be self-contained above the snapshot index:
		// the checkpoint discards earlier records, so the suffix entries ride
		// in the manifest batch (§6.1, §6.4). A suffix too large for one batch
		// follows as its own record before the checkpoint flips.
		var suffix []raft.Entry
		for _, e := range w.ents {
			if e.Index > b.Snapshot.Index {
				suffix = append(suffix, e)
			}
		}
		suffix = append(suffix, b.Entries...)
		sz := 0
		for _, e := range suffix {
			sz += 21 + len(e.Data)
		}
		if sz <= w.opts.MaxBatchBytes-1024 {
			manifest.Entries = suffix
		} else {
			postManifest = suffix
		}
	}
	stream := b.Snapshot != nil && len(b.Snapshot.Data) > w.opts.MaxBatchBytes/2
	var streamStart int64
	if stream {
		if len(b.Snapshot.Data) > w.opts.MaxSnapshotBytes {
			return fmt.Errorf("%w: snapshot %d exceeds cap %d", ErrNoSpace, len(b.Snapshot.Data), w.opts.MaxSnapshotBytes)
		}
		var err error
		streamStart, err = w.writeSnapChunks(b.Snapshot.Index, b.Snapshot.Data)
		if err != nil {
			return err
		}
		snapCopy := *b.Snapshot
		snapCopy.Data = nil
		manifest.Snapshot = &snapCopy
		manifest.SnapshotStream = &raft.SnapshotStreamRef{
			Size: uint64(len(b.Snapshot.Data)),
			CRC:  crc32.ChecksumIEEE(b.Snapshot.Data),
		}
	}
	payload := encodeBatch(manifest)
	if err := w.appendRecord(typeBatch, payload); err != nil {
		return err
	}
	if err := w.dev.Sync(); err != nil {
		return err
	}
	if len(postManifest) > 0 {
		if err := w.appendRecord(typeBatch, encodeBatch(raft.PersistBatch{Entries: postManifest})); err != nil {
			return err
		}
		if err := w.dev.Sync(); err != nil {
			return err
		}
	}
	manifestOff := w.writeOff - int64(headerSize+len(payload)+crcSize)
	// The checkpoint must cover the manifest's dependencies: for streamed
	// publications the chunks precede the manifest (§6.4). It is rounded down
	// to a segment boundary so compaction preserves padding alignment; the
	// extra records replayed below the snapshot are superseded by it.
	checkpointOff := manifestOff
	if stream {
		checkpointOff = streamStart
	}
	checkpointOff = w.segmentStart(checkpointOff)
	if err := w.applyBatch(b); err != nil {
		return err
	}
	if b.Snapshot != nil {
		// Persist the applied watermark so the checkpointed region is
		// self-contained for recovery (§6.1).
		if w.applied > 0 {
			payload := make([]byte, 8)
			binary.BigEndian.PutUint64(payload, uint64(w.applied))
			if err := w.appendRecord(typeApplied, payload); err != nil {
				return err
			}
			if err := w.dev.Sync(); err != nil {
				return err
			}
		}
		if err := w.flipCheckpoint(checkpointOff); err != nil {
			return err
		}
		if err := w.maybeCompact(); err != nil {
			return err
		}
	}
	return nil
}

// MarkApplied implements raft.Storage.
func (w *WAL) MarkApplied(idx raft.Index) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("wal: closed")
	}
	if idx <= w.applied {
		return nil
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(idx))
	if err := w.appendRecord(typeApplied, payload); err != nil {
		return err
	}
	if err := w.dev.Sync(); err != nil {
		return err
	}
	w.applied = idx
	return nil
}

// Load implements raft.Storage.
func (w *WAL) Load() (raft.InitState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	cfg := w.snap.Config.Clone()
	if len(cfg.Voters) == 0 {
		cfg = w.opts.InitialConfig.Clone()
	}
	return raft.InitState{
		HardState:    w.hs,
		Snapshot:     w.snap,
		Entries:      append([]raft.Entry(nil), w.ents...),
		Config:       cfg,
		AppliedIndex: w.applied,
		DurableIndex: w.lastIndex(),
	}, nil
}

// ReadEntries implements raft.Storage.
func (w *WAL) ReadEntries(lo, hi raft.Index) ([]raft.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if lo <= w.snap.Index {
		return nil, raft.ErrCompacted
	}
	if hi > w.lastIndex()+1 {
		return nil, raft.ErrUnavailable
	}
	if lo >= hi {
		return nil, nil
	}
	out := make([]raft.Entry, 0, hi-lo)
	for _, e := range w.ents {
		if e.Index >= lo && e.Index < hi {
			out = append(out, e)
		}
	}
	return out, nil
}

// ReadSnapshot implements raft.Storage.
func (w *WAL) ReadSnapshot() (raft.SnapshotMeta, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snap, nil
}

// Close implements raft.Storage.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func isZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// HasState reports whether dev already holds a WAL superblock. It is used to
// distinguish explicit creation from ordinary restart (§9).
func HasState(dev Device) (bool, error) {
	size, err := dev.Size()
	if err != nil {
		return false, err
	}
	if size < superSize {
		return false, nil
	}
	sb := make([]byte, 4)
	if _, err := dev.ReadAt(sb, 0); err != nil {
		return false, err
	}
	return binary.BigEndian.Uint32(sb) == superMagic, nil
}

// ReadIdentity reads the persisted cluster/member/storage generation from a
// device's superblock without a full recovery.
func ReadIdentity(dev Device) (cluster, member, generation uint64, err error) {
	size, err := dev.Size()
	if err != nil {
		return 0, 0, 0, err
	}
	if size < superSize {
		return 0, 0, 0, nil
	}
	sb := make([]byte, 32)
	if _, err := dev.ReadAt(sb, 0); err != nil {
		return 0, 0, 0, err
	}
	if binary.BigEndian.Uint32(sb[0:4]) != superMagic {
		return 0, 0, 0, nil
	}
	return binary.BigEndian.Uint64(sb[8:16]),
		binary.BigEndian.Uint64(sb[16:24]),
		binary.BigEndian.Uint64(sb[24:32]), nil
}

// AdvanceGeneration atomically raises the persisted storage generation to newGen
// for an explicit disaster-recovery incarnation (§9). It refuses to regress.
func AdvanceGeneration(dev Device, newGen uint64) error {
	size, err := dev.Size()
	if err != nil {
		return err
	}
	if size < superSize {
		return fmt.Errorf("%w: no superblock", ErrCorrupt)
	}
	sb := make([]byte, superSize)
	if _, err := dev.ReadAt(sb, 0); err != nil {
		return err
	}
	if binary.BigEndian.Uint32(sb[0:4]) != superMagic {
		return fmt.Errorf("%w: bad superblock", ErrCorrupt)
	}
	cur := binary.BigEndian.Uint64(sb[24:32])
	if newGen <= cur {
		return fmt.Errorf("wal: generation %d does not advance %d", newGen, cur)
	}
	binary.BigEndian.PutUint64(sb[24:32], newGen)
	if _, err := dev.WriteAt(sb, 0); err != nil {
		return err
	}
	return dev.Sync()
}
