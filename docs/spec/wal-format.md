# WAL and manifest format specification, version 3

Status: implemented by `wal/`; required by RFC-0001 §6.3 before durable
implementation acceptance. This document pins the on-disk format, both
substrate bindings, synchronization primitives, recovery rules, and the
upgrade mechanism.

## 1. Device model

A `Device` is a byte space with explicit operations:

- `ReadAt(p, off)`, `WriteAt(p, off)`: positional I/O.
- `Sync()`: the durable receipt. A successful `WriteAt` alone is **not**
  durable. Every publication path ends in `Sync` before acknowledging.
- `Size()`, `Truncate(size)`.

Two bindings are qualified:

| Binding | Size | Truncate | Failure mode |
| --- | --- | --- | --- |
| File (`FileDevice`) | growable | shrinks the file | filesystem ENOSPC |
| Raw block (`FixedRegionDevice`) | constant | **no-op** (logical disposal only) | ENOSPC past region |

Protocol semantics never depend on the binding. On raw block, dead space is
disposed by the recovery protocol (checkpoint + stale-tail rule), never by
resizing. Directory durability applies to the file binding when the adapter
creates/renames/publishes files; the current WAL is a single pre-created file
per store, so publication is record-append + `Sync`, and the superblock is
written in place.

## 2. Layout

```
offset 0                     superblock (4096 bytes)
offset 4096                  record region, segmented
  segment k: [4096 + k*SegmentSize, 4096 + (k+1)*SegmentSize)
```

Records never cross segment boundaries: a record that does not fit in the
remaining segment space is written at the next segment start; the gap is
zero padding. `SegmentSize` default 8 MiB; `MaxBatchBytes` default 8 MiB
(a record must fit both). `SegmentSize` is always raised to at least
`MaxBatchBytes` so any admitted batch fits a segment, and it is **persisted in
the superblock** (§2.1): recovery scans with the exact grid the store was
written with, never the caller's current option.

### 2.1 Superblock (4096 bytes)

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | magic `RWAL` (0x5257414c) |
| 4 | 4 | format version (3; older versions rejected, see §7) |
| 8 | 8 | cluster identity |
| 16 | 8 | member identity |
| 24 | 8 | storage generation |
| 32 | 4 | initial configuration length |
| 36 | n | initial configuration (`raft.EncodeConfig`); n ≤ 988 |
| 512 | 28 | checkpoint slot A |
| 768 | 28 | checkpoint slot B |
| 1024 | 8 | `SegmentSize` (persisted segment grid; authoritative on recovery) |

All integers big-endian. The remaining bytes are reserved (zero).

**Checkpoint slot** (28 bytes): magic `RCKP` (0x52434b50), slot sequence
(u64), checkpoint offset (u64), CRC32-IEEE over the first 24 bytes. Slots
alternate on every flip; the slot sequence is independent of record
sequences and strictly increases per flip. Recovery selects the valid slot
with the highest sequence; a torn slot write fails CRC and falls back to the
other slot. A valid slot pointing past EOF is stale (superseded by a
compaction that truncated) and skipped; if all valid slots are out of range,
recovery requires intervention.

### 2.2 Records

```
[4] magic RBAT (0x52424154)
[4] payload length
[8] sequence (strictly increasing across the live region)
[1] type: 1=batch, 2=applied, 3=snapshot-chunk
[payload]
[4] CRC32-IEEE over sequence||type||payload
```

**Batch payload** (type 1), the atomic unit of §6.1:

```
[1] hard-state present; if 1: [8]term [8]vote [8]commit
[8] truncate-at (0 = none; discards entries >= truncate-at before append)
[4] entry count; per entry: [8]term [8]index [1]type [4]len [len]data
[1] commit present; if 1: [8]commit
[1] snapshot present; if 1:
    [8]index [8]term [4]cfg-len [cfg] [4]data-len [data]
    [1] stream-ref present; if 1: [8]size [4]crc32   (v2 trailer)
```

The v2 stream trailer is absent in v1 payloads; decoders treat end-of-payload
as "no stream reference". Entry count and all length claims are validated
against remaining payload before allocation (§8 decode allocation cap).

**Applied payload** (type 2): `[8] applied index`.

**Snapshot-chunk payload** (type 3): `[8] snapshot index [8] byte offset
[chunk bytes]`. Chunks stream a snapshot larger than `MaxBatchBytes/2`;
each chunk record fits one batch budget. Chunk size = min(MaxBatchBytes,
SegmentSize) − header − CRC − 16.

## 3. Publication protocols

### 3.1 Ordinary batch
Append one batch record at the write cursor (segment-aligned), `Sync`, then
apply to in-memory state. Recovery exposes the previous complete batch or the
next complete batch, never a partial suffix replacement.

### 3.2 Snapshot publication (self-contained)
A snapshot publication must survive checkpointing on its own:

1. The manifest batch carries the current hard state (term/vote/commit) and
   the suffix entries above the snapshot index. A suffix too large for one
   batch follows as its own batch record before the checkpoint flips.
2. Large snapshot data streams as chunk records **before** the manifest;
   the manifest's stream reference records total size and end-to-end CRC32.
3. `Sync` after chunks, after the manifest (and suffix record), and after the
   applied-watermark record.
4. Flip the checkpoint to the segment-aligned start of the publication
   (first chunk for streamed data, else the manifest). Segment alignment
   keeps compaction copies consistent with the scan grid.
5. Compact when eligible (§4).

Interrupted staging (chunks without a durable manifest) is orphaned and
discarded by recovery; it never becomes a snapshot.

### 3.3 Compaction (in-place reclamation)
Eligible when `dead = checkpoint − 4096 ≥ max(MinReclaimBytes, live)` where
`live = writeOff − checkpoint`:

1. Copy `[checkpoint, writeOff)` to offset 4096. The destination lies entirely
   inside the dead prefix (live ≤ dead), so the live source is untouched.
2. `Sync`.
3. Flip the checkpoint to 4096 (alternating slot).
4. `writeOff = 4096 + live`; `Truncate(writeOff)` (no-op on raw block) + `Sync`.

Crash at any boundary recovers: before the flip, the old checkpoint's source
is intact; after the flip, the copy is complete and the stale tail is
recognized by the recovery rule below.

## 4. Recovery

1. Read superblock; validate magic, version (3), cluster/member identity,
   and storage generation (regression rejected, §9). Read the persisted
   `SegmentSize` and use it as the scan grid (the caller's option does not
   override it). A fresh (all-zero) device is initialized only when the caller
   explicitly allows creation.
2. Select the checkpoint (dual slots). Scan from the checkpoint:
   - zero magic → skip to next segment boundary;
   - bad magic / CRC mismatch / torn record → disposable **only** if no live
     record follows; otherwise recovery intervention (`ErrCorrupt`);
   - a valid-CRC record whose sequence ≤ the last applied sequence marks the
     **stale tail** left by compaction: stop scanning (sequence regression
     under valid checksums cannot occur in the live region);
   - apply batches/chunks/applied records in order; a snapshot manifest
     resolves its stream reference against staged chunks (missing or corrupt
     data → intervention).
3. `writeOff` = end of the last valid record (never the zero-skip cursor).
   Dispose the torn tail via `Truncate` (logical on raw block) + `Sync`.
4. Commit metadata must never reference entries missing from the recoverable
   log/snapshot; violation → intervention.
5. The persisted `SegmentSize` governs the scan grid for the store's lifetime;
   a caller option larger than the persisted grid is ignored for recovery (the
   grid is fixed at creation).

Ambiguous corruption in a committed prefix, a missing referenced chunk, or an
unsupported version stops recovery; the library never silently truncates
arbitrary corruption and continues (§6.3).

## 5. Synchronization primitives

- File binding: `pwrite`/`pread` + `fsync` (`FileDevice.Sync`). Single
  pre-created file; no rename-based publication, so directory fsync is not on
  the critical path. Creation of the file itself must be directory-durable
  before first use (the bootstrap layer creates stores explicitly).
- Raw-block binding: positional writes + device flush; `Truncate` is a no-op.
  Exclusive volume attachment is the ownership fence; file locking is
  advisory only and never a distributed fence (§6.3, §9).
- Group commit: one ordered batch in flight per node (driver storage loop);
  all requests in a batch wait for the same `Sync` boundary.

## 6. Limits and preflight

`MaxBatchBytes`, `SegmentSize`, `MaxSnapshotBytes`, `MaxDeviceBytes`,
`MinReclaimBytes` are finite and configured. `Preflight(additionalBytes)`
rejects admission when `writeOff + additional + MaxBatchBytes` would exceed
`MaxDeviceBytes`, reserving space for hard-state updates and snapshot-based
reclamation (§8). An actual failed write/sync (including ENOSPC) is terminal
`Failed` for the runtime instance (§6.2).

## 7. Upgrade mechanism

Format changes bump the superblock version. Record payload additions are
trailer-appenditive with presence flags so older payloads decode unchanged.
Incompatible changes (layout, checksum scope, the segment grid) require a new
version and an explicit migration; silent reinterpretation is forbidden.

Version 3 persists `SegmentSize` (§2.1). Versions 1 and 2 did not persist the
segment grid, so reopening such a store with a different `SegmentSize` could
skip live records; version-3 readers therefore **reject** versions 1 and 2 with
an explicit `unsupported version (format upgrade required)` error rather than
reinterpret them. Migrating a pre-v3 store requires an explicit offline tool
that rewrites it with a persisted grid; no in-place upgrade is performed.
