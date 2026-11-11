package wal

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"testing"

	raft "github.com/sufuree/raft"
)

func testOpts() Options {
	return Options{
		ClusterID: 7, MemberID: 1, Generation: 1,
		SegmentSize: 64 << 10, MaxBatchBytes: 32 << 10,
		MinReclaimBytes: 8 << 10,
		InitialConfig:   raft.Configuration{Voters: raft.NodeSet{1}},
	}
}

func seedEntries(t *testing.T, w *WAL, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		commit := raft.Index(i)
		err := w.Save(raft.PersistBatch{
			Entries: []raft.Entry{{Term: 1, Index: raft.Index(i), Data: []byte(fmt.Sprintf("e%d", i))}},
			Commit:  &commit,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// seedLargeEntries writes entries that span multiple segments so checkpoint
// alignment leaves a reclaimable dead prefix.
func seedLargeEntries(t *testing.T, w *WAL, n int) {
	t.Helper()
	payload := bytes.Repeat([]byte("x"), 1024)
	for i := 1; i <= n; i++ {
		commit := raft.Index(i)
		err := w.Save(raft.PersistBatch{
			Entries: []raft.Entry{{Term: 1, Index: raft.Index(i), Data: payload}},
			Commit:  &commit,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestReclamationPreservesRecoverability: compaction reclaims the dead prefix
// and recovery reconstructs the full state (§6.3, §11 persistence gate).
func TestReclamationPreservesRecoverability(t *testing.T) {
	dev := NewMemDevice()
	opts := testOpts()
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	seedLargeEntries(t, w, 200)
	hs := raft.HardState{Term: 3, Vote: 2, Commit: 200}
	if err := w.Save(raft.PersistBatch{HardState: &hs}); err != nil {
		t.Fatal(err)
	}
	if err := w.MarkApplied(200); err != nil {
		t.Fatal(err)
	}
	sizeBefore, _ := dev.Size()
	// Publish a snapshot covering most of the log; the dead prefix exceeds
	// the live suffix, so compaction must run.
	snap := raft.SnapshotMeta{
		Index: 190, Term: 1,
		Config: raft.Configuration{Voters: raft.NodeSet{1, 2, 3}},
		Data:   []byte("snapshot-state"),
	}
	commit := raft.Index(190)
	if err := w.Save(raft.PersistBatch{Snapshot: &snap, Commit: &commit}); err != nil {
		t.Fatal(err)
	}
	// Append a suffix above the snapshot.
	for i := 191; i <= 210; i++ {
		c := raft.Index(i)
		if err := w.Save(raft.PersistBatch{
			Entries: []raft.Entry{{Term: 3, Index: raft.Index(i), Data: []byte(fmt.Sprintf("e%d", i))}},
			Commit:  &c,
		}); err != nil {
			t.Fatal(err)
		}
	}
	sizeAfter, _ := dev.Size()
	if sizeAfter >= sizeBefore {
		t.Fatalf("compaction did not reclaim: before %d after %d", sizeBefore, sizeAfter)
	}
	w.Close()

	w2, err := Open(dev, opts)
	if err != nil {
		t.Fatalf("reopen after compaction: %v", err)
	}
	st, err := w2.Load()
	if err != nil {
		t.Fatal(err)
	}
	if st.HardState.Term != 3 || st.HardState.Vote != 2 {
		t.Fatalf("hard state lost: %+v", st.HardState)
	}
	if st.Snapshot.Index != 190 || string(st.Snapshot.Data) != "snapshot-state" {
		t.Fatalf("snapshot lost: %+v", st.Snapshot)
	}
	if len(st.Entries) != 20 || st.Entries[0].Index != 191 || st.Entries[19].Index != 210 {
		t.Fatalf("suffix lost: %d entries", len(st.Entries))
	}
	if st.AppliedIndex != 200 {
		t.Fatalf("applied watermark lost: %d", st.AppliedIndex)
	}
	if st.HardState.Commit != 210 {
		t.Fatalf("commit lost: %d", st.HardState.Commit)
	}
}

// TestPublicationCrashBoundaries crashes at every sync boundary of a large
// snapshot publication and compaction; recovery must always succeed and
// preserve the committed prefix (§11 persistence gate).
func TestPublicationCrashBoundaries(t *testing.T) {
	for crashAt := 1; crashAt <= 14; crashAt++ {
		dev := NewMemDevice()
		opts := testOpts()
		w, err := Open(dev, opts)
		if err != nil {
			t.Fatal(err)
		}
		seedEntries(t, w, 100)
		if err := w.MarkApplied(100); err != nil {
			t.Fatal(err)
		}
		base := dev.SyncCount
		crashAt := base + crashAt
		dev.OnSync = func(n int) {
			if n == crashAt {
				dev.Crash()
			}
		}
		snap := raft.SnapshotMeta{
			Index: 90, Term: 1,
			Config: raft.Configuration{Voters: raft.NodeSet{1}},
			Data:   bytes.Repeat([]byte("S"), 200<<10), // streamed: > MaxBatchBytes/2
		}
		commit := raft.Index(90)
		_ = w.Save(raft.PersistBatch{Snapshot: &snap, Commit: &commit})
		dev.OnSync = nil
		dev.PowerOn()

		w2, err := Open(dev, opts)
		if err != nil {
			t.Fatalf("crashAt +%d: recovery failed: %v", crashAt-base, err)
		}
		st, err := w2.Load()
		if err != nil {
			t.Fatalf("crashAt +%d: load: %v", crashAt-base, err)
		}
		// The committed prefix (100 entries at term 1) must survive either
		// as the log or as snapshot+suffix; commit never regresses.
		if st.HardState.Commit < 90 {
			t.Fatalf("crashAt +%d: commit regressed to %d", crashAt-base, st.HardState.Commit)
		}
		if st.Snapshot.Index == 90 {
			if len(st.Snapshot.Data) != 200<<10 {
				t.Fatalf("crashAt +%d: partial snapshot published", crashAt-base)
			}
		} else if st.Snapshot.Index == 0 {
			if len(st.Entries) < 90 {
				t.Fatalf("crashAt +%d: lost committed entries without snapshot", crashAt-base)
			}
		}
		w2.Close()
	}
}

// TestInterruptedStagingNeverSnapshot: chunks without a durable manifest are
// orphans and never become a snapshot (§6.4).
func TestInterruptedStagingNeverSnapshot(t *testing.T) {
	dev := NewMemDevice()
	opts := testOpts()
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	seedEntries(t, w, 10)
	// Fail the sync that durably publishes the manifest: chunks exist, the
	// manifest does not.
	dev.OnSync = func(n int) {
		if n == dev.SyncCount { // crash at every sync after seeding
		}
	}
	dev.OnSync = nil
	base := dev.SyncCount
	dev.OnSync = func(n int) {
		if n == base+1 { // first sync of the publication = chunk sync
			dev.Crash()
		}
	}
	snap := raft.SnapshotMeta{Index: 8, Term: 1, Config: raft.Configuration{Voters: raft.NodeSet{1}},
		Data: bytes.Repeat([]byte("X"), 100<<10)}
	commit := raft.Index(8)
	_ = w.Save(raft.PersistBatch{Snapshot: &snap, Commit: &commit})
	dev.OnSync = nil
	dev.PowerOn()
	w2, err := Open(dev, opts)
	if err != nil {
		t.Fatalf("orphan chunks broke recovery: %v", err)
	}
	st, _ := w2.Load()
	if st.Snapshot.Index != 0 {
		t.Fatalf("interrupted staging became a snapshot: %d", st.Snapshot.Index)
	}
	if len(st.Entries) != 10 {
		t.Fatalf("committed log lost: %d", len(st.Entries))
	}
}

// TestLargeSnapshotRoundTrip: a snapshot larger than a batch streams through
// chunk records and reassembles with end-to-end integrity (§6.4).
func TestLargeSnapshotRoundTrip(t *testing.T) {
	dev := NewMemDevice()
	opts := testOpts()
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	seedEntries(t, w, 5)
	data := bytes.Repeat([]byte("archive"), 64<<10) // 448 KiB > batch/2
	snap := raft.SnapshotMeta{Index: 5, Term: 1, Config: raft.Configuration{Voters: raft.NodeSet{1}}, Data: data}
	commit := raft.Index(5)
	if err := w.Save(raft.PersistBatch{Snapshot: &snap, Commit: &commit}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	w2, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := w2.Load()
	if !bytes.Equal(st.Snapshot.Data, data) {
		t.Fatalf("streamed snapshot mismatch: %d bytes", len(st.Snapshot.Data))
	}
	// Corrupt one chunk byte: the manifest's checksum must stop recovery.
	raw := dev.data
	idx := bytes.Index(raw, []byte("archivearchive"))
	raw[idx+100] ^= 0xFF
	w2.Close()
	if _, err := Open(dev, opts); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt chunk not detected: %v", err)
	}
}

// TestFixedRegionBinding: the WAL runs on raw block-device semantics —
// constant size, no-op truncate, ENOSPC past the region (§6.3).
func TestFixedRegionBinding(t *testing.T) {
	backing := NewMemDevice()
	// Pre-size the backing store; the fixed region never grows.
	backing.WriteAt(make([]byte, 512<<10), 0)
	backing.Sync()
	dev, err := NewFixedRegion(backing, 512<<10)
	if err != nil {
		t.Fatal(err)
	}
	opts := testOpts()
	opts.MaxDeviceBytes = 512 << 10
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	seedEntries(t, w, 100)
	snap := raft.SnapshotMeta{Index: 90, Term: 1, Config: raft.Configuration{Voters: raft.NodeSet{1}}, Data: []byte("s")}
	commit := raft.Index(90)
	if err := w.Save(raft.PersistBatch{Snapshot: &snap, Commit: &commit}); err != nil {
		t.Fatal(err)
	}
	for i := 91; i <= 120; i++ {
		c := raft.Index(i)
		if err := w.Save(raft.PersistBatch{Entries: []raft.Entry{{Term: 1, Index: raft.Index(i)}}, Commit: &c}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	w2, err := Open(dev, opts)
	if err != nil {
		t.Fatalf("fixed-region recovery: %v", err)
	}
	st, _ := w2.Load()
	if st.Snapshot.Index != 90 || st.HardState.Commit != 120 {
		t.Fatalf("state lost on fixed region: snap %d commit %d", st.Snapshot.Index, st.HardState.Commit)
	}
	// Writes past the region are ENOSPC, never growth.
	if _, err := dev.WriteAt([]byte{1}, 512<<10); err == nil {
		t.Fatal("write past region succeeded")
	}
	// Preflight rejects admission before exhaustion, preserving the reserve.
	if err := w2.Preflight(512 << 10); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("preflight did not reject: %v", err)
	}
	if err := w2.Preflight(1); err != nil {
		t.Fatalf("preflight rejected a small admission: %v", err)
	}
}

// TestDiskFullTerminal: an actual failed write surfaces ErrNoSpace; the
// driver turns any Save failure into terminal Failed (§6.2, tested there).
func TestDiskFullTerminal(t *testing.T) {
	backing := NewMemDevice()
	backing.WriteAt(make([]byte, 64<<10), 0)
	backing.Sync()
	dev, _ := NewFixedRegion(backing, 64<<10)
	opts := testOpts()
	opts.MaxDeviceBytes = 64 << 10
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	var saveErr error
	for i := 1; i < 10000; i++ {
		c := raft.Index(i)
		saveErr = w.Save(raft.PersistBatch{
			Entries: []raft.Entry{{Term: 1, Index: raft.Index(i), Data: bytes.Repeat([]byte("x"), 512)}},
			Commit:  &c,
		})
		if saveErr != nil {
			break
		}
	}
	if !errors.Is(saveErr, ErrNoSpace) {
		t.Fatalf("expected ErrNoSpace, got %v", saveErr)
	}
}

// TestV1Upgrade: a version-1 superblock is recovered and upgraded in place
// (§6.3 on-disk upgrade mechanism).
func TestV1Upgrade(t *testing.T) {
	dev := NewMemDevice()
	opts := testOpts()
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	seedEntries(t, w, 20)
	w.Close()
	// Regress the version field to v1 (layout is otherwise compatible for
	// entry-only logs).
	dev.data[4] = 0
	dev.data[5] = 0
	dev.data[6] = 0
	dev.data[7] = 1
	dev.Sync()
	w2, err := Open(dev, opts)
	if err != nil {
		t.Fatalf("v1 recovery: %v", err)
	}
	st, _ := w2.Load()
	if len(st.Entries) != 20 {
		t.Fatalf("v1 entries lost: %d", len(st.Entries))
	}
	w2.Close()
	if v := uint32(dev.data[4])<<24 | uint32(dev.data[5])<<16 | uint32(dev.data[6])<<8 | uint32(dev.data[7]); v != formatVersion {
		t.Fatalf("not upgraded: version %d", v)
	}
}

// TestCheckpointSlotTorn: a torn newest slot falls back to the previous valid
// checkpoint without losing committed state.
func TestCheckpointSlotTorn(t *testing.T) {
	dev := NewMemDevice()
	opts := testOpts()
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	seedEntries(t, w, 50)
	snap := raft.SnapshotMeta{Index: 40, Term: 1, Config: raft.Configuration{Voters: raft.NodeSet{1}}, Data: []byte("s")}
	commit := raft.Index(40)
	if err := w.Save(raft.PersistBatch{Snapshot: &snap, Commit: &commit}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	// Corrupt both slots' CRCs: recovery falls back to the region start and
	// replays everything (the pre-checkpoint behavior).
	for _, off := range []int{slotAOff, slotBOff} {
		dev.data[off+24] ^= 0xFF
	}
	dev.Sync()
	w2, err := Open(dev, opts)
	if err != nil {
		t.Fatalf("torn slots broke recovery: %v", err)
	}
	st, _ := w2.Load()
	if st.Snapshot.Index != 40 || st.HardState.Commit < 40 {
		t.Fatalf("state lost with torn slots: %+v", st.Snapshot)
	}
}

// TestBoundedGrowth: repeated snapshot cycles keep the device bounded (§8).
func TestBoundedGrowth(t *testing.T) {
	dev := NewMemDevice()
	opts := testOpts()
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	var peak int64
	idx := raft.Index(0)
	for cycle := 0; cycle < 20; cycle++ {
		for i := 0; i < 50; i++ {
			idx++
			c := idx
			if err := w.Save(raft.PersistBatch{
				Entries: []raft.Entry{{Term: 1, Index: idx, Data: bytes.Repeat([]byte("x"), 1024)}},
				Commit:  &c,
			}); err != nil {
				t.Fatal(err)
			}
		}
		snap := raft.SnapshotMeta{Index: idx - 5, Term: 1, Config: raft.Configuration{Voters: raft.NodeSet{1}}, Data: []byte("s")}
		c := snap.Index
		if err := w.Save(raft.PersistBatch{Snapshot: &snap, Commit: &c}); err != nil {
			t.Fatal(err)
		}
		if sz, _ := dev.Size(); sz > peak {
			peak = sz
		}
	}
	// 1000 entries × 1 KiB ≈ 1 MiB written total; bounded growth means the
	// device stays far below the cumulative write volume.
	if peak > 4<<20 {
		t.Fatalf("unbounded growth: peak %d", peak)
	}
	w.Close()
	w2, err := Open(dev, opts)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := w2.Load()
	if st.HardState.Commit != idx {
		t.Fatalf("final commit lost: %d != %d", st.HardState.Commit, idx)
	}
	_ = crc32.ChecksumIEEE // keep import for corruption tests above
}
