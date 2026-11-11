package wal

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"

	raft "github.com/sufuree/raft"
)

func entry(i raft.Index, term raft.Term, data string) raft.Entry {
	return raft.Entry{Index: i, Term: term, Type: raft.EntryNormal, Data: []byte(data)}
}

func openMem(t *testing.T, dev *MemDevice, opts Options) *WAL {
	t.Helper()
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w
}

func TestPersistReopen(t *testing.T) {
	dev := NewMemDevice()
	opts := Options{ClusterID: 1, MemberID: 2, Generation: 1,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{1, 2, 3}}}
	w := openMem(t, dev, opts)
	hs := raft.HardState{Term: 3, Vote: 2}
	c := raft.Index(2)
	if err := w.Save(raft.PersistBatch{
		HardState: &hs,
		Entries:   []raft.Entry{entry(1, 3, "a"), entry(2, 3, "b")},
		Commit:    &c,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	w.MarkApplied(2)
	w.Close()

	w2 := openMem(t, dev, opts)
	init, _ := w2.Load()
	if init.HardState.Term != 3 || init.HardState.Vote != 2 || init.HardState.Commit != 2 {
		t.Fatalf("hard state lost: %+v", init.HardState)
	}
	if init.AppliedIndex != 2 || init.DurableIndex != 2 {
		t.Fatalf("watermarks lost: applied=%d durable=%d", init.AppliedIndex, init.DurableIndex)
	}
	if len(init.Entries) != 2 || string(init.Entries[1].Data) != "b" {
		t.Fatalf("entries lost: %+v", init.Entries)
	}
}

func TestTornFinalBatchDiscarded(t *testing.T) {
	dev := NewMemDevice()
	opts := Options{ClusterID: 1, MemberID: 1}
	w := openMem(t, dev, opts)
	c1 := raft.Index(1)
	w.Save(raft.PersistBatch{Entries: []raft.Entry{entry(1, 1, "a")}, Commit: &c1})
	// Tear the next write so its tail never lands.
	dev.TornTail = 6
	c2 := raft.Index(2)
	w.Save(raft.PersistBatch{Entries: []raft.Entry{entry(2, 1, "b")}, Commit: &c2})
	dev.Crash()
	dev.PowerOn()
	w2, err := Open(dev, opts)
	if err != nil {
		t.Fatalf("recovery after torn batch: %v", err)
	}
	init, _ := w2.Load()
	if init.DurableIndex != 1 || init.HardState.Commit != 1 {
		t.Fatalf("torn batch not discarded: durable=%d commit=%d", init.DurableIndex, init.HardState.Commit)
	}
}

func TestUnsyncedTailDisappears(t *testing.T) {
	dev := NewMemDevice()
	opts := Options{ClusterID: 1, MemberID: 1}
	w := openMem(t, dev, opts)
	w.Save(raft.PersistBatch{Entries: []raft.Entry{entry(1, 1, "a")}})
	// Write a partial record directly without syncing, then crash.
	dev.WriteAt([]byte{0x52, 0x42, 0x41, 0x54, 0xff, 0xff}, w.writeOff)
	dev.Crash()
	dev.PowerOn()
	w2, err := Open(dev, opts)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	init, _ := w2.Load()
	if init.DurableIndex != 1 {
		t.Fatalf("unsynced tail survived: durable=%d", init.DurableIndex)
	}
}

func TestSuffixReplacement(t *testing.T) {
	dev := NewMemDevice()
	opts := Options{ClusterID: 1, MemberID: 1}
	w := openMem(t, dev, opts)
	w.Save(raft.PersistBatch{Entries: []raft.Entry{entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")}})
	// Replace suffix from index 2 with a new term-2 entry.
	w.Save(raft.PersistBatch{TruncateAt: 2, Entries: []raft.Entry{entry(2, 2, "B2")}})
	w.Close()
	w2 := openMem(t, dev, opts)
	init, _ := w2.Load()
	if len(init.Entries) != 2 || init.Entries[1].Term != 2 || string(init.Entries[1].Data) != "B2" {
		t.Fatalf("suffix replacement wrong: %+v", init.Entries)
	}
}

func TestCommitBeyondLogCorrupt(t *testing.T) {
	dev := NewMemDevice()
	opts := Options{ClusterID: 1, MemberID: 1}
	w := openMem(t, dev, opts)
	c := raft.Index(99)
	w.Save(raft.PersistBatch{Entries: []raft.Entry{entry(1, 1, "a")}, Commit: &c})
	w.Close()
	if _, err := Open(dev, opts); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt for commit beyond log, got %v", err)
	}
}

func TestSyncFailureTerminal(t *testing.T) {
	dev := NewMemDevice()
	w := openMem(t, dev, Options{ClusterID: 1, MemberID: 1})
	dev.FailSync = true
	if err := w.Save(raft.PersistBatch{Entries: []raft.Entry{entry(1, 1, "a")}}); err == nil {
		t.Fatalf("expected sync failure")
	}
}

func TestIdentityMismatch(t *testing.T) {
	dev := NewMemDevice()
	openMem(t, dev, Options{ClusterID: 1, MemberID: 1}).Close()
	if _, err := Open(dev, Options{ClusterID: 999, MemberID: 1}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected identity mismatch, got %v", err)
	}
}

func TestGenerationRegression(t *testing.T) {
	dev := NewMemDevice()
	// Volume holds generation 3; the external authority expects generation 5,
	// so this volume is a rolled-back older image and must be rejected.
	openMem(t, dev, Options{ClusterID: 1, MemberID: 1, Generation: 3}).Close()
	if _, err := Open(dev, Options{ClusterID: 1, MemberID: 1, Generation: 5}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected generation regression rejection, got %v", err)
	}
}

// TestDifferential runs identical valid batch sequences against the WAL and the
// in-memory reference storage and asserts identical recovered state.
func TestDifferential(t *testing.T) {
	for seed := int64(1); seed <= 30; seed++ {
		rng := rand.New(rand.NewSource(seed))
		dev := NewMemDevice()
		opts := Options{ClusterID: 1, MemberID: 1, InitialConfig: raft.Configuration{Voters: raft.NodeSet{1}}}
		w := openMem(t, dev, opts)
		mem := raft.NewMemStorage()
		mem.SetInitialConfig(opts.InitialConfig)

		var next raft.Index = 1
		var term raft.Term = 1
		for step := 0; step < 60; step++ {
			var b raft.PersistBatch
			switch rng.Intn(4) {
			case 0: // append
				n := 1 + rng.Intn(3)
				for i := 0; i < n; i++ {
					b.Entries = append(b.Entries, entry(next, term, "x"))
					next++
				}
			case 1: // hard state
				term++
				b.HardState = &raft.HardState{Term: term, Vote: raft.NodeID(1 + rng.Intn(3))}
			case 2: // suffix replacement (uncommitted)
				if next > 2 {
					trunc := raft.Index(1 + rng.Intn(int(next-1)))
					b.TruncateAt = trunc
					next = trunc
					b.Entries = append(b.Entries, entry(next, term, "y"))
					next++
				}
			case 3: // commit advance
				if next > 1 {
					c := raft.Index(rng.Intn(int(next)))
					b.Commit = &c
				}
			}
			if err := w.Save(b); err != nil {
				t.Fatalf("seed %d step %d wal save: %v", seed, step, err)
			}
			if err := mem.Save(b); err != nil {
				t.Fatalf("seed %d step %d mem save: %v", seed, step, err)
			}
			wi, _ := w.Load()
			mi, _ := mem.Load()
			if wi.HardState != mi.HardState || wi.DurableIndex != mi.DurableIndex {
				t.Fatalf("seed %d step %d state mismatch: wal=%+v mem=%+v", seed, step, wi.HardState, mi.HardState)
			}
			if len(wi.Entries) != len(mi.Entries) {
				t.Fatalf("seed %d step %d entry count mismatch", seed, step)
			}
			for i := range wi.Entries {
				if wi.Entries[i].Index != mi.Entries[i].Index || wi.Entries[i].Term != mi.Entries[i].Term || !bytes.Equal(wi.Entries[i].Data, mi.Entries[i].Data) {
					t.Fatalf("seed %d step %d entry %d mismatch", seed, step, i)
				}
			}
		}
	}
}
