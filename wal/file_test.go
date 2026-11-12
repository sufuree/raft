package wal

import (
	"path/filepath"
	"testing"

	raft "github.com/sufuree/raft"
)

// TestFileSubstrate exercises the WAL on a real filesystem file.
func TestFileSubstrate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	opts := Options{ClusterID: 7, MemberID: 3, Generation: 1,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{1, 2, 3}}}
	dev, err := OpenFile(path)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	w, err := Open(dev, opts)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	for i := raft.Index(1); i <= 20; i++ {
		c := i
		if err := w.Save(raft.PersistBatch{
			Entries: []raft.Entry{entry(i, 1, "v")}, Commit: &c,
		}); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	w.MarkApplied(20)
	w.Close()
	dev.Close()

	dev2, err := OpenFile(path)
	if err != nil {
		t.Fatalf("reopen file: %v", err)
	}
	w2, err := Open(dev2, opts)
	if err != nil {
		t.Fatalf("recover file: %v", err)
	}
	init, _ := w2.Load()
	if init.DurableIndex != 20 || init.HardState.Commit != 20 || init.AppliedIndex != 20 {
		t.Fatalf("file recovery wrong: %+v", init)
	}
	ents, err := w2.ReadEntries(1, 21)
	if err != nil || len(ents) != 20 {
		t.Fatalf("read entries: %v len=%d", err, len(ents))
	}
	w2.Close()
	dev2.Close()
}
