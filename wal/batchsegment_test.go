package wal

import (
	"bytes"
	"testing"

	raft "github.com/sufuree/raft"
)

// TestCoalescedBatchFitsSegment: a coalesced append batch larger than the old
// 1 MiB default segment (but within MaxBatchBytes) must persist, not terminally
// fail the node. During 3-voter catch-up the leader ships many entries in one
// batch; the WAL must accept any batch up to MaxBatchBytes, so the segment is
// sized to hold it (§8 limit chain, §6.2). Regression for the P5 benchmark
// follower failing with "record ... exceeds budget (batch 8388608, segment
// 1048576)".
func TestCoalescedBatchFitsSegment(t *testing.T) {
	dev := NewMemDevice()
	w, err := Open(dev, Options{
		ClusterID: 1, MemberID: 1, Generation: 1,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{1}},
		// Defaults: SegmentSize and MaxBatchBytes left zero.
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	payload := bytes.Repeat([]byte("b"), 1024)
	const n = 2500 // ~2.5 MiB batch: > 1 MiB, < 8 MiB MaxBatchBytes
	ents := make([]raft.Entry, n)
	for i := range ents {
		ents[i] = raft.Entry{Term: 1, Index: raft.Index(i + 1), Type: raft.EntryNormal, Data: payload}
	}
	commit := raft.Index(n)
	if err := w.Save(raft.PersistBatch{Entries: ents, Commit: &commit}); err != nil {
		t.Fatalf("coalesced batch rejected: %v", err)
	}
	got, err := w.ReadEntries(1, n+1)
	if err != nil || len(got) != n {
		t.Fatalf("read back: %d entries, err %v", len(got), err)
	}
}
