package wal

import (
	"bytes"
	"testing"

	raft "github.com/sufuree/raft"
)

// TestStreamedPublicationCheckpoint: successive streamed snapshot publications
// must leave a clean-reopenable store. The segment-aligned checkpoint of a newer
// publication must exclude a superseded publication whole (manifest + chunks),
// never strand its manifest above the checkpoint without its chunks (§6.4).
// Regression for the B4 clean-shutdown reopen ErrCorrupt.
func TestStreamedPublicationCheckpoint(t *testing.T) {
	for _, seg := range []int64{16 << 20, 64 << 20} {
		dev := NewMemDevice()
		opts := testOpts()
		opts.SegmentSize = seg
		opts.MaxBatchBytes = 8 << 20
		opts.MaxSnapshotBytes = 128 << 20
		w, err := Open(dev, opts)
		if err != nil {
			t.Fatal(err)
		}
		sizes := []int{18, 5, 20, 9} // MiB; mix of multi- and single-segment
		var last raft.Index
		for i, mb := range sizes {
			last = raft.Index(100 * (i + 1))
			c := last
			snap := raft.SnapshotMeta{
				Index: last, Term: 1,
				Config: raft.Configuration{Voters: raft.NodeSet{1}},
				Data:   bytes.Repeat([]byte{byte(i + 1)}, mb<<20),
			}
			if err := w.Save(raft.PersistBatch{Snapshot: &snap, Commit: &c}); err != nil {
				t.Fatalf("seg %d save %d: %v", seg, last, err)
			}
		}
		w.Close()
		w2, err := Open(dev, opts)
		if err != nil {
			t.Fatalf("seg %d: reopen after clean close failed: %v", seg, err)
		}
		st, err := w2.Load()
		if err != nil {
			t.Fatalf("seg %d load: %v", seg, err)
		}
		if st.Snapshot.Index != last {
			t.Fatalf("seg %d: snapshot index %d, want newest %d", seg, st.Snapshot.Index, last)
		}
		if len(st.Snapshot.Data) != sizes[len(sizes)-1]<<20 {
			t.Fatalf("seg %d: snapshot data len %d, want %d", seg, len(st.Snapshot.Data), sizes[len(sizes)-1]<<20)
		}
		w2.Close()
	}
}
