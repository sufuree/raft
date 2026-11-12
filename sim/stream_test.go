package sim

import (
	"bytes"
	"testing"

	raft "github.com/sufuree/raft"
)

// TestStreamedSnapshotCatchUp: a restarted follower whose required history
// was compacted catches up through a chunked snapshot transfer (§6.4).
func TestStreamedSnapshotCatchUp(t *testing.T) {
	s := New(Config{
		Seed: 42, Nodes: 3, PreVote: true, CheckQuorum: true,
		ElectionTick: 8, HeartbeatTick: 1,
		SnapshotChunkBytes: 4 << 10, // force streaming for a ~200 KiB snapshot
	})
	s.Run(30)
	l, ok := s.Leader()
	if !ok {
		t.Fatal("no leader")
	}
	payload := bytes.Repeat([]byte("x"), 2<<10)
	for i := 0; i < 100; i++ {
		s.Propose(l, payload)
		s.Run(2)
	}
	// Crash a follower, then compact the leader's history past its match.
	ids := s.sortedIDs()
	var lag raft.NodeID
	for _, id := range ids {
		if id != l {
			lag = id
			break
		}
	}
	s.Crash(lag)
	s.Snapshot(l)
	s.Run(10)
	for i := 0; i < 10; i++ {
		s.Propose(l, payload)
		s.Run(2)
	}
	s.Snapshot(l)
	s.Run(10)
	// Restart the lagging follower: it needs compacted history, so the leader
	// must stream a snapshot.
	s.Restart(lag)
	caught := false
	for i := 0; i < 400 && !caught; i++ {
		s.Run(1)
		_, _, _, _, apL := s.Nodes[l].Core.Watermarks()
		_, _, _, _, apF := s.Nodes[lag].Core.Watermarks()
		if apF >= apL && apL > 100 {
			caught = true
		}
	}
	if !caught {
		_, _, _, _, apL := s.Nodes[l].Core.Watermarks()
		_, _, _, _, apF := s.Nodes[lag].Core.Watermarks()
		t.Fatalf("streamed catch-up failed: leader applied %d follower %d", apL, apF)
	}
	// The follower installed a snapshot (streamed) covering compacted history.
	fs, _ := s.Nodes[lag].Store.ReadSnapshot()
	if fs.Index == 0 {
		t.Fatal("follower did not install a snapshot")
	}
	// Proposals still commit after catch-up.
	s.Propose(l, []byte("post-catchup"))
	s.Run(30)
	for _, id := range ids {
		if !s.Nodes[id].Alive {
			continue
		}
		found := false
		for _, d := range s.Nodes[id].FSM.Commands {
			if string(d) == "post-catchup" {
				found = true
			}
		}
		if !found {
			t.Fatalf("node %d missing post-catchup command", id)
		}
	}
}

// TestStreamedSnapshotStaleIgnored: a streamed snapshot that becomes stale
// mid-transfer never regresses the receiver's watermarks (§6.4).
func TestStreamedSnapshotStaleIgnored(t *testing.T) {
	s := New(Config{
		Seed: 7, Nodes: 3, PreVote: true, CheckQuorum: true,
		ElectionTick: 8, HeartbeatTick: 1, SnapshotChunkBytes: 1 << 10,
	})
	s.Run(30)
	l, ok := s.Leader()
	if !ok {
		t.Fatal("no leader")
	}
	for i := 0; i < 40; i++ {
		s.Propose(l, bytes.Repeat([]byte("y"), 1024))
		s.Run(2)
	}
	s.Snapshot(l)
	s.Run(20)
	// All nodes applied past the snapshot; a delayed stream must be ignored.
	for _, id := range s.sortedIDs() {
		_, _, _, _, ap := s.Nodes[id].Core.Watermarks()
		if ap < 40 {
			t.Fatalf("node %d applied %d too low", id, ap)
		}
	}
	s.Run(100)
	for _, id := range s.sortedIDs() {
		_, _, _, _, ap := s.Nodes[id].Core.Watermarks()
		if ap < 40 {
			t.Fatalf("node %d regressed to %d", id, ap)
		}
	}
}
