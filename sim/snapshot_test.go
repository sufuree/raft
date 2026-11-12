package sim

import (
	"fmt"
	"testing"

	raft "github.com/sufuree/raft"
)

func TestSnapshotCatchUp(t *testing.T) {
	s := New(Config{Seed: 51, Nodes: 3, PreVote: true, CheckQuorum: true})
	leader := waitLeader(t, s, 200)
	var follower raft.NodeID
	for id := range s.Nodes {
		if id != leader {
			follower = id
			break
		}
	}
	s.Propose(leader, []byte("early"))
	if err := s.Run(20); err != nil {
		t.Fatalf("safety: %v", err)
	}
	// Freeze the follower at a short log, then advance and compact the leader.
	s.Crash(follower)
	for i := 0; i < 30; i++ {
		s.Propose(leader, []byte(fmt.Sprintf("c%d", i)))
		if err := s.Run(3); err != nil {
			t.Fatalf("safety: %v", err)
		}
	}
	if err := s.Run(50); err != nil {
		t.Fatalf("safety: %v", err)
	}
	s.Snapshot(leader)
	if err := s.Run(20); err != nil {
		t.Fatalf("safety: %v", err)
	}
	s.Propose(leader, []byte("after-snap"))
	if err := s.Run(20); err != nil {
		t.Fatalf("safety: %v", err)
	}
	s.Restart(follower)
	if err := s.Run(300); err != nil {
		t.Fatalf("safety: %v", err)
	}
	n := s.Nodes[follower]
	hasEarly, hasAfter := false, false
	for _, d := range n.FSM.Commands {
		if string(d) == "early" {
			hasEarly = true
		}
		if string(d) == "after-snap" {
			hasAfter = true
		}
	}
	if !hasEarly || !hasAfter {
		t.Fatalf("snapshot catch-up failed: early=%v after=%v applied=%d", hasEarly, hasAfter, n.FSM.Applied)
	}
}
