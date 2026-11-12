package sim

import (
	"testing"

	raft "github.com/sufuree/raft"
)

// TestRejoinDoesNotDepose verifies a partitioned member that raised its term
// cannot depose a healthy leader on rejoin (§4.1, §11 progress gate).
func TestRejoinDoesNotDepose(t *testing.T) {
	s := New(Config{Seed: 61, Nodes: 3, PreVote: false, CheckQuorum: true})
	leader := waitLeader(t, s, 200)
	var isolated raft.NodeID
	for id := range s.Nodes {
		if id != leader {
			isolated = id
			break
		}
	}
	groups := map[raft.NodeID]int{isolated: 1}
	for id := range s.Nodes {
		if id != isolated {
			groups[id] = 0
		}
	}
	s.Partition(groups)
	// Isolated node campaigns and raises its term; majority keeps its leader.
	if err := s.Run(150); err != nil {
		t.Fatalf("safety: %v", err)
	}
	if s.Nodes[isolated].Core.Term() <= s.Nodes[leader].Core.Term() {
		t.Fatalf("isolated node did not raise term: iso=%d leader=%d",
			s.Nodes[isolated].Core.Term(), s.Nodes[leader].Core.Term())
	}
	s.Heal()
	if err := s.Run(60); err != nil {
		t.Fatalf("safety after rejoin: %v", err)
	}
	// The healthy majority leader must retain leadership.
	l, ok := s.Leader()
	if !ok || l == isolated {
		t.Fatalf("healthy leader deposed by rejoining member: leader=%d ok=%v", l, ok)
	}
}
