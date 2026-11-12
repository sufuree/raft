package sim

import (
	"fmt"
	"testing"

	raft "github.com/sufuree/raft"
)

// TestPreVoteCheckQuorumInteraction: with PreVote and CheckQuorum enabled, an
// isolated member cannot raise its term or depose the healthy leader, and a
// leader without quorum steps down (§4.1, §4.2, §11 progress gate).
func TestPreVoteCheckQuorumInteraction(t *testing.T) {
	s := New(Config{Seed: 71, Nodes: 3, PreVote: true, CheckQuorum: true,
		ElectionTick: 8, HeartbeatTick: 1})
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
	if err := s.Run(150); err != nil {
		t.Fatalf("safety: %v", err)
	}
	// PreVote: the isolated member cannot win a pre-vote, so its term stays.
	if s.Nodes[isolated].Core.Term() > s.Nodes[leader].Core.Term() {
		t.Fatalf("pre-vote did not prevent term raise: iso=%d leader=%d",
			s.Nodes[isolated].Core.Term(), s.Nodes[leader].Core.Term())
	}
	s.Heal()
	if err := s.Run(60); err != nil {
		t.Fatalf("safety after rejoin: %v", err)
	}
	if l, ok := s.Leader(); !ok || l == isolated {
		t.Fatalf("healthy leader deposed: leader=%d ok=%v", l, ok)
	}
	// Now isolate the leader: CheckQuorum must step it down.
	l2, _ := s.Leader()
	groups = map[raft.NodeID]int{l2: 1}
	for id := range s.Nodes {
		if id != l2 {
			groups[id] = 0
		}
	}
	s.Partition(groups)
	if err := s.Run(150); err != nil {
		t.Fatalf("safety: %v", err)
	}
	if s.Nodes[l2].Core.Role() == raft.Leader {
		t.Fatal("leader without quorum did not step down (CheckQuorum)")
	}
	s.Heal()
	if err := s.Run(200); err != nil {
		t.Fatalf("safety after heal: %v", err)
	}
	if _, ok := s.Leader(); !ok {
		t.Fatal("no leader after heal")
	}
}

// TestLeaderRemoval removes the current leader through joint consensus; the
// remaining voters elect a new leader and keep committing (§4.4, §11).
func TestLeaderRemoval(t *testing.T) {
	for seed := int64(80); seed <= 84; seed++ {
		s := New(Config{Seed: seed, Nodes: 3, PreVote: true, CheckQuorum: true,
			ElectionTick: 8, HeartbeatTick: 1})
		leader := waitLeader(t, s, 200)
		s.Propose(leader, []byte("before-removal"))
		if err := s.Run(30); err != nil {
			t.Fatal(err)
		}
		var keep raft.NodeSet
		for id := range s.Nodes {
			if id != leader {
				keep = append(keep, id)
			}
		}
		s.ConfChange(leader, raft.Configuration{Voters: keep})
		if err := s.Run(300); err != nil {
			t.Fatalf("seed %d safety during removal: %v", seed, err)
		}
		// The removed leader must not be a voter in the final configuration.
		for id, n := range s.Nodes {
			cfg := n.Core.Config()
			if cfg.InJoint() {
				t.Fatalf("seed %d node %d stuck in joint", seed, id)
			}
			if cfg.HasVoter(leader) {
				t.Fatalf("seed %d node %d still lists removed leader", seed, id)
			}
		}
		// A new leader from the remaining set commits new commands.
		l2, ok := s.Leader()
		if !ok || l2 == leader {
			t.Fatalf("seed %d: no new leader after removal (leader=%d)", seed, l2)
		}
		s.Propose(l2, []byte("after-removal"))
		if err := s.Run(100); err != nil {
			t.Fatalf("seed %d safety after removal: %v", seed, err)
		}
		for _, id := range keep {
			found := false
			for _, d := range s.Nodes[id].FSM.Commands {
				if string(d) == "after-removal" {
					found = true
				}
			}
			if !found {
				t.Fatalf("seed %d node %d missing post-removal command", seed, id)
			}
		}
	}
}

// TestOneToThreeVoters grows a single-voter cluster to three through joint
// consensus; the new majority governs commitment (§9 expansion, §11).
func TestOneToThreeVoters(t *testing.T) {
	s := New(Config{Seed: 91, Nodes: 3, PreVote: true, CheckQuorum: true,
		ElectionTick: 8, HeartbeatTick: 1, Voters: []raft.NodeID{1}})
	leader := waitLeader(t, s, 200)
	if leader != 1 {
		t.Fatalf("single voter 1 must lead, got %d", leader)
	}
	for i := 0; i < 10; i++ {
		s.Propose(1, []byte(fmt.Sprintf("solo-%d", i)))
		if err := s.Run(5); err != nil {
			t.Fatal(err)
		}
	}
	// Admit 2 and 3 as learners, catch them up, then promote.
	s.ConfChange(1, raft.Configuration{Voters: []raft.NodeID{1}, Learners: []raft.NodeID{2, 3}})
	if err := s.Run(100); err != nil {
		t.Fatal(err)
	}
	s.ConfChange(1, raft.Configuration{Voters: []raft.NodeID{1, 2, 3}})
	if err := s.Run(200); err != nil {
		t.Fatal(err)
	}
	cfg := s.Nodes[1].Core.Config()
	if cfg.InJoint() || len(cfg.Voters) != 3 {
		t.Fatalf("transition did not finalize: %+v", cfg)
	}
	// With three voters, one failure still commits.
	s.Crash(3)
	l2, ok := s.Leader()
	if !ok {
		t.Fatal("no leader after crash")
	}
	s.Propose(l2, []byte("two-of-three"))
	if err := s.Run(100); err != nil {
		t.Fatal(err)
	}
	for _, id := range []raft.NodeID{1, 2} {
		found := false
		for _, d := range s.Nodes[id].FSM.Commands {
			if string(d) == "two-of-three" {
				found = true
			}
		}
		if !found {
			t.Fatalf("node %d missing two-of-three command", id)
		}
	}
}

// TestLearnerPromotionDurableCatchUp: a learner catches up durably before
// promotion, and participates in commitment afterwards (§4.4, §9).
func TestLearnerPromotionDurableCatchUp(t *testing.T) {
	s := New(Config{Seed: 95, Nodes: 4, PreVote: true, CheckQuorum: true,
		ElectionTick: 8, HeartbeatTick: 1, Voters: []raft.NodeID{1, 2, 3}})
	leader := waitLeader(t, s, 200)
	for i := 0; i < 30; i++ {
		s.Propose(leader, []byte(fmt.Sprintf("bulk-%d", i)))
		if err := s.Run(3); err != nil {
			t.Fatal(err)
		}
	}
	s.ConfChange(leader, raft.Configuration{Voters: []raft.NodeID{1, 2, 3}, Learners: []raft.NodeID{4}})
	if err := s.Run(200); err != nil {
		t.Fatal(err)
	}
	// Learner 4 must have durably caught up before promotion.
	_, _, _, _, ap4 := s.Nodes[4].Core.Watermarks()
	_, _, _, _, apL := s.Nodes[leader].Core.Watermarks()
	if ap4 < apL-2 {
		t.Fatalf("learner not caught up: %d vs leader %d", ap4, apL)
	}
	s.ConfChange(leader, raft.Configuration{Voters: []raft.NodeID{1, 2, 3, 4}})
	if err := s.Run(200); err != nil {
		t.Fatal(err)
	}
	if s.Nodes[leader].Core.Config().InJoint() {
		t.Fatal("promotion stuck in joint")
	}
	// Commit with the new four-voter majority (3 of 4): crash one voter.
	s.Crash(2)
	l2, ok := s.Leader()
	if !ok {
		t.Fatal("no leader")
	}
	s.Propose(l2, []byte("four-voter"))
	if err := s.Run(150); err != nil {
		t.Fatal(err)
	}
	for _, id := range []raft.NodeID{1, 3, 4} {
		if !s.Nodes[id].Alive {
			continue
		}
		found := false
		for _, d := range s.Nodes[id].FSM.Commands {
			if string(d) == "four-voter" {
				found = true
			}
		}
		if !found {
			t.Fatalf("node %d missing four-voter command", id)
		}
	}
}
