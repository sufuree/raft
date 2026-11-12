package sim

import (
	"testing"

	raft "github.com/sufuree/raft"
)

// TestMembershipAddVoter grows a 3-voter cluster to 5 via joint consensus.
func TestMembershipAddVoter(t *testing.T) {
	s := New(Config{Seed: 11, Nodes: 5, PreVote: true, CheckQuorum: true,
		Voters: []raft.NodeID{1, 2, 3}})
	// Nodes 4,5 start as learners outside the voter set; admit them via conf.
	leader := waitLeader(t, s, 300)
	s.Propose(leader, []byte("before-membership"))
	if err := s.Run(50); err != nil {
		t.Fatalf("safety: %v", err)
	}
	target := raft.Configuration{Voters: []raft.NodeID{1, 2, 3, 4, 5}}
	s.ConfChange(leader, target)
	if err := s.Run(300); err != nil {
		t.Fatalf("safety during membership: %v", err)
	}
	// Configuration must finalize to the 5-voter set (no joint).
	cfg := s.Nodes[leader].Core.Config()
	if cfg.InJoint() {
		t.Fatalf("configuration stuck in joint: %+v", cfg)
	}
	if len(cfg.Voters) != 5 {
		t.Fatalf("expected 5 voters, got %d", len(cfg.Voters))
	}
	// Propose after transition; all five must apply.
	l2, ok := s.Leader()
	if !ok {
		t.Fatalf("no leader after membership change")
	}
	s.Propose(l2, []byte("after-membership"))
	if err := s.Run(200); err != nil {
		t.Fatalf("safety: %v", err)
	}
	for id, n := range s.Nodes {
		found := false
		for _, d := range n.FSM.Commands {
			if string(d) == "after-membership" {
				found = true
			}
		}
		if !found {
			t.Fatalf("node %d missing post-membership command (applied=%d)", id, n.FSM.Applied)
		}
	}
}

// TestMembershipCrashDuringJoint crashes nodes during a joint transition and
// verifies recovery preserves quorum rules and the configuration.
func TestMembershipCrashDuringJoint(t *testing.T) {
	for seed := int64(20); seed <= 30; seed++ {
		s := New(Config{Seed: seed, Nodes: 5, PreVote: true, CheckQuorum: true,
			Voters: []raft.NodeID{1, 2, 3}})
		leader := waitLeader(t, s, 300)
		target := raft.Configuration{Voters: []raft.NodeID{1, 2, 3, 4, 5}}
		s.ConfChange(leader, target)
		// Crash a node mid-transition.
		if err := s.Run(10); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		s.Crash(2)
		if err := s.Run(200); err != nil {
			t.Fatalf("seed %d safety during joint+crash: %v", seed, err)
		}
		s.Restart(2)
		if err := s.Run(300); err != nil {
			t.Fatalf("seed %d safety after restart: %v", seed, err)
		}
	}
}
