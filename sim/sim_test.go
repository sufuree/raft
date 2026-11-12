package sim

import (
	"fmt"
	"testing"

	raft "github.com/sufuree/raft"
)

func waitLeader(t *testing.T, s *Simulator, ticks int) raft.NodeID {
	t.Helper()
	for i := 0; i < ticks; i++ {
		if err := s.Run(1); err != nil {
			t.Fatalf("safety: %v", err)
		}
		if l, ok := s.Leader(); ok {
			return l
		}
	}
	t.Fatalf("no leader after %d ticks", ticks)
	return 0
}

func TestElectionAndCommit(t *testing.T) {
	s := New(Config{Seed: 1, Nodes: 3, PreVote: true, CheckQuorum: true})
	leader := waitLeader(t, s, 200)
	pid := s.Propose(leader, []byte("x=1"))
	if err := s.Run(100); err != nil {
		t.Fatalf("safety: %v", err)
	}
	// All nodes must apply the command at the same index.
	var idx raft.Index
	for id, n := range s.Nodes {
		found := false
		for i, d := range n.FSM.Commands {
			if string(d) == "x=1" {
				idx, found = i, true
			}
		}
		if !found {
			t.Fatalf("node %d did not apply command (applied=%d)", id, n.FSM.Applied)
		}
	}
	for id, n := range s.Nodes {
		if string(n.FSM.Commands[idx]) != "x=1" {
			t.Fatalf("node %d has wrong command at %d", id, idx)
		}
	}
	_ = pid
}

func TestSingleNode(t *testing.T) {
	s := New(Config{Seed: 2, Nodes: 1, PreVote: true})
	leader := waitLeader(t, s, 100)
	if leader != 1 {
		t.Fatalf("expected node 1 leader, got %d", leader)
	}
	s.Propose(1, []byte("a"))
	s.Propose(1, []byte("b"))
	if err := s.Run(50); err != nil {
		t.Fatalf("safety: %v", err)
	}
	n := s.Nodes[1]
	if n.FSM.Applied < 2 {
		t.Fatalf("single node applied=%d, want >=2", n.FSM.Applied)
	}
}

func TestManyProposals(t *testing.T) {
	s := New(Config{Seed: 3, Nodes: 3, PreVote: true, CheckQuorum: true})
	leader := waitLeader(t, s, 200)
	for i := 0; i < 50; i++ {
		s.Propose(leader, []byte(fmt.Sprintf("cmd-%d", i)))
		if err := s.Run(3); err != nil {
			t.Fatalf("safety: %v", err)
		}
	}
	if err := s.Run(200); err != nil {
		t.Fatalf("safety: %v", err)
	}
	for id, n := range s.Nodes {
		if n.FSM.Applied < 50 {
			t.Fatalf("node %d applied=%d, want >=50", id, n.FSM.Applied)
		}
	}
}

func TestPartitionMinority(t *testing.T) {
	s := New(Config{Seed: 4, Nodes: 3, PreVote: true, CheckQuorum: true})
	leader := waitLeader(t, s, 200)
	// Isolate a minority node; the majority must keep committing.
	var minority raft.NodeID
	for id := range s.Nodes {
		if id != leader {
			minority = id
			break
		}
	}
	groups := map[raft.NodeID]int{minority: 1}
	for id := range s.Nodes {
		if id != minority {
			groups[id] = 0
		}
	}
	s.Partition(groups)
	if err := s.Run(50); err != nil {
		t.Fatalf("safety: %v", err)
	}
	// Leader may change; find the majority leader.
	ml, ok := s.Leader()
	if !ok {
		t.Fatalf("no leader in majority after partition")
	}
	s.Propose(ml, []byte("after-partition"))
	if err := s.Run(100); err != nil {
		t.Fatalf("safety: %v", err)
	}
	count := 0
	for id, n := range s.Nodes {
		if id == minority {
			continue
		}
		for _, d := range n.FSM.Commands {
			if string(d) == "after-partition" {
				count++
				break
			}
		}
	}
	if count < 2 {
		t.Fatalf("majority did not commit after partition: %d", count)
	}
}

func TestCrashRecovery(t *testing.T) {
	s := New(Config{Seed: 5, Nodes: 3, PreVote: true, CheckQuorum: true})
	leader := waitLeader(t, s, 200)
	s.Propose(leader, []byte("before-crash"))
	if err := s.Run(50); err != nil {
		t.Fatalf("safety: %v", err)
	}
	s.Crash(leader)
	if err := s.Run(200); err != nil {
		t.Fatalf("safety: %v", err)
	}
	nl, ok := s.Leader()
	if !ok || nl == leader {
		t.Fatalf("expected new leader after crash, got %d ok=%v", nl, ok)
	}
	s.Propose(nl, []byte("after-crash"))
	if err := s.Run(100); err != nil {
		t.Fatalf("safety: %v", err)
	}
	s.Restart(leader)
	if err := s.Run(200); err != nil {
		t.Fatalf("safety: %v", err)
	}
	// Recovered node must have both commands.
	n := s.Nodes[leader]
	hasBefore, hasAfter := false, false
	for _, d := range n.FSM.Commands {
		if string(d) == "before-crash" {
			hasBefore = true
		}
		if string(d) == "after-crash" {
			hasAfter = true
		}
	}
	if !hasBefore || !hasAfter {
		t.Fatalf("recovered node missing data: before=%v after=%v applied=%d", hasBefore, hasAfter, n.FSM.Applied)
	}
}
