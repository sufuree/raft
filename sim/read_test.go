package sim

import (
	"testing"

	raft "github.com/sufuree/raft"
)

func TestReadBarrier(t *testing.T) {
	s := New(Config{Seed: 41, Nodes: 3, PreVote: true, CheckQuorum: true})
	leader := waitLeader(t, s, 200)
	s.Propose(leader, []byte("v1"))
	if err := s.Run(50); err != nil {
		t.Fatalf("safety: %v", err)
	}
	var idx raft.Index
	for i, d := range s.Nodes[leader].FSM.Commands {
		if string(d) == "v1" {
			idx = i
		}
	}
	s.Read(leader, []byte("r1"))
	if err := s.Run(50); err != nil {
		t.Fatalf("safety: %v", err)
	}
	found := false
	for _, h := range s.history {
		if h.Kind == "read" && h.Ok && h.Index >= idx {
			found = true
		}
	}
	if !found {
		t.Fatalf("no confirmed read at index >= %d", idx)
	}
}

func TestReadOnFollowerRejected(t *testing.T) {
	s := New(Config{Seed: 42, Nodes: 3, PreVote: true, CheckQuorum: true})
	leader := waitLeader(t, s, 200)
	var follower raft.NodeID
	for id := range s.Nodes {
		if id != leader {
			follower = id
			break
		}
	}
	s.Read(follower, []byte("rf"))
	if err := s.Run(10); err != nil {
		t.Fatalf("safety: %v", err)
	}
	rejected := false
	for _, h := range s.history {
		if h.Kind == "read" && h.Node == follower && !h.Ok {
			rejected = true
		}
	}
	if !rejected {
		t.Fatalf("follower read was not rejected")
	}
}
