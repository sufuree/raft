package sim

import (
	"fmt"
	"math/rand"
	"testing"

	raft "github.com/sufuree/raft"
)

// TestCheckerSanity verifies the checker accepts linearizable histories and
// rejects non-linearizable ones.
func TestCheckerSanity(t *testing.T) {
	// w(a) then r(a): linearizable.
	ok := []LinOp{
		{Write: true, Value: "a", Start: 0, End: 10},
		{Write: false, Value: "a", Start: 11, End: 20},
	}
	if !CheckLinearizable(ok) {
		t.Fatal("linearizable history rejected")
	}
	// r(b) concurrent with w(a), but r returns before w invokes and sees b
	// that was never written: not linearizable.
	bad := []LinOp{
		{Write: false, Value: "b", Start: 0, End: 5},
		{Write: true, Value: "a", Start: 6, End: 10},
	}
	if CheckLinearizable(bad) {
		t.Fatal("non-linearizable history accepted")
	}
	// Stale read: w(a) completes, then r("") — not linearizable.
	stale := []LinOp{
		{Write: true, Value: "a", Start: 0, End: 5},
		{Write: false, Value: "", Start: 6, End: 10},
	}
	if CheckLinearizable(stale) {
		t.Fatal("stale read accepted")
	}
	// Concurrent writes, read sees one of them: linearizable.
	conc := []LinOp{
		{Write: true, Value: "a", Start: 0, End: 10},
		{Write: true, Value: "b", Start: 1, End: 9},
		{Write: false, Value: "b", Start: 2, End: 8},
	}
	if !CheckLinearizable(conc) {
		t.Fatal("concurrent history rejected")
	}
	// Optional write that never took effect: read of "" is fine.
	opt := []LinOp{
		{Write: true, Value: "a", Start: 0, End: -1, Optional: true},
		{Write: false, Value: "", Start: 1, End: 10},
	}
	if !CheckLinearizable(opt) {
		t.Fatal("optional-write history rejected")
	}
	// Optional write that must have taken effect (read sees its value).
	opt2 := []LinOp{
		{Write: true, Value: "a", Start: 0, End: -1, Optional: true},
		{Write: false, Value: "a", Start: 1, End: 10},
	}
	if !CheckLinearizable(opt2) {
		t.Fatal("optional-write applied history rejected")
	}
}

// TestLinearizabilityUnderAdversity records client histories across
// partitions, crashes, restarts, and leader changes, then checks them
// against the sequential register specification (§11 reads/application and
// client-uncertainty gates).
func TestLinearizabilityUnderAdversity(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		rng := rand.New(rand.NewSource(seed * 101))
		s := New(Config{Seed: seed, Nodes: 3, PreVote: true, CheckQuorum: true,
			ElectionTick: 8, HeartbeatTick: 1})
		s.Net.DropRate = 0.05
		s.Net.MinDelay = 1
		s.Net.MaxDelay = 3
		ids := s.sortedIDs()
		if err := s.Run(20); err != nil {
			t.Fatal(err)
		}
		for step := 0; step < 60; step++ {
			if l, ok := s.Leader(); ok {
				switch rng.Intn(3) {
				case 0:
					s.Propose(l, []byte(fmt.Sprintf("v%d-%d", seed, step)))
				case 1:
					// Read on a random node (followers reject; excluded).
					s.Read(ids[rng.Intn(len(ids))], []byte(fmt.Sprintf("r%d-%d", seed, step)))
				case 2:
					s.Propose(l, []byte(fmt.Sprintf("w%d-%d", seed, step)))
				}
			}
			if rng.Intn(20) == 0 {
				groups := make(map[raft.NodeID]int)
				for _, id := range ids {
					groups[id] = rng.Intn(2)
				}
				s.Partition(groups)
			}
			if rng.Intn(10) == 0 {
				s.Heal()
			}
			if rng.Intn(25) == 0 {
				id := ids[rng.Intn(len(ids))]
				if s.Nodes[id].Alive {
					s.Crash(id)
				} else {
					s.Restart(id)
				}
			}
			if err := s.Run(2); err != nil {
				t.Fatalf("seed %d step %d safety: %v", seed, step, err)
			}
		}
		s.Heal()
		for _, id := range ids {
			if !s.Nodes[id].Alive {
				s.Restart(id)
			}
		}
		if err := s.Run(100); err != nil {
			t.Fatalf("seed %d settle: %v", seed, err)
		}
		ops := HistoryOps(s.History())
		if !CheckLinearizable(ops) {
			t.Fatalf("seed %d: history not linearizable (%d ops)", seed, len(ops))
		}
	}
}

// TestLinearizabilityReadAfterWrite: a read invoked after a write's applied
// return must observe that write (or a later one).
func TestLinearizabilityReadAfterWrite(t *testing.T) {
	for seed := int64(1); seed <= 10; seed++ {
		s := New(Config{Seed: seed, Nodes: 3, PreVote: true, CheckQuorum: true})
		if err := s.Run(20); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 15; i++ {
			l, ok := s.Leader()
			if !ok {
				if err := s.Run(10); err != nil {
					t.Fatal(err)
				}
				continue
			}
			val := fmt.Sprintf("seq-%d-%d", seed, i)
			s.Propose(l, []byte(val))
			if err := s.Run(15); err != nil {
				t.Fatal(err)
			}
			s.Read(l, []byte(fmt.Sprintf("ctx-%d-%d", seed, i)))
			if err := s.Run(15); err != nil {
				t.Fatal(err)
			}
		}
		ops := HistoryOps(s.History())
		if !CheckLinearizable(ops) {
			t.Fatalf("seed %d: read-after-write history not linearizable", seed)
		}
	}
}
