package sim

import (
	"fmt"
	"math/rand"
	"testing"

	raft "github.com/sufuree/raft"
)

// TestStress runs many seeded adversarial schedules and asserts the safety
// invariants every tick (RFC-0001 §11).
func TestStress(t *testing.T) {
	for seed := int64(1); seed <= 150; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed * 31))
			nodes := 3 + rng.Intn(3)*2 // 3 or 5
			s := New(Config{
				Seed: seed, Nodes: nodes, PreVote: true, CheckQuorum: true,
				ElectionTick: 8, HeartbeatTick: 1,
			})
			s.Net.DropRate = 0.10
			s.Net.DupRate = 0.10
			s.Net.MinDelay = 1
			s.Net.MaxDelay = 5
			ids := s.sortedIDs()
			for tick := 0; tick < 400; tick++ {
				// Random proposals to a live leader.
				if l, ok := s.Leader(); ok && rng.Intn(3) == 0 {
					s.Propose(l, []byte(fmt.Sprintf("s%d-t%d", seed, tick)))
				}
				// Random partition / heal.
				if rng.Intn(25) == 0 {
					groups := make(map[raft.NodeID]int)
					for _, id := range ids {
						groups[id] = rng.Intn(2)
					}
					s.Partition(groups)
				}
				if rng.Intn(15) == 0 {
					s.Heal()
				}
				// Random crash / restart.
				if rng.Intn(30) == 0 {
					id := ids[rng.Intn(len(ids))]
					if s.Nodes[id].Alive {
						s.Crash(id)
					} else {
						s.Restart(id)
					}
				}
				if err := s.Run(1); err != nil {
					t.Fatalf("seed %d tick %d: %v", seed, tick, err)
				}
			}
		})
	}
}
