package raft

import "testing"

func BenchmarkCoreTick(b *testing.B) {
	init := InitState{Config: Configuration{Voters: NodeSet{1, 2, 3}}, Epoch: 1}
	c := NewCore(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, Seed: 1}, init)
	ev := Event{Kind: EvTick}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Step(ev)
	}
}

func BenchmarkCorePropose(b *testing.B) {
	init := InitState{Config: Configuration{Voters: NodeSet{1}}, Epoch: 1}
	c := NewCore(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, Seed: 1}, init)
	// Become leader (single voter).
	for i := 0; i < 30; i++ {
		c.Step(Event{Kind: EvTick})
	}
	data := make([]byte, 256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := c.Step(Event{Kind: EvPropose, WorkID: uint64(i), Data: data})
		// Complete persistence so the core can keep admitting.
		for _, w := range out.Work {
			if w.Kind == WorkPersist {
				c.Step(Event{Kind: EvPersisted, Epoch: w.Epoch, WorkID: w.ID})
			}
		}
	}
}
