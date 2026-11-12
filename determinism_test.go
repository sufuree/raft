package raft

import (
	"reflect"
	"testing"
)

// TestCoreDeterminism asserts identical inputs yield identical outputs (§5,§11).
func TestCoreDeterminism(t *testing.T) {
	run := func() []Outputs {
		init := InitState{Config: Configuration{Voters: NodeSet{1, 2, 3}}, Epoch: 1}
		c := NewCore(Config{ID: 1, ElectionTick: 5, HeartbeatTick: 1, PreVote: true, Seed: 99}, init)
		var outs []Outputs
		for i := 0; i < 40; i++ {
			outs = append(outs, c.Step(Event{Kind: EvTick}))
		}
		outs = append(outs, c.Step(Event{Kind: EvPropose, WorkID: 1, Data: []byte("x")}))
		for i := 0; i < 10; i++ {
			outs = append(outs, c.Step(Event{Kind: EvTick}))
		}
		return outs
	}
	a, b := run(), run()
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("core outputs differ across identical runs")
	}
}
