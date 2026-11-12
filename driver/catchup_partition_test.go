package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	raft "github.com/sufuree/raft"
)

// TestPartitionedFollowerCatchesUp: a follower partitioned for many election
// timeouts must catch up after the partition heals (RFC §11 progress gate: a
// lagging minority does not stop majority commit, and rejoins). Mirrors the
// P5 benchmark profile with in-memory storage so the assertion isolates the
// protocol from filesystem fsync latency (run with GOMAXPROCS=1 to match the
// single-vCPU target).
func TestPartitionedFollowerCatchesUp(t *testing.T) {
	net := NewLoopNetwork()
	voters := raft.Configuration{Voters: raft.NodeSet{1, 2, 3}}
	mk := func(id raft.NodeID) *Driver {
		store := raft.NewMemStorage()
		store.SetInitialConfig(voters)
		d, err := New(Config{
			ID: id, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true,
			Seed: int64(id) * 17, TickInterval: 5 * time.Millisecond,
		}, store, newTestFSM(), net.NewTransport(id))
		if err != nil {
			t.Fatal(err)
		}
		d.Start()
		return d
	}
	ds := []*Driver{mk(1), mk(2), mk(3)}
	defer func() {
		for _, d := range ds {
			d.Stop()
		}
	}()
	var leader *Driver
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, d := range ds {
			if d.IsLeader() {
				leader = d
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	propose := func(n int) {
		for i := 0; i < n; i++ {
			if _, err := leader.Propose(ctx, []byte("x")); err != nil {
				t.Fatalf("propose: %v", err)
			}
		}
	}
	propose(20) // warmup: all three caught up
	// Partition node 3 for ~30 election timeouts while the majority advances.
	net.SetDrop(func(from, to raft.NodeID) bool { return from == 3 || to == 3 })
	time.Sleep(2 * time.Second)
	propose(1500) // majority commits without node 3
	net.SetDrop(nil)
	// After healing, node 3 must catch up to the leader's applied index.
	_, _, _, _, apL := leader.Watermarks()
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, _, _, _, ap3 := ds[2].Watermarks()
		if ap3 >= apL {
			return // caught up
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, _, _, _, ap3 := ds[2].Watermarks()
	_, _, _, _, apL2 := leader.Watermarks()
	t.Fatalf("partitioned follower did not catch up: leader applied=%d follower applied=%d", apL2, ap3)
}

var _ = fmt.Sprintf
