package driver

import (
	"context"
	"sync"
	"testing"
	"time"

	raft "github.com/sufuree/raft"
)

type orderFSM struct {
	mu  sync.Mutex
	seq []raft.Index
}

func (f *orderFSM) Apply(es []raft.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range es {
		if e.Type == raft.EntryNormal {
			f.seq = append(f.seq, e.Index)
		}
	}
	return nil
}
func (f *orderFSM) Snapshot() ([]byte, error)                   { return nil, nil }
func (f *orderFSM) Restore(s raft.SnapshotMeta, d []byte) error { return nil }

// TestApplyIsOrdered: FSM application must be strictly ordered (§7). Pipelined
// proposals must not produce out-of-order Apply calls or premature AppliedIndex.
func TestApplyIsOrdered(t *testing.T) {
	store := raft.NewMemStorage()
	store.SetInitialConfig(raft.Configuration{Voters: raft.NodeSet{1}})
	fsm := &orderFSM{}
	d, err := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, TickInterval: time.Millisecond,
		MaxPendingProposals: 4096, MaxPendingBytes: 64 << 20},
		store, fsm, NewLoopNetwork().NewTransport(1))
	if err != nil {
		t.Fatal(err)
	}
	d.Start()
	defer d.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for !d.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 30; i++ {
				if _, err := d.Propose(ctx, []byte("x")); err != nil {
					t.Errorf("propose: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	for i := 1; i < len(fsm.seq); i++ {
		if fsm.seq[i] <= fsm.seq[i-1] {
			t.Fatalf("out-of-order apply at position %d: %d after %d (total %d)", i, fsm.seq[i], fsm.seq[i-1], len(fsm.seq))
		}
	}
}
