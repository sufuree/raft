package driver

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/wal"
)

type testFSM struct {
	mu      sync.Mutex
	applied raft.Index
	cmds    map[raft.Index][]byte
}

func newTestFSM() *testFSM { return &testFSM{cmds: make(map[raft.Index][]byte)} }

func (f *testFSM) Apply(entries []raft.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range entries {
		if e.Type == raft.EntryNormal {
			f.cmds[e.Index] = append([]byte(nil), e.Data...)
		}
		if e.Index > f.applied {
			f.applied = e.Index
		}
	}
	return nil
}

func (f *testFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(f.applied)
	enc.Encode(f.cmds)
	return buf.Bytes(), nil
}

func (f *testFSM) Restore(snap raft.SnapshotMeta, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = make(map[raft.Index][]byte)
	f.applied = snap.Index
	if len(data) > 0 {
		dec := gob.NewDecoder(bytes.NewReader(data))
		dec.Decode(&f.applied)
		dec.Decode(&f.cmds)
	}
	return nil
}

func (f *testFSM) has(data string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range f.cmds {
		if string(d) == data {
			return true
		}
	}
	return false
}

type testNode struct {
	d   *Driver
	fsm *testFSM
	dev *wal.MemDevice
}

func startCluster(t *testing.T, n int) ([]*testNode, *LoopNetwork) {
	t.Helper()
	net := NewLoopNetwork()
	cfg := raft.Configuration{Voters: make(raft.NodeSet, 0, n)}
	for i := 1; i <= n; i++ {
		cfg.Voters = append(cfg.Voters, raft.NodeID(i))
	}
	var nodes []*testNode
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		dev := wal.NewMemDevice()
		store, err := wal.Open(dev, wal.Options{ClusterID: 1, MemberID: uint64(id), InitialConfig: cfg})
		if err != nil {
			t.Fatalf("wal open: %v", err)
		}
		fsm := newTestFSM()
		tr := net.NewTransport(id)
		d, err := New(Config{
			ID: id, ElectionTick: 10, HeartbeatTick: 2, PreVote: true, CheckQuorum: true,
			Seed: int64(i) * 100, TickInterval: 2 * time.Millisecond,
		}, store, fsm, tr)
		if err != nil {
			t.Fatalf("driver new: %v", err)
		}
		d.Start()
		nodes = append(nodes, &testNode{d: d, fsm: fsm, dev: dev})
	}
	return nodes, net
}

func waitLeader(t *testing.T, nodes []*testNode) *testNode {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.d.IsLeader() {
				return n
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no leader elected")
	return nil
}

func TestClusterCommitRead(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.d.Stop()
		}
	}()
	leader := waitLeader(t, nodes)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := leader.d.Propose(ctx, []byte("hello"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if res.Outcome != raft.OutcomeApplied {
		t.Fatalf("expected applied, got %v", res.Outcome)
	}
	idx, err := leader.d.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if idx < res.Index {
		t.Fatalf("read index %d < proposal index %d", idx, res.Index)
	}
	// All nodes eventually apply.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, n := range nodes {
			if !n.fsm.has("hello") {
				all = false
			}
		}
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not all nodes applied the command")
}

func TestClusterFailover(t *testing.T) {
	nodes, _ := startCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.d.Stop()
		}
	}()
	leader := waitLeader(t, nodes)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := leader.d.Propose(ctx, []byte("before")); err != nil {
		t.Fatalf("propose: %v", err)
	}
	leader.d.Stop()
	// A new leader emerges from the remaining majority.
	var remaining []*testNode
	for _, n := range nodes {
		if n != leader {
			remaining = append(remaining, n)
		}
	}
	nl := waitLeader(t, remaining)
	if _, err := nl.d.Propose(ctx, []byte("after")); err != nil {
		t.Fatalf("propose after failover: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if nl.fsm.has("before") && nl.fsm.has("after") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("new leader missing committed data")
}

func TestProposalTooLarge(t *testing.T) {
	nodes, _ := startCluster(t, 1)
	defer nodes[0].d.Stop()
	waitLeader(t, nodes)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	big := make([]byte, nodes[0].d.cfg.MaxCommandBytes+1)
	if _, err := nodes[0].d.Propose(ctx, big); err != ErrTooLarge {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
}

// orderStorage detects concurrent or out-of-order persistence (§5: one
// ordered batch in flight per node).
type orderStorage struct {
	*raft.MemStorage
	busy     atomic.Int32
	violated atomic.Bool
	saves    atomic.Int64
}

func (s *orderStorage) Save(b raft.PersistBatch) error {
	if s.busy.Add(1) != 1 {
		s.violated.Store(true)
	}
	s.saves.Add(1)
	err := s.MemStorage.Save(b)
	s.busy.Add(-1)
	return err
}

func (s *orderStorage) MarkApplied(idx raft.Index) error {
	if s.busy.Add(1) != 1 {
		s.violated.Store(true)
	}
	err := s.MemStorage.MarkApplied(idx)
	s.busy.Add(-1)
	return err
}

// TestOrderedPersistence verifies storage operations never overlap, even
// under a proposal burst with concurrent apply workers.
func TestOrderedPersistence(t *testing.T) {
	// A single-node driver under a proposal burst maximizes overlap between
	// persistence and apply work; storage operations must never overlap.
	store := &orderStorage{MemStorage: raft.NewMemStorage()}
	store.SetInitialConfig(raft.Configuration{Voters: raft.NodeSet{1}})
	d, err := New(Config{ID: 1, ElectionTick: 5, HeartbeatTick: 1, TickInterval: time.Millisecond},
		store, newTestFSM(), NewLoopNetwork().NewTransport(1))
	if err != nil {
		t.Fatal(err)
	}
	d.Start()
	defer d.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for !d.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := d.Propose(ctx, []byte(fmt.Sprintf("burst-%d", i))); err != nil {
				t.Errorf("propose %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if store.violated.Load() {
		t.Fatal("concurrent storage operations detected; persistence must be ordered (§5)")
	}
	if store.saves.Load() < 200 {
		t.Fatalf("expected >=200 saves, got %d", store.saves.Load())
	}
}
