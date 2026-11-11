package driver

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// stallFSM blocks application so read barriers accumulate waiters.
type stallFSM struct {
	testFSM
	gate chan struct{}
}

func (f *stallFSM) Apply(entries []raft.Entry) error {
	<-f.gate
	return f.testFSM.Apply(entries)
}

// TestReadWaiterCap: concurrent read barriers are bounded; excess callers
// receive typed overload (§8).
func TestReadWaiterCap(t *testing.T) {
	store := raft.NewMemStorage()
	store.SetInitialConfig(raft.Configuration{Voters: raft.NodeSet{1}})
	fsm := &stallFSM{testFSM: testFSM{cmds: make(map[raft.Index][]byte)}, gate: make(chan struct{})}
	d, err := New(Config{ID: 1, ElectionTick: 5, HeartbeatTick: 1, TickInterval: time.Millisecond,
		MaxReadWaiters: 2}, store, fsm, NewLoopNetwork().NewTransport(1))
	if err != nil {
		t.Fatal(err)
	}
	d.Start()
	defer d.Stop()
	gateClosed := false
	defer func() {
		if !gateClosed {
			close(fsm.gate)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !d.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !d.IsLeader() {
		t.Fatal("no leader")
	}
	// The stalled FSM keeps applied below the barrier index, so reads wait.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Read(ctx) // outcome irrelevant; these occupy the waiter slots
		}()
	}
	// Wait for both waiters to register.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		n := len(d.readWait)
		d.mu.Unlock()
		if n == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := d.Read(ctx); !errors.Is(err, ErrOverload) {
		t.Fatalf("want ErrOverload beyond waiter cap, got %v", err)
	}
	gateClosed = true
	close(fsm.gate) // release; waiters drain
	wg.Wait()
}

// TestLimitChainValidation: command plus framing must fit the append batch
// (§8); an incompatible configuration is refused at construction.
func TestLimitChainValidation(t *testing.T) {
	store := raft.NewMemStorage()
	store.SetInitialConfig(raft.Configuration{Voters: raft.NodeSet{1}})
	_, err := New(Config{ID: 1, MaxCommandBytes: 8 << 20, MaxBatchBytes: 1 << 20},
		store, newTestFSM(), NewLoopNetwork().NewTransport(1))
	if !errors.Is(err, ErrLimits) {
		t.Fatalf("want ErrLimits, got %v", err)
	}
}

// TestSingleNodeReadBarrier: a single-voter cluster completes read barriers
// with no peers to acknowledge (regression: the confirmation round was never
// marked confirmed when self alone formed the quorum).
func TestSingleNodeReadBarrier(t *testing.T) {
	store := raft.NewMemStorage()
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := d.Propose(ctx, []byte("x")); err != nil {
		t.Fatal(err)
	}
	idx, err := d.Read(ctx)
	if err != nil {
		t.Fatalf("single-voter read barrier failed: %v", err)
	}
	if idx < 2 {
		t.Fatalf("read index %d too low", idx)
	}
}

// plainStore hides Preflight so writes proceed to an actual ENOSPC, testing
// the terminal failure path rather than admission rejection.
type plainStore struct{ s raft.Storage }

func (p plainStore) Load() (raft.InitState, error)  { return p.s.Load() }
func (p plainStore) Save(b raft.PersistBatch) error { return p.s.Save(b) }
func (p plainStore) ReadEntries(lo, hi raft.Index) ([]raft.Entry, error) {
	return p.s.ReadEntries(lo, hi)
}
func (p plainStore) ReadSnapshot() (raft.SnapshotMeta, error) { return p.s.ReadSnapshot() }
func (p plainStore) MarkApplied(i raft.Index) error           { return p.s.MarkApplied(i) }
func (p plainStore) Close() error                             { return p.s.Close() }

// TestDiskFullIsTerminal: a failed durable write (ENOSPC) enters terminal
// Failed and stops success output (§6.2, §11 persistence gate).
func TestDiskFullIsTerminal(t *testing.T) {
	backing := wal.NewMemDevice()
	backing.WriteAt(make([]byte, 128<<10), 0)
	backing.Sync()
	dev, err := wal.NewFixedRegion(backing, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	store, err := wal.Open(dev, wal.Options{
		ClusterID: 1, MemberID: 1, Generation: 1,
		SegmentSize: 16 << 10, MaxBatchBytes: 8 << 10, MaxDeviceBytes: 128 << 10,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	d, err := New(Config{ID: 1, ElectionTick: 5, HeartbeatTick: 1, TickInterval: time.Millisecond},
		plainStore{store}, newTestFSM(), NewLoopNetwork().NewTransport(1))
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
	payload := bytes.Repeat([]byte("x"), 4<<10)
	failed := false
	for i := 0; i < 200; i++ {
		_, err := d.Propose(ctx, payload)
		if err != nil {
			// ErrOutcomeUnknown is the conservative report for proposals
			// admitted when the node failed (§6.2).
			if errors.Is(err, ErrFailed) || errors.Is(err, ErrOverload) || errors.Is(err, ErrOutcomeUnknown) {
				failed = true
				break
			}
			t.Fatalf("unexpected error: %v", err)
		}
		if d.Role() == raft.Failed {
			failed = true
			break
		}
	}
	if !failed {
		t.Fatal("disk never filled")
	}
	// Allow the failure event to propagate.
	deadline = time.Now().Add(2 * time.Second)
	for d.Role() != raft.Failed && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if d.Role() != raft.Failed {
		t.Fatalf("expected terminal Failed, got %v", d.Role())
	}
	if _, err := d.Propose(ctx, []byte("after")); !errors.Is(err, ErrFailed) {
		t.Fatalf("admission after failure: %v", err)
	}
	if _, err := d.Read(ctx); !errors.Is(err, ErrFailed) {
		t.Fatalf("read after failure: %v", err)
	}
}

// TestPreflightRejectsAdmission: with the WAL's Preflight visible, admission
// is rejected with typed overload before exhaustion, preserving the recovery
// reserve (§8).
func TestPreflightRejectsAdmission(t *testing.T) {
	backing := wal.NewMemDevice()
	backing.WriteAt(make([]byte, 128<<10), 0)
	backing.Sync()
	dev, _ := wal.NewFixedRegion(backing, 128<<10)
	store, err := wal.Open(dev, wal.Options{
		ClusterID: 1, MemberID: 1, Generation: 1,
		SegmentSize: 16 << 10, MaxBatchBytes: 8 << 10, MaxDeviceBytes: 128 << 10,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{1}},
	})
	if err != nil {
		t.Fatal(err)
	}
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
	// Fill the device directly until the reserve is exhausted.
	for i := 1; ; i++ {
		c := raft.Index(i)
		err := store.Save(raft.PersistBatch{
			Entries: []raft.Entry{{Term: 1, Index: raft.Index(i), Data: make([]byte, 4<<10)}},
			Commit:  &c,
		})
		if err != nil {
			break
		}
		if store.Preflight(4<<10) != nil {
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := d.Propose(ctx, make([]byte, 4<<10)); !errors.Is(err, ErrOverload) {
		t.Fatalf("want ErrOverload from preflight, got %v", err)
	}
	if d.Role() == raft.Failed {
		t.Fatal("preflight rejection must not fail the node")
	}
}

// TestLargeSnapshotStreamedCatchUp exercises the full streaming chain: core
// chunking, driver staging files with end-to-end CRC, WAL chunked
// publication, and recovery (§6.4).
func TestLargeSnapshotStreamedCatchUp(t *testing.T) {
	net := NewLoopNetwork()
	voters := raft.Configuration{Voters: raft.NodeSet{1, 2, 3}}
	stagingDir := t.TempDir()
	type node struct {
		d     *Driver
		fsm   *testFSM
		dev   *wal.MemDevice
		store *wal.WAL
	}
	mkNode := func(id raft.NodeID) *node {
		dev := wal.NewMemDevice()
		store, err := wal.Open(dev, wal.Options{
			ClusterID: 1, MemberID: uint64(id), InitialConfig: voters,
			SegmentSize: 64 << 10, MaxBatchBytes: 32 << 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		fsm := newTestFSM()
		d, err := New(Config{
			ID: id, ElectionTick: 10, HeartbeatTick: 2, PreVote: true, CheckQuorum: true,
			Seed: int64(id) * 100, TickInterval: time.Millisecond,
			SnapshotChunkBytes: 4 << 10, StagingDir: stagingDir,
			MaxBatchBytes: 32 << 10, MaxCommandBytes: 16 << 10,
		}, store, fsm, net.NewTransport(id))
		if err != nil {
			t.Fatal(err)
		}
		d.Start()
		return &node{d: d, fsm: fsm, dev: dev, store: store}
	}
	nodes := []*node{mkNode(1), mkNode(2), mkNode(3)}
	defer func() {
		for _, n := range nodes {
			n.d.Stop()
		}
	}()
	var leader *node
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.d.IsLeader() {
				leader = n
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
	payload := bytes.Repeat([]byte("z"), 2<<10)
	for i := 0; i < 100; i++ {
		if _, err := leader.d.Propose(ctx, payload); err != nil {
			state := ""
			for j, n := range nodes {
				du, co, pe, di, ap := n.d.Watermarks()
				state += fmt.Sprintf("n%d=%v/t%d/leader%d/wm[%d,%d,%d,%d,%d]/fail%q ", j+1, n.d.Role(), n.d.Term(), n.d.Leader(), du, co, pe, di, ap, n.d.FailReason())
			}
			t.Fatalf("propose %d: %v; state=[%s]", i, err, state)
		}
	}
	// Stop a follower, then advance and compact the leader's history well
	// past the follower's match so catch-up requires a snapshot.
	lag := nodes[2]
	if lag == leader {
		lag = nodes[1]
	}
	lag.d.Stop()
	for i := 0; i < 50; i++ {
		if _, err := leader.d.Propose(ctx, payload); err != nil {
			state := ""
			for j, n := range nodes {
				du, co, pe, di, ap := n.d.Watermarks()
				state += fmt.Sprintf("n%d=%v/t%d/leader%d/wm[%d,%d,%d,%d,%d]/fail%q ", j+1, n.d.Role(), n.d.Term(), n.d.Leader(), du, co, pe, di, ap, n.d.FailReason())
			}
			leaderIdx, lagIdx := 0, 0
			for j, n := range nodes {
				if n == leader {
					leaderIdx = j + 1
				}
				if n == lag {
					lagIdx = j + 1
				}
			}
			t.Fatalf("post-stop propose %d: %v; leader=n%d lag=n%d state=[%s]", i, err, leaderIdx, lagIdx, state)
		}
	}
	leader.d.Snapshot()
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap, _ := leader.store.ReadSnapshot()
		if snap.Index >= 100 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if snap, _ := leader.store.ReadSnapshot(); snap.Index < 100 {
		t.Fatalf("leader snapshot not published: %d", snap.Index)
	}
	// Restart the lagging follower on its durable device.
	lag.store = nil
	dev := lag.dev
	store, err := wal.Open(dev, wal.Options{
		ClusterID: 1, MemberID: uint64(lag.d.cfg.ID), InitialConfig: voters,
		SegmentSize: 64 << 10, MaxBatchBytes: 32 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	fsm := newTestFSM()
	d, err := New(Config{
		ID: lag.d.cfg.ID, ElectionTick: 10, HeartbeatTick: 2, PreVote: true, CheckQuorum: true,
		Seed: 999, TickInterval: time.Millisecond,
		SnapshotChunkBytes: 4 << 10, StagingDir: stagingDir,
		MaxBatchBytes: 32 << 10, MaxCommandBytes: 16 << 10,
	}, store, fsm, net.NewTransport(lag.d.cfg.ID))
	if err != nil {
		t.Fatal(err)
	}
	lag.d = d
	lag.fsm = fsm
	lag.store = store
	d.Start()
	// Catch-up must complete through the streamed snapshot.
	deadline = time.Now().Add(30 * time.Second)
	caught := false
	for time.Now().Before(deadline) {
		_, _, _, _, apL := leader.d.Watermarks()
		_, _, _, _, apF := lag.d.Watermarks()
		if apF >= apL && apL >= 150 {
			caught = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !caught {
		_, _, _, _, apL := leader.d.Watermarks()
		_, _, _, _, apF := lag.d.Watermarks()
		matches, _ := filepath.Glob(filepath.Join(stagingDir, "*.staging"))
		var sizes []int64
		for _, m := range matches {
			fi, _ := os.Stat(m)
			if fi != nil {
				sizes = append(sizes, fi.Size())
			}
		}
		lsnap, _ := leader.store.ReadSnapshot()
		roles := ""
		for i, n := range nodes {
			roles += fmt.Sprintf("n%d=%v/leader%d ", i+1, n.d.Role(), n.d.Leader())
		}
		reasons := ""
		for i, n := range nodes {
			if r := n.d.FailReason(); r != "" {
				reasons += fmt.Sprintf("n%d:%q ", i+1, r)
			}
		}
		t.Fatalf("streamed catch-up failed: leader %d follower %d leaderSnap=%d staging=%v sizes=%v roles=[%s] reasons=[%s]",
			apL, apF, lsnap.Index, matches, sizes, roles, reasons)
	}
	fs, _ := lag.store.ReadSnapshot()
	if fs.Index == 0 || len(fs.Data) < 100<<10 {
		t.Fatalf("follower snapshot missing or small: index %d bytes %d", fs.Index, len(fs.Data))
	}
	// Staging files are removed after a successful install.
	if matches, _ := filepath.Glob(filepath.Join(stagingDir, "*.staging")); len(matches) != 0 {
		t.Fatalf("staging files left behind: %v", matches)
	}
	// The restarted follower recovers its streamed snapshot from the WAL.
	d.Stop()
	store2, err := wal.Open(dev, wal.Options{
		ClusterID: 1, MemberID: uint64(d.cfg.ID), InitialConfig: voters,
		SegmentSize: 64 << 10, MaxBatchBytes: 32 << 10,
	})
	if err != nil {
		t.Fatalf("reopen after streamed install: %v", err)
	}
	st, _ := store2.Load()
	if st.Snapshot.Index != fs.Index || len(st.Snapshot.Data) != len(fs.Data) {
		t.Fatalf("streamed snapshot not recoverable: %d vs %d", st.Snapshot.Index, fs.Index)
	}
	store2.Close()
}
