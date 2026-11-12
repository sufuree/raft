package main

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/driver"
	"github.com/sufuree/raft/wal"
)

// oursCluster runs the library's driver + WAL over the in-process loopback
// transport (loopback-class network, profiles.md). Each voter's durable state
// lives in its own subdirectory so P6 can restart one follower in place.
type oursCluster struct {
	dir   string
	k     knobs
	lat   *latCollector
	net   *driver.LoopNetwork
	nodes []*oursNode
}

type oursNode struct {
	id    raft.NodeID
	d     *driver.Driver
	store *wal.WAL
	dev   *wal.FileDevice
	fsm   *oursFSM
	dir   string
}

// oursFSM adapts the shared state machine to driver.FSM.
type oursFSM struct{ f *stateFSM }

func (a *oursFSM) Apply(es []raft.Entry) error {
	for _, e := range es {
		if e.Type == raft.EntryNormal && len(e.Data) > 0 {
			a.f.applyEntry(uint64(e.Index))
		} else {
			// No-ops (empty Data) and configuration entries advance the
			// applied watermark without changing application state.
			a.f.advance(uint64(e.Index))
		}
	}
	return nil
}
func (a *oursFSM) Snapshot() ([]byte, error) { return a.f.snapshot(), nil }
func (a *oursFSM) Restore(s raft.SnapshotMeta, data []byte) error {
	return a.f.restore(data, uint64(s.Index))
}

func oursWalOptions(id raft.NodeID, k knobs) wal.Options {
	o := wal.Options{
		ClusterID: 1, MemberID: uint64(id), Generation: 1,
		InitialConfig:    raft.Configuration{Voters: voterSet(k.voters)},
		MaxSnapshotBytes: maxInt(64<<20, 2*k.stateBytes),
	}
	if k.maxCommand > 1<<20 {
		// §8 limit chain: command + framing fits the batch; the batch fits a
		// segment (archive-class profiles).
		o.SegmentSize = 64 << 20
		o.MaxBatchBytes = 48 << 20
	}
	return o
}

func voterSet(n int) raft.NodeSet {
	s := make(raft.NodeSet, 0, n)
	for i := 1; i <= n; i++ {
		s = append(s, raft.NodeID(i))
	}
	return s
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func newOursNode(id raft.NodeID, dir string, k knobs, net *driver.LoopNetwork, lat *latCollector) (*oursNode, error) {
	ndir := filepath.Join(dir, fmt.Sprintf("n%d", id))
	if err := mkdir(ndir); err != nil {
		return nil, err
	}
	dev, err := wal.OpenFile(filepath.Join(ndir, "raft.wal"))
	if err != nil {
		return nil, err
	}
	store, err := wal.Open(dev, oursWalOptions(id, k))
	if err != nil {
		return nil, err
	}
	fsm := &oursFSM{f: newStateFSM(k.stateBytes)}
	// Consumer-side FSM reconstruction (RFC §7): an in-memory FSM rebuilds
	// from the durable snapshot plus committed replay through the persisted
	// applied watermark before serving. The core does not re-dispatch entries
	// at or below AppliedIndex (dispatchedIndex starts there).
	if err := reconstructFSM(store, fsm.f); err != nil {
		return nil, err
	}
	batch := 8 << 20
	if k.maxCommand > 1<<20 {
		batch = 48 << 20
	}
	d, err := driver.New(driver.Config{
		ID: id, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true,
		Seed: int64(id) * 17, TickInterval: 5 * time.Millisecond,
		MaxCommandBytes: k.maxCommand, MaxBatchBytes: batch,
		MaxPendingBytes:  4*k.maxCommand + (1 << 20),
		MaxSnapshotBytes: maxInt(64<<20, 2*k.stateBytes),
		// Staging on the cell's durable directory, never tmpfs (§6.4).
		StagingDir: filepath.Join(ndir, "staging"),
		OnStore:    func(op string, d time.Duration) { lat.addStore(d) },
		OnApply:    func(n int, d time.Duration) { lat.addApply(d) },
	}, store, fsm, net.NewTransport(id))
	if err != nil {
		return nil, err
	}
	d.Start()
	return &oursNode{id: id, d: d, store: store, dev: dev, fsm: fsm, dir: ndir}, nil
}

func newOursCluster(dir string, k knobs, lat *latCollector) (cluster, error) {
	c := &oursCluster{dir: dir, k: k, lat: lat, net: driver.NewLoopNetwork()}
	for i := 1; i <= k.voters; i++ {
		n, err := newOursNode(raft.NodeID(i), dir, k, c.net, lat)
		if err != nil {
			return nil, err
		}
		c.nodes = append(c.nodes, n)
	}
	if _, err := c.leader(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *oursCluster) leader() (node, error) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n.d.IsLeader() {
				return n, nil
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil, fmt.Errorf("no leader")
}

func (c *oursCluster) stall(id int) {
	c.net.SetDrop(func(from, to raft.NodeID) bool {
		return from == raft.NodeID(id) || to == raft.NodeID(id)
	})
}
func (c *oursCluster) unstall(id int) { c.net.SetDrop(nil) }

func (c *oursCluster) restart(id int) error {
	old := c.nodes[id-1]
	old.d.Stop()
	old.store.Close()
	old.dev.Close()
	n, err := newOursNode(raft.NodeID(id), c.dir, c.k, c.net, c.lat)
	if err != nil {
		return err
	}
	c.nodes[id-1] = n
	return nil
}

func (c *oursCluster) applied(id int) uint64  { return c.nodes[id-1].fsm.f.appliedIndex() }
func (c *oursCluster) checksum(id int) uint64 { return c.nodes[id-1].fsm.f.checksum() }
func (c *oursCluster) stateCRC(id int) uint32 { return c.nodes[id-1].fsm.f.bufferCRC() }

// snapshot triggers a leader snapshot and waits until the WAL has published
// it (the authoritative publication, §6.4 — not merely the request).
func (c *oursCluster) snapshot() error {
	n, err := c.leader()
	if err != nil {
		return err
	}
	on := n.(*oursNode)
	target := on.fsm.f.appliedIndex()
	on.d.Snapshot()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		s, err := on.store.ReadSnapshot()
		if err == nil && uint64(s.Index) >= target {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("snapshot not published (target %d)", target)
}

func (c *oursCluster) shutdown() {
	for _, n := range c.nodes {
		n.d.Stop()
		n.store.Close()
		n.dev.Close()
	}
}

func (n *oursNode) propose(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := n.d.Propose(ctx, data)
	return err
}

func (n *oursNode) barrier() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := n.d.Read(ctx)
	return err
}
