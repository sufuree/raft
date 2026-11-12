package main

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// hcCluster is the incumbent composition: HashiCorp Raft v1.8 + raft-boltdb
// (bbolt) log/stable stores + file snapshots, in-memory transports connected
// in a full mesh (the standard multi-node setup, profiles.md). TrailingLogs
// is reduced from the 8192 default to 128 so P6 compaction follows a snapshot
// without seeding >8192 entries; this changes replay retention, not the
// acknowledged-durability contract.
type hcCluster struct {
	dir   string
	k     knobs
	lat   *latCollector
	nodes []*hcNode
	trs   []*hraft.InmemTransport
	addrs []hraft.ServerAddress
}

type hcNode struct {
	r     *hraft.Raft
	store *raftboltdb.BoltStore
	snaps *hraft.FileSnapshotStore
	fsm   *hcFSM
	dir   string
	id    int
}

type hcFSM struct {
	f   *stateFSM
	lat *latCollector
}

func (f *hcFSM) Apply(l *hraft.Log) interface{} {
	t0 := time.Now()
	if l.Type == hraft.LogCommand {
		f.f.applyEntry(l.Index)
	} else {
		f.f.advance(l.Index)
	}
	f.lat.addApply(time.Since(t0))
	return nil
}
func (f *hcFSM) Snapshot() (hraft.FSMSnapshot, error) {
	return &hcSnap{data: f.f.snapshot()}, nil
}
func (f *hcFSM) Restore(rc io.ReadCloser) error {
	data, err := readAll(rc)
	rc.Close()
	if err != nil {
		return err
	}
	return f.f.restore(data, 0)
}

type hcSnap struct{ data []byte }

func (s *hcSnap) Persist(sink hraft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}
func (s *hcSnap) Release() {}

func hcConfig(id int) *hraft.Config {
	cfg := hraft.DefaultConfig()
	cfg.LocalID = hraft.ServerID(fmt.Sprintf("n%d", id))
	// Comparable tick cadence to the library profile.
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.TrailingLogs = 128
	cfg.ShutdownOnRemove = false
	return cfg
}

func newHCNode(id int, dir string, k knobs, lat *latCollector) (*hcNode, *hraft.InmemTransport, error) {
	hdir := filepath.Join(dir, fmt.Sprintf("hc%d", id))
	if err := mkdir(hdir); err != nil {
		return nil, nil, err
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(hdir, "raft.db"))
	if err != nil {
		return nil, nil, err
	}
	snaps, err := hraft.NewFileSnapshotStore(hdir, 1, io.Discard)
	if err != nil {
		return nil, nil, err
	}
	addr, tr := hraft.NewInmemTransport(hraft.ServerAddress(fmt.Sprintf("n%d", id)))
	_ = addr
	fsm := &hcFSM{f: newStateFSM(k.stateBytes), lat: lat}
	r, err := hraft.NewRaft(hcConfig(id), fsm, store, store, snaps, tr)
	if err != nil {
		return nil, nil, err
	}
	return &hcNode{r: r, store: store, snaps: snaps, fsm: fsm, dir: hdir, id: id}, tr, nil
}

func (c *hcCluster) mesh() {
	for i := range c.trs {
		for j := range c.trs {
			if i != j {
				c.trs[i].Connect(c.addrs[j], c.trs[j])
			}
		}
	}
}

func newHCCluster(dir string, k knobs, lat *latCollector) (cluster, error) {
	c := &hcCluster{dir: dir, k: k, lat: lat}
	for i := 1; i <= k.voters; i++ {
		n, tr, err := newHCNode(i, dir, k, lat)
		if err != nil {
			return nil, err
		}
		c.nodes = append(c.nodes, n)
		c.trs = append(c.trs, tr)
		c.addrs = append(c.addrs, hraft.ServerAddress(fmt.Sprintf("n%d", i)))
	}
	c.mesh()
	// Bootstrap once, from node 1, unless durable state already exists.
	hasState, err := hraft.HasExistingState(c.nodes[0].store, c.nodes[0].store, c.nodes[0].snaps)
	if err != nil {
		return nil, err
	}
	if !hasState {
		var servers []hraft.Server
		for i := 1; i <= k.voters; i++ {
			servers = append(servers, hraft.Server{
				ID: hraft.ServerID(fmt.Sprintf("n%d", i)), Address: c.addrs[i-1], Suffrage: hraft.Voter,
			})
		}
		if err := c.nodes[0].r.BootstrapCluster(hraft.Configuration{Servers: servers}).Error(); err != nil {
			return nil, err
		}
	}
	if _, err := c.leader(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *hcCluster) leader() (node, error) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n.r.State() == hraft.Leader {
				return n, nil
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil, fmt.Errorf("no leader")
}

func (c *hcCluster) stall(id int) {
	for j := range c.trs {
		if j != id-1 {
			c.trs[j].Disconnect(c.addrs[id-1])
			c.trs[id-1].Disconnect(c.addrs[j])
		}
	}
}

func (c *hcCluster) unstall(id int) {
	for j := range c.trs {
		if j != id-1 {
			c.trs[j].Connect(c.addrs[id-1], c.trs[id-1])
			c.trs[id-1].Connect(c.addrs[j], c.trs[j])
		}
	}
}

func (c *hcCluster) restart(id int) error {
	old := c.nodes[id-1]
	if err := old.r.Shutdown().Error(); err != nil {
		return err
	}
	old.store.Close()
	n, tr, err := newHCNode(id, c.dir, c.k, c.lat)
	if err != nil {
		return err
	}
	c.nodes[id-1] = n
	c.trs[id-1] = tr
	c.mesh()
	return nil
}

func (c *hcCluster) applied(id int) uint64  { return c.nodes[id-1].fsm.f.appliedIndex() }
func (c *hcCluster) checksum(id int) uint64 { return c.nodes[id-1].fsm.f.checksum() }
func (c *hcCluster) stateCRC(id int) uint32 { return c.nodes[id-1].fsm.f.bufferCRC() }
func (c *hcCluster) diagnose(id int) string {
	n := c.nodes[id-1]
	return fmt.Sprintf("n%d state=%v term=%d leader=%s applied=%d", id, n.r.State(), n.r.Stats()["term"], n.r.Stats()["last_leader"], n.fsm.f.appliedIndex())
}

func (c *hcCluster) snapshot() error {
	n, err := c.leader()
	if err != nil {
		return err
	}
	hn := n.(*hcNode)
	target := hn.fsm.f.appliedIndex()
	if err := hn.r.Snapshot().Error(); err != nil {
		return err
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		list, err := hn.snaps.List()
		if err == nil && len(list) > 0 && list[0].Index >= target {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("snapshot not published (target %d)", target)
}

func (c *hcCluster) shutdown() {
	for _, n := range c.nodes {
		n.r.Shutdown().Error()
		n.store.Close()
	}
}

func (n *hcNode) propose(data []byte) error {
	f := n.r.Apply(data, 30*time.Second)
	if err := f.Error(); err != nil {
		return err
	}
	if resp, ok := f.Response().(error); ok && resp != nil {
		return resp
	}
	return nil
}

func (n *hcNode) barrier() error {
	return n.r.Barrier(30 * time.Second).Error()
}
