package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"sync"
	"time"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/wal"
	etcdraft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// etcdStorage backs etcd Raft with the same WAL used by the library, so all
// compositions acknowledge the same durability (fsync per batch). The bench
// never changes membership, so the configured voter set is the confstate
// fallback until a snapshot carries one.
type etcdStorage struct {
	mu     sync.Mutex
	w      *wal.WAL
	snap   raft.SnapshotMeta
	hs     raft.HardState
	last   raft.Index
	voters raft.NodeSet
}

func newEtcdStorage(dir string, id uint64, k knobs) (*etcdStorage, error) {
	ndir := filepath.Join(dir, fmt.Sprintf("etcd%d", id))
	if err := mkdir(ndir); err != nil {
		return nil, err
	}
	dev, err := wal.OpenFile(filepath.Join(ndir, "raft.wal"))
	if err != nil {
		return nil, err
	}
	o := wal.Options{
		ClusterID: 2, MemberID: id, Generation: 1,
		InitialConfig:    raft.Configuration{Voters: voterSet(k.voters)},
		MaxSnapshotBytes: maxInt(64<<20, 2*k.stateBytes),
		// etcd coalesces entries into multi-MB Ready batches; segments must be
		// large enough to hold one batch (the WAL's §8 budget rejects a record
		// larger than a segment). MaxSizePerMsg is 8 MiB (48 MiB for archive).
		SegmentSize: 16 << 20,
	}
	if k.maxCommand > 1<<20 {
		o.SegmentSize = 64 << 20
		o.MaxBatchBytes = 48 << 20
	}
	w, err := wal.Open(dev, o)
	if err != nil {
		return nil, err
	}
	st, err := w.Load()
	if err != nil {
		return nil, err
	}
	return &etcdStorage{w: w, snap: st.Snapshot, hs: st.HardState, last: st.DurableIndex, voters: voterSet(k.voters)}, nil
}

func (s *etcdStorage) confState() *pb.ConfState {
	cs := pb.ConfState{}
	voters := s.snap.Config.Voters
	if len(voters) == 0 {
		voters = s.voters
	}
	for _, v := range voters {
		cs.Voters = append(cs.Voters, uint64(v))
	}
	return &cs
}

func (s *etcdStorage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.HardState{Term: proto.Uint64(uint64(s.hs.Term)), Vote: proto.Uint64(uint64(s.hs.Vote)), Commit: proto.Uint64(uint64(s.hs.Commit))}, s.confState(), nil
}

func (s *etcdStorage) Entries(lo, hi, maxSize uint64) ([]*pb.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lo <= uint64(s.snap.Index) {
		return nil, etcdraft.ErrCompacted
	}
	ents, err := s.w.ReadEntries(raft.Index(lo), raft.Index(hi))
	if err != nil {
		return nil, err
	}
	var out []*pb.Entry
	var size uint64
	for _, e := range ents {
		pe := &pb.Entry{Term: proto.Uint64(uint64(e.Term)), Index: proto.Uint64(uint64(e.Index)), Data: e.Data}
		if e.Type == raft.EntryConfChange {
			t := pb.EntryConfChange
			pe.Type = &t
		}
		size += uint64(proto.Size(pe))
		if maxSize > 0 && size > maxSize && len(out) > 0 {
			break
		}
		out = append(out, pe)
	}
	return out, nil
}

func (s *etcdStorage) Term(i uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < uint64(s.snap.Index) {
		return 0, etcdraft.ErrCompacted
	}
	if i == uint64(s.snap.Index) {
		return uint64(s.snap.Term), nil
	}
	ents, err := s.w.ReadEntries(raft.Index(i), raft.Index(i+1))
	if err != nil || len(ents) == 0 {
		return 0, etcdraft.ErrUnavailable
	}
	return uint64(ents[0].Term), nil
}

func (s *etcdStorage) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(s.last), nil
}

func (s *etcdStorage) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(s.snap.Index) + 1, nil
}

func (s *etcdStorage) Snapshot() (*pb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.Snapshot{
		Data: s.snap.Data,
		Metadata: &pb.SnapshotMetadata{
			Index: proto.Uint64(uint64(s.snap.Index)), Term: proto.Uint64(uint64(s.snap.Term)), ConfState: s.confState(),
		},
	}, nil
}

// saveSnapshot publishes a leader snapshot into the WAL and advances the
// storage floor: etcd discovers the compaction through FirstIndex/ErrCompacted
// and streams the snapshot to followers that need compacted entries (P6).
func (s *etcdStorage) saveSnapshot(index uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	term := uint64(s.snap.Term)
	if index > uint64(s.snap.Index) {
		ents, err := s.w.ReadEntries(raft.Index(index), raft.Index(index+1))
		if err != nil || len(ents) == 0 {
			return fmt.Errorf("snapshot term at %d: %v", index, err)
		}
		term = uint64(ents[0].Term)
	}
	meta := raft.SnapshotMeta{
		Index: raft.Index(index), Term: raft.Term(term),
		Config: raft.Configuration{Voters: s.voters}, Data: data,
	}
	commit := raft.Index(index)
	if err := s.w.Save(raft.PersistBatch{Snapshot: &meta, Commit: &commit}); err != nil {
		return err
	}
	s.snap = meta
	if s.last < raft.Index(index) {
		s.last = raft.Index(index)
	}
	return nil
}

// etcdNode is one etcd Raft member driven by a Ready loop with an in-process
// message pump (the standard multi-node setup, profiles.md). Proposals carry
// an 8-byte tracking ID overwriting the payload head (size unchanged); the
// FSM work is identical to the other compositions.
type etcdNode struct {
	id    uint64
	n     etcdraft.Node
	store *etcdStorage
	fsm   *stateFSM
	cl    *etcdCluster
	lat   *latCollector

	mu      sync.Mutex
	pending map[uint64]chan error
	reads   map[string]chan uint64
	applied uint64
	nextID  uint64
	readSeq uint64

	stop chan struct{}
	done chan struct{}
}

type etcdCluster struct {
	dir   string
	k     knobs
	lat   *latCollector
	nodes []*etcdNode
	mu    sync.Mutex
	drop  map[uint64]bool
}

func newEtcdCluster(dir string, k knobs, lat *latCollector) (cluster, error) {
	c := &etcdCluster{dir: dir, k: k, lat: lat, drop: make(map[uint64]bool)}
	for i := 1; i <= k.voters; i++ {
		n, err := c.newNode(uint64(i))
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

func (c *etcdCluster) newNode(id uint64) (*etcdNode, error) {
	store, err := newEtcdStorage(c.dir, id, c.k)
	if err != nil {
		return nil, err
	}
	st, err := store.w.Load()
	if err != nil {
		return nil, err
	}
	maxMsg := uint64(8 << 20)
	if c.k.maxCommand > 1<<20 {
		maxMsg = uint64(c.k.maxCommand) + (1 << 20)
	}
	cfg := &etcdraft.Config{
		ID: id, ElectionTick: 10, HeartbeatTick: 1, Storage: store,
		MaxSizePerMsg: maxMsg, MaxCommittedSizePerReady: maxMsg,
		MaxInflightMsgs: 256, PreVote: true, CheckQuorum: true,
		Logger: &etcdraft.DefaultLogger{Logger: log.New(io.Discard, "", 0)},
	}
	var n etcdraft.Node
	if st.HardState.Term > 0 || st.DurableIndex > 0 {
		cfg.Applied = uint64(st.AppliedIndex)
		n = etcdraft.RestartNode(cfg)
	} else {
		var peers []etcdraft.Peer
		for i := 1; i <= c.k.voters; i++ {
			peers = append(peers, etcdraft.Peer{ID: uint64(i)})
		}
		n = etcdraft.StartNode(cfg, peers)
	}
	fsm := newStateFSM(c.k.stateBytes)
	if err := reconstructFSM(store.w, fsm); err != nil {
		return nil, err
	}
	en := &etcdNode{
		id: id, n: n, store: store, fsm: fsm, cl: c, lat: c.lat,
		pending: make(map[uint64]chan error), reads: make(map[string]chan uint64),
		applied: uint64(st.AppliedIndex), stop: make(chan struct{}), done: make(chan struct{}),
	}
	go en.run()
	if c.k.voters == 1 && st.HardState.Term == 0 {
		// Single voter: campaign immediately (fresh node only).
		n.Campaign(context.Background())
	}
	return en, nil
}

func (c *etcdCluster) stalled(id uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.drop[id]
}

func (c *etcdCluster) leader() (node, error) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n.n.Status().RaftState == etcdraft.StateLeader {
				return n, nil
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil, fmt.Errorf("no leader")
}

func (c *etcdCluster) stall(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drop[uint64(id)] = true
}
func (c *etcdCluster) unstall(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.drop, uint64(id))
}

func (c *etcdCluster) restart(id int) error {
	old := c.nodes[id-1]
	close(old.stop)
	<-old.done
	old.n.Stop()
	old.store.w.Close()
	n, err := c.newNode(uint64(id))
	if err != nil {
		return err
	}
	c.nodes[id-1] = n
	return nil
}

func (c *etcdCluster) applied(id int) uint64 {
	n := c.nodes[id-1]
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.applied
}
func (c *etcdCluster) checksum(id int) uint64 { return c.nodes[id-1].fsm.checksum() }
func (c *etcdCluster) stateCRC(id int) uint32 { return c.nodes[id-1].fsm.bufferCRC() }
func (c *etcdCluster) diagnose(id int) string {
	n := c.nodes[id-1]
	st := n.n.Status()
	return fmt.Sprintf("n%d state=%v term=%d leader=%d applied=%d", id, st.RaftState, st.Term, st.Lead, n.fsm.appliedIndex())
}

func (c *etcdCluster) snapshot() error {
	n, err := c.leader()
	if err != nil {
		return err
	}
	en := n.(*etcdNode)
	target := en.fsm.appliedIndex()
	return en.store.saveSnapshot(target, en.fsm.snapshot())
}

func (c *etcdCluster) shutdown() {
	for _, n := range c.nodes {
		close(n.stop)
		<-n.done
		n.n.Stop()
		n.store.w.Close()
	}
}

// run is the Ready loop: persist hard state/entries/snapshot before sending
// messages (etcd's durability ordering), then apply committed entries and
// release read barriers. Storage saves are timed into the shared collector
// (same WAL as the library composition).
func (e *etcdNode) run() {
	defer close(e.done)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-ticker.C:
			e.n.Tick()
		case rd := <-e.n.Ready():
			// 1. Durable state first.
			t0 := time.Now()
			batch := raft.PersistBatch{}
			if !etcdraft.IsEmptyHardState(rd.HardState) {
				hs := raft.HardState{Term: raft.Term(rd.HardState.GetTerm()), Vote: raft.NodeID(rd.HardState.GetVote()), Commit: raft.Index(rd.HardState.GetCommit())}
				batch.HardState = &hs
				e.store.mu.Lock()
				e.store.hs = hs
				e.store.mu.Unlock()
			}
			if len(rd.Entries) > 0 {
				for _, pe := range rd.Entries {
					t := raft.EntryNormal
					if pe.GetType() == pb.EntryConfChange {
						t = raft.EntryConfChange
					}
					batch.Entries = append(batch.Entries, raft.Entry{Term: raft.Term(pe.GetTerm()), Index: raft.Index(pe.GetIndex()), Type: t, Data: pe.Data})
				}
			}
			if batch.HardState != nil || len(batch.Entries) > 0 {
				if err := e.store.w.Save(batch); err != nil {
					panic(err)
				}
				e.lat.addStore(time.Since(t0))
				if len(batch.Entries) > 0 {
					e.store.mu.Lock()
					e.store.last = batch.Entries[len(batch.Entries)-1].Index
					e.store.mu.Unlock()
				}
			}
			if !etcdraft.IsEmptySnap(rd.Snapshot) {
				meta := rd.Snapshot.Metadata
				data := rd.Snapshot.Data
				if err := e.fsm.restore(data, meta.GetIndex()); err != nil {
					panic(fmt.Sprintf("etcd snapshot restore: %v", err))
				}
				sm := raft.SnapshotMeta{
					Index: raft.Index(meta.GetIndex()), Term: raft.Term(meta.GetTerm()),
					Config: raft.Configuration{Voters: voterSet(e.cl.k.voters)}, Data: data,
				}
				commit := sm.Index
				if err := e.store.w.Save(raft.PersistBatch{Snapshot: &sm, Commit: &commit}); err != nil {
					panic(err)
				}
				e.store.mu.Lock()
				e.store.snap = sm
				e.store.last = sm.Index
				e.store.mu.Unlock()
				e.mu.Lock()
				e.applied = meta.GetIndex()
				// In-flight proposals subsumed by the snapshot end unknown;
				// the bench quiesces before snapshot transfers.
				for id, ch := range e.pending {
					ch <- fmt.Errorf("subsumed by snapshot")
					delete(e.pending, id)
				}
				e.mu.Unlock()
			}
			// 2. Messages (in-process pump with stall predicate).
			for _, m := range rd.Messages {
				if e.cl.stalled(m.GetFrom()) || e.cl.stalled(m.GetTo()) {
					continue
				}
				to := m.GetTo()
				if to >= 1 && int(to) <= len(e.cl.nodes) {
					_ = e.cl.nodes[to-1].n.Step(context.Background(), m)
				}
			}
			// 3. Committed entries: FSM application + waiter release.
			if len(rd.CommittedEntries) > 0 {
				t1 := time.Now()
				var last raft.Index
				for _, ce := range rd.CommittedEntries {
					last = raft.Index(ce.GetIndex())
					if ce.GetType() == pb.EntryNormal && len(ce.Data) >= 8 {
						e.fsm.applyEntry(ce.GetIndex())
						id := binary.LittleEndian.Uint64(ce.Data[:8])
						e.mu.Lock()
						if ch := e.pending[id]; ch != nil {
							ch <- nil
							delete(e.pending, id)
						}
						e.mu.Unlock()
					} else {
						e.fsm.advance(ce.GetIndex())
					}
				}
				e.mu.Lock()
				if uint64(last) > e.applied {
					e.applied = uint64(last)
				}
				e.mu.Unlock()
				e.lat.addApply(time.Since(t1))
				if err := e.store.w.MarkApplied(last); err != nil {
					panic(err)
				}
			}
			// 4. Read barriers: release at the confirmed index.
			for _, rs := range rd.ReadStates {
				e.mu.Lock()
				if ch := e.reads[string(rs.RequestCtx)]; ch != nil {
					ch <- rs.Index
					delete(e.reads, string(rs.RequestCtx))
				}
				e.mu.Unlock()
			}
			e.n.Advance()
		}
	}
}

func (e *etcdNode) propose(data []byte) error {
	e.mu.Lock()
	e.nextID++
	id := e.nextID
	ch := make(chan error, 1)
	e.pending[id] = ch
	e.mu.Unlock()
	buf := append([]byte(nil), data...)
	binary.LittleEndian.PutUint64(buf[:8], id)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := e.n.Propose(ctx, buf); err != nil {
		e.mu.Lock()
		delete(e.pending, id)
		e.mu.Unlock()
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		e.mu.Lock()
		delete(e.pending, id)
		e.mu.Unlock()
		return fmt.Errorf("apply timeout")
	}
}

func (e *etcdNode) barrier() error {
	e.mu.Lock()
	e.readSeq++
	key := fmt.Sprintf("read-%d-%d", e.id, e.readSeq)
	ch := make(chan uint64, 1)
	e.reads[key] = ch
	e.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := e.n.ReadIndex(ctx, []byte(key)); err != nil {
		return err
	}
	var idx uint64
	select {
	case idx = <-ch:
	case <-ctx.Done():
		return fmt.Errorf("read index timeout")
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		e.mu.Lock()
		ap := e.applied
		e.mu.Unlock()
		if ap >= idx {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("read apply timeout")
		}
		time.Sleep(time.Millisecond)
	}
}
