package main

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	etcdraft "go.etcd.io/raft/v3"
	"google.golang.org/protobuf/proto"
)

// etcdStorage backs etcd Raft with the same WAL used by the library, so all
// compositions acknowledge the same durability (fsync per batch).
type etcdStorage struct {
	mu   sync.Mutex
	w    *wal.WAL
	snap raft.SnapshotMeta
	hs   raft.HardState
	last raft.Index
}

func newEtcdStorage(dir string) (*etcdStorage, error) {
	dev, err := wal.OpenFile(filepath.Join(dir, "etcd.wal"))
	if err != nil {
		return nil, err
	}
	w, err := wal.Open(dev, wal.Options{
		ClusterID: 2, MemberID: 1, Generation: 1,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{1}},
	})
	if err != nil {
		return nil, err
	}
	st, err := w.Load()
	if err != nil {
		return nil, err
	}
	return &etcdStorage{w: w, snap: st.Snapshot, hs: st.HardState, last: st.DurableIndex}, nil
}

func (s *etcdStorage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cs := pb.ConfState{}
	for _, v := range s.snap.Config.Voters {
		cs.Voters = append(cs.Voters, uint64(v))
	}
	if len(cs.Voters) == 0 {
		cs.Voters = []uint64{1}
	}
	return &pb.HardState{Term: proto.Uint64(uint64(s.hs.Term)), Vote: proto.Uint64(uint64(s.hs.Vote)), Commit: proto.Uint64(uint64(s.hs.Commit))}, &cs, nil
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
	cs := pb.ConfState{}
	for _, v := range s.snap.Config.Voters {
		cs.Voters = append(cs.Voters, uint64(v))
	}
	return &pb.Snapshot{
		Data: s.snap.Data,
		Metadata: &pb.SnapshotMetadata{
			Index: proto.Uint64(uint64(s.snap.Index)), Term: proto.Uint64(uint64(s.snap.Term)), ConfState: &cs,
		},
	}, nil
}

type etcdNode struct {
	n         etcdraft.Node
	store     *etcdStorage
	applied   uint64
	mu        sync.Mutex
	cond      *sync.Cond
	stop      chan struct{}
	done      chan struct{}
	proposeFn func([]byte) error
}

func newEtcd(dir string) (*etcdNode, error) {
	store, err := newEtcdStorage(dir)
	if err != nil {
		return nil, err
	}
	cfg := &etcdraft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         store,
		MaxSizePerMsg:   1 << 20,
		MaxInflightMsgs: 256,
		PreVote:         true,
		CheckQuorum:     true,
	}
	n := etcdraft.StartNode(cfg, []etcdraft.Peer{{ID: 1}})
	en := &etcdNode{n: n, store: store, stop: make(chan struct{}), done: make(chan struct{})}
	en.cond = sync.NewCond(&en.mu)
	go en.run()
	// Single voter: campaign immediately.
	n.Campaign(context.Background())
	if err := waitLeader(func() bool { return n.Status().RaftState == etcdraft.StateLeader }); err != nil {
		return nil, err
	}
	en.proposeFn = func(data []byte) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		en.mu.Lock()
		start := en.applied
		en.mu.Unlock()
		if err := n.Propose(ctx, data); err != nil {
			return err
		}
		// Wait for application of a new entry (single proposer: index advances).
		deadline := time.Now().Add(10 * time.Second)
		en.mu.Lock()
		for en.applied <= start {
			if time.Now().After(deadline) {
				en.mu.Unlock()
				return fmt.Errorf("apply timeout")
			}
			en.cond.Wait()
		}
		en.mu.Unlock()
		return nil
	}
	return en, nil
}

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
			if !etcdraft.IsEmptyHardState(rd.HardState) {
				hs := raft.HardState{Term: raft.Term(rd.HardState.GetTerm()), Vote: raft.NodeID(rd.HardState.GetVote()), Commit: raft.Index(rd.HardState.GetCommit())}
				if err := e.store.w.Save(raft.PersistBatch{HardState: &hs}); err != nil {
					panic(err)
				}
				e.store.mu.Lock()
				e.store.hs = hs
				e.store.mu.Unlock()
			}
			if len(rd.Entries) > 0 {
				var ents []raft.Entry
				for _, pe := range rd.Entries {
					t := raft.EntryNormal
					if pe.GetType() == pb.EntryConfChange {
						t = raft.EntryConfChange
					}
					ents = append(ents, raft.Entry{Term: raft.Term(pe.GetTerm()), Index: raft.Index(pe.GetIndex()), Type: t, Data: pe.Data})
				}
				if err := e.store.w.Save(raft.PersistBatch{Entries: ents}); err != nil {
					panic(err)
				}
				e.store.mu.Lock()
				e.store.last = ents[len(ents)-1].Index
				e.store.mu.Unlock()
			}
			if !etcdraft.IsEmptySnap(rd.Snapshot) {
				e.store.mu.Lock()
				e.store.snap.Index = raft.Index(rd.Snapshot.Metadata.GetIndex())
				e.store.snap.Term = raft.Term(rd.Snapshot.Metadata.GetTerm())
				e.store.mu.Unlock()
			}
			for _, ce := range rd.CommittedEntries {
				if ce.GetType() == pb.EntryNormal && len(ce.Data) > 0 {
					e.mu.Lock()
					if ce.GetIndex() > e.applied {
						e.applied = ce.GetIndex()
					}
					e.cond.Broadcast()
					e.mu.Unlock()
				}
				e.store.w.MarkApplied(raft.Index(ce.GetIndex()))
			}
			e.n.Advance()
		}
	}
}

func (e *etcdNode) propose(data []byte) error { return e.proposeFn(data) }

func (e *etcdNode) barrier() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return e.n.ReadIndex(ctx, []byte("barrier"))
}

func (e *etcdNode) shutdown() {
	close(e.stop)
	<-e.done
	e.n.Stop()
	e.store.w.Close()
}
