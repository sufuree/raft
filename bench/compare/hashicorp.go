package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

type hcNode struct {
	r     *hraft.Raft
	store *raftboltdb.BoltStore
	dir   string
}

type hcFSM struct{ applied uint64 }

func (f *hcFSM) Apply(l *hraft.Log) interface{} {
	if l.Index > f.applied {
		f.applied = l.Index
	}
	return nil
}
func (f *hcFSM) Snapshot() (hraft.FSMSnapshot, error) { return &hcSnap{}, nil }
func (f *hcFSM) Restore(rc io.ReadCloser) error       { return rc.Close() }

type hcSnap struct{}

func (s *hcSnap) Persist(sink hraft.SnapshotSink) error { return sink.Close() }
func (s *hcSnap) Release()                              {}

func newHashicorp(dir string) (*hcNode, error) {
	hdir := filepath.Join(dir, "hc")
	os.MkdirAll(hdir, 0o755)
	cfg := hraft.DefaultConfig()
	cfg.LocalID = "1"
	// Comparable tick cadence to the library profile.
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.ShutdownOnRemove = false
	store, err := raftboltdb.NewBoltStore(filepath.Join(hdir, "raft.db"))
	if err != nil {
		return nil, err
	}
	snapStore, err := hraft.NewFileSnapshotStore(hdir, 1, os.Stderr)
	if err != nil {
		return nil, err
	}
	_, transport := hraft.NewInmemTransport("1")
	r, err := hraft.NewRaft(cfg, &hcFSM{}, store, store, snapStore, transport)
	if err != nil {
		return nil, err
	}
	hasState, err := hraft.HasExistingState(store, store, snapStore)
	if err != nil {
		return nil, err
	}
	if !hasState {
		f := r.BootstrapCluster(hraft.Configuration{
			Servers: []hraft.Server{{ID: "1", Address: "1", Suffrage: hraft.Voter}},
		})
		if err := f.Error(); err != nil {
			return nil, err
		}
	}
	if err := waitLeader(func() bool { return r.State() == hraft.Leader }); err != nil {
		return nil, err
	}
	return &hcNode{r: r, store: store, dir: hdir}, nil
}

func (n *hcNode) propose(data []byte) error {
	f := n.r.Apply(data, 10*time.Second)
	if err := f.Error(); err != nil {
		return err
	}
	if resp, ok := f.Response().(error); ok && resp != nil {
		return resp
	}
	return nil
}

func (n *hcNode) barrier() error {
	return n.r.Barrier(10 * time.Second).Error()
}

func (n *hcNode) shutdown() {
	n.r.Shutdown().Error()
	n.store.Close()
}

var _ = fmt.Sprintf
