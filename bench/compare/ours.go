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

type oursNode struct {
	d     *driver.Driver
	store *wal.WAL
	dev   *wal.FileDevice
	dir   string
}

type compareFSM struct {
	applied raft.Index
}

func (f *compareFSM) Apply(es []raft.Entry) error {
	for _, e := range es {
		if e.Index > f.applied {
			f.applied = e.Index
		}
	}
	return nil
}
func (f *compareFSM) Snapshot() ([]byte, error)                   { return []byte("snap"), nil }
func (f *compareFSM) Restore(s raft.SnapshotMeta, d []byte) error { f.applied = s.Index; return nil }

func newOurs(dir string) (*oursNode, error) {
	dev, err := wal.OpenFile(filepath.Join(dir, "ours.wal"))
	if err != nil {
		return nil, err
	}
	store, err := wal.Open(dev, wal.Options{
		ClusterID: 1, MemberID: 1, Generation: 1,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{1}},
	})
	if err != nil {
		return nil, err
	}
	d, err := driver.New(driver.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true,
		TickInterval: 5 * time.Millisecond,
	}, store, &compareFSM{}, driver.NewLoopNetwork().NewTransport(1))
	if err != nil {
		return nil, err
	}
	d.Start()
	if err := waitLeader(func() bool { return d.IsLeader() }); err != nil {
		return nil, err
	}
	return &oursNode{d: d, store: store, dev: dev, dir: dir}, nil
}

func waitLeader(is func() bool) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if is() {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("no leader")
}

func (n *oursNode) propose(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := n.d.Propose(ctx, data)
	return err
}

func (n *oursNode) barrier() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := n.d.Read(ctx)
	return err
}

func (n *oursNode) shutdown() {
	n.d.Stop()
	n.store.Close()
	n.dev.Close()
}
