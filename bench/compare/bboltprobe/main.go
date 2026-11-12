// Command bboltprobe exercises the incumbent composition's storage primitives
// step by step so a unikernel failure records the exact failing primitive
// (RFC-0001 §6.3: bbolt's memory-mapping growth, file locking, and
// synchronization are qualification risks on unikernels). It first probes bbolt
// directly (open/flock, mmap growth, write+fsync, read, reopen), then — if
// those pass — bootstraps HashiCorp Raft on bbolt and commits 100 entries.
//
// Output: "PROBE <step> PASS|FAIL <detail>" lines, then "PROBE DONE".
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	bolt "go.etcd.io/bbolt"
)

var dir = "/data/bboltprobe"

func pass(step, detail string) { fmt.Printf("PROBE %s PASS %s\n", step, detail) }
func fail(step string, err error) {
	fmt.Printf("PROBE %s FAIL %v\n", step, err)
	fmt.Println("PROBE DONE")
	os.Exit(1)
}

func main() {
	if d := os.Getenv("PROBE_DIR"); d != "" {
		dir = d
	}
	fmt.Printf("PROBE start go=%s arch=%s dir=%s\n", runtimeVersion(), runtimeArch(), dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail("mkdir", err)
	}
	dbpath := filepath.Join(dir, "probe.db")
	os.Remove(dbpath)

	// 1. Open: exercises flock (exclusive) and the initial mmap.
	t0 := time.Now()
	db, err := bolt.Open(dbpath, 0o600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		fail("open-flock-mmap", err)
	}
	pass("open-flock-mmap", fmt.Sprintf("bolt.Open ok in %v", time.Since(t0)))

	// 2. Single write: bucket create + put + commit (fsync).
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("probe"))
		if err != nil {
			return err
		}
		return b.Put([]byte("k0"), make([]byte, 1024))
	})
	if err != nil {
		fail("write-fsync", err)
	}
	pass("write-fsync", "bucket create + 1 KiB put + commit ok")

	// 3. Batch growth: 1000 puts forces mmap remap/growth.
	t0 = time.Now()
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("probe"))
		for i := 0; i < 1000; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%d", i)), make([]byte, 1024)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		fail("mmap-growth", err)
	}
	pass("mmap-growth", fmt.Sprintf("1000 puts (mmap remap) ok in %v", time.Since(t0)))

	// 4. Read back via View (read-only mmap transaction).
	var got int
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("probe"))
		return b.ForEach(func(k, v []byte) error { got++; return nil })
	})
	if err != nil {
		fail("read-view", err)
	}
	if got != 1000 {
		fail("read-view", fmt.Errorf("read %d keys, want 1000", got))
	}
	pass("read-view", fmt.Sprintf("read %d keys ok", got))

	// 5. Close + reopen: flock release/reacquire and recovery.
	if err := db.Close(); err != nil {
		fail("close", err)
	}
	db2, err := bolt.Open(dbpath, 0o600, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		fail("reopen-flock", err)
	}
	pass("reopen-flock", "close + reopen ok")
	db2.Close()

	// 6. Full incumbent composition: HashiCorp Raft on bbolt, single voter.
	raftProbe()
	fmt.Println("PROBE DONE")
}

func raftProbe() {
	rdir := filepath.Join(dir, "raft")
	os.MkdirAll(rdir, 0o755)
	store, err := raftboltdb.NewBoltStore(filepath.Join(rdir, "raft.db"))
	if err != nil {
		fail("raft-boltstore", err)
	}
	pass("raft-boltstore", "raft-boltdb NewBoltStore ok")
	snaps, err := hraft.NewFileSnapshotStore(rdir, 1, io.Discard)
	if err != nil {
		fail("raft-snapstore", err)
	}
	cfg := hraft.DefaultConfig()
	cfg.LocalID = "1"
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	_, tr := hraft.NewInmemTransport("1")
	fsm := &probeFSM{}
	r, err := hraft.NewRaft(cfg, fsm, store, store, snaps, tr)
	if err != nil {
		fail("raft-new", err)
	}
	pass("raft-new", "HashiCorp NewRaft ok")
	if err := r.BootstrapCluster(hraft.Configuration{
		Servers: []hraft.Server{{ID: "1", Address: "1", Suffrage: hraft.Voter}},
	}).Error(); err != nil {
		fail("raft-bootstrap", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for r.State() != hraft.Leader && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r.State() != hraft.Leader {
		fail("raft-leader", fmt.Errorf("no leader"))
	}
	pass("raft-leader", "single voter elected")
	t0 := time.Now()
	for i := 0; i < 100; i++ {
		if err := r.Apply(make([]byte, 1024), 10*time.Second).Error(); err != nil {
			fail("raft-apply", fmt.Errorf("at %d: %w", i, err))
		}
	}
	pass("raft-apply", fmt.Sprintf("100 commits in %v", time.Since(t0)))
	if err := r.Barrier(10 * time.Second).Error(); err != nil {
		fail("raft-barrier", err)
	}
	pass("raft-barrier", "read barrier ok")
	if err := r.Shutdown().Error(); err != nil {
		fail("raft-shutdown", err)
	}
	store.Close()
	pass("raft-shutdown", "shutdown ok")
}

type probeFSM struct{ applied uint64 }

func (f *probeFSM) Apply(l *hraft.Log) interface{} {
	if l.Index > f.applied {
		f.applied = l.Index
	}
	return nil
}
func (f *probeFSM) Snapshot() (hraft.FSMSnapshot, error) { return &probeSnap{}, nil }
func (f *probeFSM) Restore(rc io.ReadCloser) error       { return rc.Close() }

type probeSnap struct{}

func (s *probeSnap) Persist(sink hraft.SnapshotSink) error { return sink.Close() }
func (s *probeSnap) Release()                              {}
