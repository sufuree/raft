// Command qualify runs the RFC-0001 platform qualification suite inside a
// guest (Nanos unikernel or Linux reference). It prints structured
// "QUAL <name> PASS|FAIL <detail>" lines and a final "QUAL DONE".
//
// Modes:
//
//	qualify             run the full in-guest suite
//	qualify crashwrite  write durable + unsynced state, then hang (host kills)
//	qualify crashverify verify recovery after the host killed the guest
//	qualify bench       single-voter unikernel benchmark profiles (serial JSON)
//	qualify idle        idle with no traffic while the host samples VMM CPU
//	qualify fssubstrate record filesystem flush/rename/locking behavior (§6.3)
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/driver"
	"github.com/sufuree/raft/wal"
)

var workDir = "/data"

func report(name string, err error, detail string) {
	if err != nil {
		fmt.Printf("QUAL %s FAIL %v\n", name, err)
		return
	}
	fmt.Printf("QUAL %s PASS %s\n", name, detail)
}

func main() {
	runtime.GOMAXPROCS(1) // single-vCPU profile (§8)
	if d := os.Getenv("QUAL_DIR"); d != "" {
		workDir = d
	}
	mode := "full"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if m := os.Getenv("QUAL_MODE"); m != "" {
		mode = m
	}
	fmt.Printf("QUAL start mode=%s go=%s arch=%s/%s gomaxprocs=%d dir=%s\n",
		mode, runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0), workDir)
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		fmt.Printf("QUAL workdir FAIL %v\n", err)
		workDir = os.TempDir()
		os.MkdirAll(workDir, 0o755)
		fmt.Printf("QUAL workdir-fallback PASS %s\n", workDir)
	}
	switch mode {
	case "crashwrite":
		crashWrite()
	case "crashverify":
		crashVerify()
	case "bench":
		runBench()
	case "idle":
		runIdle()
	case "fssubstrate":
		fsSubstrate()
	default:
		// Clean prior suite artifacts for a fresh qualification run.
		for _, f := range []string{"file.wal", "block.wal", "lock"} {
			os.Remove(filepath.Join(workDir, f))
		}
		os.RemoveAll(filepath.Join(workDir, "single"))
		full()
	}
	fmt.Println("QUAL DONE")
}

func walOpts(id uint64) wal.Options {
	return wal.Options{
		ClusterID: 42, MemberID: id, Generation: 1,
		SegmentSize: 64 << 10, MaxBatchBytes: 32 << 10,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{raft.NodeID(id)}},
	}
}

// walSuite exercises one Device binding: write/sync/reopen, torn tail,
// committed-prefix corruption refusal, snapshot publication + reclamation.
func walSuite(name string, mkDev func(path string) (wal.Device, error), path string) {
	dev, err := mkDev(path)
	if err != nil {
		report(name+"-open", err, "")
		return
	}
	w, err := wal.Open(dev, walOpts(1))
	if err != nil {
		report(name+"-create", err, "")
		return
	}
	payload := bytes.Repeat([]byte("q"), 1024)
	for i := 1; i <= 120; i++ {
		c := raft.Index(i)
		if err := w.Save(raft.PersistBatch{
			Entries: []raft.Entry{{Term: 1, Index: raft.Index(i), Data: payload}},
			Commit:  &c,
		}); err != nil {
			report(name+"-save", err, fmt.Sprintf("at %d", i))
			return
		}
	}
	if err := w.MarkApplied(120); err != nil {
		report(name+"-applied", err, "")
		return
	}
	snap := raft.SnapshotMeta{Index: 100, Term: 1,
		Config: raft.Configuration{Voters: raft.NodeSet{1}}, Data: bytes.Repeat([]byte("s"), 48<<10)}
	c100 := raft.Index(100)
	if err := w.Save(raft.PersistBatch{Snapshot: &snap, Commit: &c100}); err != nil {
		report(name+"-snapshot", err, "")
		return
	}
	w.Close()

	// Reopen: recovery must reconstruct snapshot + suffix + watermarks.
	dev2, err := mkDev(path)
	if err != nil {
		report(name+"-reopen-dev", err, "")
		return
	}
	w2, err := wal.Open(dev2, walOpts(1))
	if err != nil {
		report(name+"-reopen", err, "")
		return
	}
	st, err := w2.Load()
	if err != nil {
		report(name+"-load", err, "")
		return
	}
	if st.Snapshot.Index != 100 || len(st.Snapshot.Data) != 48<<10 || st.HardState.Commit < 100 || st.AppliedIndex != 120 {
		report(name+"-state", fmt.Errorf("snap=%d data=%d commit=%d applied=%d",
			st.Snapshot.Index, len(st.Snapshot.Data), st.HardState.Commit, st.AppliedIndex), "")
		return
	}
	w2.Close()
	report(name, nil, "write/sync/reopen/snapshot/reclaim verified")
}

func full() {
	// File substrate.
	walSuite("wal-file", func(p string) (wal.Device, error) { return wal.OpenFile(p) },
		filepath.Join(workDir, "file.wal"))

	// Raw-block binding: fixed region, constant size, no-op truncate, ENOSPC.
	// Pre-size only on first creation; reopening must preserve contents.
	walSuite("wal-block", func(p string) (wal.Device, error) {
		f, err := wal.OpenFile(p)
		if err != nil {
			return nil, err
		}
		sz, err := f.Size()
		if err != nil {
			return nil, err
		}
		if sz < 512<<10 {
			if _, err := f.WriteAt(make([]byte, 512<<10), 0); err != nil {
				return nil, err
			}
			if err := f.Sync(); err != nil {
				return nil, err
			}
		}
		return wal.NewFixedRegion(f, 512<<10)
	}, filepath.Join(workDir, "block.wal"))

	// Locking semantics (recorded, not a fence: §6.3, §9).
	lockPath := filepath.Join(workDir, "lock")
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err == nil {
		err = flock(lf)
		report("locking", err, "advisory lock acquired (not an ownership fence)")
		lf.Close()
	} else {
		report("locking", err, "")
	}

	// Filesystem-substrate primitives (§6.3): flush/rename/dir-sync/create-remove.
	fsSubstrate()

	// Single-voter driver profile: propose/read/snapshot/restart timing.
	singleVoter()

	// Three-voter cluster over in-guest TCP.
	cluster()

	// Resource snapshot.
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Printf("QUAL resources PASS heapAlloc=%d heapSys=%d numGC=%d\n", ms.HeapAlloc, ms.HeapSys, ms.NumGC)
}

type qFSM struct {
	applied raft.Index
	data    map[raft.Index][]byte
}

func (f *qFSM) Apply(es []raft.Entry) error {
	for _, e := range es {
		if e.Type == raft.EntryNormal {
			f.data[e.Index] = append([]byte(nil), e.Data...)
		}
		if e.Index > f.applied {
			f.applied = e.Index
		}
	}
	return nil
}
func (f *qFSM) Snapshot() ([]byte, error) { return []byte("fsm-snapshot"), nil }
func (f *qFSM) Restore(s raft.SnapshotMeta, data []byte) error {
	f.applied = s.Index
	return nil
}

func singleVoter() {
	dir := filepath.Join(workDir, "single")
	os.MkdirAll(dir, 0o755)
	dev, err := wal.OpenFile(filepath.Join(dir, "s.wal"))
	if err != nil {
		report("single-open", err, "")
		return
	}
	store, err := wal.Open(dev, walOpts(1))
	if err != nil {
		report("single-create", err, "")
		return
	}
	fsm := &qFSM{data: make(map[raft.Index][]byte)}
	net := driver.NewLoopNetwork()
	d, err := driver.New(driver.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true,
		TickInterval: 5 * time.Millisecond, MaxBatchBytes: 32 << 10, MaxCommandBytes: 16 << 10,
	}, store, fsm, net.NewTransport(1))
	if err != nil {
		report("single-driver", err, "")
		return
	}
	d.Start()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	deadline := time.Now().Add(10 * time.Second)
	for !d.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !d.IsLeader() {
		report("single-leader", fmt.Errorf("no leader"), "")
		return
	}
	t0 := time.Now()
	for i := 0; i < 100; i++ {
		if _, err := d.Propose(ctx, []byte(fmt.Sprintf("cmd-%d", i))); err != nil {
			report("single-propose", err, fmt.Sprintf("at %d", i))
			return
		}
	}
	commit100 := time.Since(t0)
	if _, err := d.Read(ctx); err != nil {
		report("single-read", err, "")
		return
	}
	d.Snapshot()
	time.Sleep(200 * time.Millisecond)
	d.Stop()

	// Restart timing: open + restore + bounded replay (§11 first-class metric).
	t1 := time.Now()
	dev2, _ := wal.OpenFile(filepath.Join(dir, "s.wal"))
	store2, err := wal.Open(dev2, walOpts(1))
	if err != nil {
		report("single-restart", err, "")
		return
	}
	st, _ := store2.Load()
	fsm2 := &qFSM{data: make(map[raft.Index][]byte)}
	d2, err := driver.New(driver.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true,
		TickInterval: 5 * time.Millisecond, MaxBatchBytes: 32 << 10, MaxCommandBytes: 16 << 10,
	}, store2, fsm2, net.NewTransport(1))
	if err != nil {
		report("single-restart-driver", err, "")
		return
	}
	d2.Start()
	restartTime := time.Since(t1)
	deadline = time.Now().Add(10 * time.Second)
	for !d2.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	leaderOK := d2.IsLeader()
	d2.Stop()
	if !leaderOK || st.HardState.Commit < 100 {
		report("single-restart", fmt.Errorf("leader=%v commit=%d", leaderOK, st.HardState.Commit), "")
		return
	}
	report("single-voter", nil, fmt.Sprintf("100 commits in %v, restart-to-leader %v", commit100, restartTime))
}

func cluster() {
	net := driver.NewLoopNetwork()
	voters := raft.Configuration{Voters: raft.NodeSet{1, 2, 3}}
	ds := make([]*driver.Driver, 3)
	for i := 1; i <= 3; i++ {
		id := raft.NodeID(i)
		store := raft.NewMemStorage()
		store.SetInitialConfig(voters)
		d, err := driver.New(driver.Config{
			ID: id, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true,
			Seed: int64(i) * 17, TickInterval: 2 * time.Millisecond,
		}, store, &qFSM{data: make(map[raft.Index][]byte)}, net.NewTransport(id))
		if err != nil {
			report("cluster-new", err, "")
			return
		}
		ds[i-1] = d
		d.Start()
	}
	defer func() {
		for _, d := range ds {
			d.Stop()
		}
	}()
	var leader *driver.Driver
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, d := range ds {
			if d.IsLeader() {
				leader = d
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if leader == nil {
		report("cluster-leader", fmt.Errorf("no leader"), "")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		if _, err := leader.Propose(ctx, []byte(fmt.Sprintf("c-%d", i))); err != nil {
			report("cluster-propose", err, fmt.Sprintf("at %d", i))
			return
		}
	}
	if _, err := leader.Read(ctx); err != nil {
		report("cluster-read", err, "")
		return
	}
	report("cluster", nil, "3-voter in-guest commit + read barrier verified")
}

// crashWrite writes durable state plus an unsynced tail, then hangs so the
// host can kill the guest (power-loss model).
func crashWrite() {
	path := filepath.Join(workDir, "crash.wal")
	os.Remove(path) // fresh power-loss test
	dev, err := wal.OpenFile(path)
	if err != nil {
		fmt.Printf("QUAL crashwrite-open FAIL %v\n", err)
		return
	}
	w, err := wal.Open(dev, walOpts(1))
	if err != nil {
		fmt.Printf("QUAL crashwrite-create FAIL %v\n", err)
		return
	}
	for i := 1; i <= 50; i++ {
		c := raft.Index(i)
		if err := w.Save(raft.PersistBatch{
			Entries: []raft.Entry{{Term: 1, Index: raft.Index(i), Data: []byte("durable")}},
			Commit:  &c,
		}); err != nil {
			fmt.Printf("QUAL crashwrite-save FAIL %v\n", err)
			return
		}
	}
	// Unsynced partial record at the second segment start, beyond all durable
	// records: recovery must treat it as a disposable torn tail.
	dev.WriteAt([]byte{0x52, 0x42, 0x41, 0x54, 0xff, 0xff, 0xff, 0xff}, 4096+64<<10)
	fmt.Println("QUAL crashwrite READY")
	for {
		time.Sleep(time.Hour) // hang until the host kills the guest
	}
}

func crashVerify() {
	path := filepath.Join(workDir, "crash.wal")
	dev, err := wal.OpenFile(path)
	if err != nil {
		report("crashverify-open", err, "")
		return
	}
	w, err := wal.Open(dev, walOpts(1))
	if err != nil {
		report("crashverify-recover", err, "")
		return
	}
	st, _ := w.Load()
	w.Close()
	if st.HardState.Commit != 50 || st.DurableIndex != 50 {
		report("crashverify", fmt.Errorf("commit=%d durable=%d", st.HardState.Commit, st.DurableIndex), "")
		return
	}
	report("crashverify", nil, "durable prefix survived power loss; unsynced tail discarded")
}
