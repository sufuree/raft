package main

// In-guest single-voter benchmark mode (RFC-0001 §11: one-voter unikernel
// profile first). It runs the published profiles (docs/benchmarks/profiles.md)
// against the library on the guest's durable substrate and prints one serial
// JSON line per profile ("QUALBENCH {...}") for the host to capture. Memory is
// reported as guest Go heap + runtime mappings (MemStats.Sys); the host samples
// total VMM RSS for guest-total accounting (heap + mappings + kernel cache).

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/driver"
	"github.com/sufuree/raft/wal"
)

type benchResult struct {
	Profile    string  `json:"profile"`
	Ops        int     `json:"ops"`
	Errors     int     `json:"errors"`
	P50ms      float64 `json:"p50_ms"`
	P95ms      float64 `json:"p95_ms"`
	P99ms      float64 `json:"p99_ms"`
	OpsSec     float64 `json:"ops_sec"`
	HeapMB     float64 `json:"heap_mb"`
	SysMB      float64 `json:"sys_mb"`
	FsyncP50ms float64 `json:"fsync_p50_ms"`
	FsyncP99ms float64 `json:"fsync_p99_ms"`
	StoreP50ms float64 `json:"store_p50_ms"`
	StoreP99ms float64 `json:"store_p99_ms"`
	ApplyP50ms float64 `json:"apply_p50_ms"`
	ApplyP99ms float64 `json:"apply_p99_ms"`
	ArchiveMB  int     `json:"archive_mb,omitempty"`
	RestartMs  float64 `json:"restart_ms,omitempty"`
	Integrity  bool    `json:"integrity_ok,omitempty"`
	Note       string  `json:"note,omitempty"`
}

// bFSM is the slot-buffer state machine (same design as bench/compare): each
// non-empty normal entry stamps its index into a slot; snapshots carry an
// applied-index header + buffer; restore validates every live slot.
type bFSM struct {
	mu      sync.Mutex
	cells   int
	buf     []byte
	applied uint64
}

func newBFSM(stateBytes int) *bFSM {
	n := stateBytes / 16
	if n < 1 {
		n = 1
	}
	return &bFSM{cells: n, buf: make([]byte, n*16)}
}
func (f *bFSM) stamp(index uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	off := int(index%uint64(f.cells)) * 16
	binary.LittleEndian.PutUint64(f.buf[off:], index)
	for i := off + 8; i < off+16; i++ {
		f.buf[i] = byte(index)
	}
	if index > f.applied {
		f.applied = index
	}
}
func (f *bFSM) advance(index uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if index > f.applied {
		f.applied = index
	}
}
func (f *bFSM) Apply(es []raft.Entry) error {
	for _, e := range es {
		if e.Type == raft.EntryNormal && len(e.Data) > 0 {
			f.stamp(uint64(e.Index))
		} else {
			f.advance(uint64(e.Index))
		}
	}
	return nil
}
func (f *bFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]byte, 8+len(f.buf))
	binary.LittleEndian.PutUint64(out, f.applied)
	copy(out[8:], f.buf)
	return out, nil
}
func (f *bFSM) Restore(s raft.SnapshotMeta, data []byte) error {
	if len(data) != 8+len(f.buf) {
		return fmt.Errorf("snapshot size %d want %d", len(data), 8+len(f.buf))
	}
	idx := binary.LittleEndian.Uint64(data)
	if idx != uint64(s.Index) {
		return fmt.Errorf("snapshot header %d != manifest %d", idx, s.Index)
	}
	for slot := 0; slot < f.cells; slot++ {
		off := 8 + slot*16
		v := binary.LittleEndian.Uint64(data[off:])
		if v == 0 {
			continue
		}
		if v > idx || v%uint64(f.cells) != uint64(slot) {
			return fmt.Errorf("slot %d index %d invalid at %d", slot, v, idx)
		}
		for i := off + 8; i < off+16; i++ {
			if data[i] != byte(v) {
				return fmt.Errorf("slot %d fill mismatch", slot)
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	copy(f.buf, data[8:])
	f.applied = idx
	return nil
}
func (f *bFSM) crc() uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return crc32.ChecksumIEEE(f.buf)
}
func (f *bFSM) appliedIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applied
}

type benchNode struct {
	d     *driver.Driver
	store *wal.WAL
	dev   *wal.FileDevice
	fsm   *bFSM
	dir   string
}

func benchWalOpts(maxCommand int) wal.Options {
	o := wal.Options{
		ClusterID: 7, MemberID: 1, Generation: 1,
		InitialConfig:    raft.Configuration{Voters: raft.NodeSet{1}},
		MaxSnapshotBytes: 128 << 20,
	}
	if maxCommand > 1<<20 {
		o.SegmentSize = 64 << 20
		o.MaxBatchBytes = 48 << 20
	}
	return o
}

func newBenchNode(dir string, stateBytes, maxCommand int, lat *latObs) (*benchNode, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dev, err := wal.OpenFile(filepath.Join(dir, "bench.wal"))
	if err != nil {
		return nil, err
	}
	store, err := wal.Open(dev, benchWalOpts(maxCommand))
	if err != nil {
		return nil, err
	}
	fsm := newBFSM(stateBytes)
	// Consumer-side FSM reconstruction on restart (RFC §7).
	st, err := store.Load()
	if err != nil {
		return nil, err
	}
	if st.Snapshot.Index > 0 && len(st.Snapshot.Data) > 0 {
		if err := fsm.Restore(st.Snapshot, st.Snapshot.Data); err != nil {
			return nil, err
		}
	}
	if st.AppliedIndex > st.Snapshot.Index {
		ents, err := store.ReadEntries(st.Snapshot.Index+1, st.AppliedIndex+1)
		if err != nil {
			return nil, err
		}
		if err := fsm.Apply(ents); err != nil {
			return nil, err
		}
	}
	batch := 8 << 20
	if maxCommand > 1<<20 {
		batch = 48 << 20
	}
	d, err := driver.New(driver.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true,
		TickInterval:    5 * time.Millisecond,
		MaxCommandBytes: maxCommand, MaxBatchBytes: batch,
		MaxPendingBytes: 4*maxCommand + (1 << 20), MaxSnapshotBytes: 128 << 20,
		StagingDir: filepath.Join(dir, "staging"),
		OnStore:    func(op string, d time.Duration) { lat.store(d) },
		OnApply:    func(n int, d time.Duration) { lat.apply(d) },
	}, store, fsm, driver.NewLoopNetwork().NewTransport(1))
	if err != nil {
		return nil, err
	}
	d.Start()
	deadline := time.Now().Add(15 * time.Second)
	for !d.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !d.IsLeader() {
		return nil, fmt.Errorf("no leader")
	}
	return &benchNode{d: d, store: store, dev: dev, fsm: fsm, dir: dir}, nil
}

func (n *benchNode) propose(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := n.d.Propose(ctx, data)
	return err
}
func (n *benchNode) barrier() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := n.d.Read(ctx)
	return err
}
func (n *benchNode) close() {
	n.d.Stop()
	n.store.Close()
	n.dev.Close()
}
func (n *benchNode) snapshot() error {
	target := n.fsm.appliedIndex()
	n.d.Snapshot()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		s, err := n.store.ReadSnapshot()
		if err == nil && uint64(s.Index) >= target {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("snapshot not published")
}

type latObs struct {
	mu   sync.Mutex
	s, a []float64
}

func (l *latObs) store(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.s = append(l.s, float64(d.Microseconds())/1000)
}
func (l *latObs) apply(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.a = append(l.a, float64(d.Microseconds())/1000)
}
func pct(s []float64, p float64) float64 {
	if len(s) == 0 {
		return 0
	}
	return s[int(p/100*float64(len(s)-1))]
}
func (l *latObs) fill(r *benchResult) {
	l.mu.Lock()
	defer l.mu.Unlock()
	sort.Float64s(l.s)
	sort.Float64s(l.a)
	r.StoreP50ms, r.StoreP99ms = pct(l.s, 50), pct(l.s, 99)
	r.ApplyP50ms, r.ApplyP99ms = pct(l.a, 50), pct(l.a, 99)
}

func benchFsyncProbe(dir string) (p50, p99 float64) {
	f, err := os.OpenFile(filepath.Join(dir, "fsync-probe"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, 0
	}
	defer os.Remove(f.Name())
	defer f.Close()
	buf := make([]byte, 1024)
	var lats []float64
	for i := 0; i < 200; i++ {
		t0 := time.Now()
		f.WriteAt(buf, 0)
		f.Sync()
		lats = append(lats, float64(time.Since(t0).Microseconds())/1000)
	}
	sort.Float64s(lats)
	return pct(lats, 50), pct(lats, 99)
}

func emitBench(r benchResult) {
	runtime.GC() // settle live heap before reporting library overhead
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	r.HeapMB = float64(ms.HeapAlloc) / (1 << 20)
	r.SysMB = float64(ms.Sys) / (1 << 20)
	b, _ := json.Marshal(r)
	fmt.Printf("QUALBENCH %s\n", b)
}

func benchEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// runBench executes the single-voter unikernel profiles serially.
func runBench() {
	seconds := benchEnvInt("QUAL_BENCH_SECONDS", 60)
	burstN := benchEnvInt("QUAL_BENCH_BURST", 1000)
	archiveOps := benchEnvInt("QUAL_BENCH_ARCHIVE_OPS", 30)
	pipeline := benchEnvInt("QUAL_BENCH_PIPELINE", 32)
	base := filepath.Join(workDir, "bench")
	os.MkdirAll(base, 0o755)

	// P1: sequential 1 KiB closed-loop.
	benchSequential(base, "P1", 1024, 4<<10, time.Duration(seconds)*time.Second, 0, 0)
	// P2: burst.
	benchSequential(base, "P2", 1024, 4<<10, 0, 0, burstN)
	// P4: archive 8 MiB and 32 MiB.
	for _, mb := range []int{8, 32} {
		benchSequential(base, "P4", mb<<20, 4<<10, 0, time.Second, archiveOps, mb)
	}
	// P8: pipelined.
	benchPipelined(base, pipeline, time.Duration(seconds)*time.Second)
	// P7: restart with 64 MiB snapshot + 1000-entry suffix.
	benchRestart(base)
}

func benchSequential(base, profile string, cmdBytes, stateBytes int, dur, pace time.Duration, count int, archiveMB ...int) {
	dir := filepath.Join(base, profile)
	os.RemoveAll(dir)
	lat := &latObs{}
	r := benchResult{Profile: profile}
	if len(archiveMB) > 0 {
		r.ArchiveMB = archiveMB[0]
	}
	r.FsyncP50ms, r.FsyncP99ms = benchFsyncProbe(dir)
	n, err := newBenchNode(dir, stateBytes, cmdBytes, lat)
	if err != nil {
		r.Note = err.Error()
		emitBench(r)
		return
	}
	payload := make([]byte, cmdBytes)
	var lats []float64
	start := time.Now()
	deadline := start.Add(dur)
	for {
		if count > 0 && r.Ops+r.Errors >= count {
			break
		}
		if count == 0 && !time.Now().Before(deadline) {
			break
		}
		t0 := time.Now()
		if err := n.propose(payload); err != nil {
			r.Errors++
		} else {
			r.Ops++
			lats = append(lats, float64(time.Since(t0).Microseconds())/1000)
		}
		if pace > 0 {
			if d := pace - time.Since(t0); d > 0 {
				time.Sleep(d)
			}
		}
	}
	total := time.Since(start)
	n.close()
	sort.Float64s(lats)
	r.P50ms, r.P95ms, r.P99ms = pct(lats, 50), pct(lats, 95), pct(lats, 99)
	if total > 0 {
		r.OpsSec = float64(r.Ops) / total.Seconds()
	}
	lat.fill(&r)
	emitBench(r)
}

func benchPipelined(base string, workers int, dur time.Duration) {
	dir := filepath.Join(base, "P8")
	os.RemoveAll(dir)
	lat := &latObs{}
	r := benchResult{Profile: "P8"}
	r.FsyncP50ms, r.FsyncP99ms = benchFsyncProbe(dir)
	n, err := newBenchNode(dir, 4<<10, 1<<20, lat)
	if err != nil {
		r.Note = err.Error()
		emitBench(r)
		return
	}
	var mu sync.Mutex
	var lats []float64
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(dur)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1024)
			var local []float64
			var ops, errs int
			for time.Now().Before(deadline) {
				t0 := time.Now()
				if err := n.propose(buf); err != nil {
					errs++
					continue
				}
				ops++
				local = append(local, float64(time.Since(t0).Microseconds())/1000)
			}
			mu.Lock()
			lats = append(lats, local...)
			r.Ops += ops
			r.Errors += errs
			mu.Unlock()
		}()
	}
	wg.Wait()
	total := time.Since(start)
	n.close()
	sort.Float64s(lats)
	r.P50ms, r.P95ms, r.P99ms = pct(lats, 50), pct(lats, 95), pct(lats, 99)
	if total > 0 {
		r.OpsSec = float64(r.Ops) / total.Seconds()
	}
	lat.fill(&r)
	emitBench(r)
}

func benchRestart(base string) {
	dir := filepath.Join(base, "P7")
	os.RemoveAll(dir)
	lat := &latObs{}
	r := benchResult{Profile: "P7"}
	r.FsyncP50ms, r.FsyncP99ms = benchFsyncProbe(dir)
	n, err := newBenchNode(dir, 64<<20, 1<<20, lat)
	if err != nil {
		r.Note = err.Error()
		emitBench(r)
		return
	}
	seed := func(count int) {
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				buf := make([]byte, 1024)
				for i := 0; i < count/8; i++ {
					if err := n.propose(buf); err != nil {
						panic(err)
					}
				}
			}()
		}
		wg.Wait()
	}
	seed(2000)
	if err := n.snapshot(); err != nil {
		r.Note = err.Error()
		emitBench(r)
		n.close()
		return
	}
	seed(1000)
	n.barrier()
	wantCRC := n.fsm.crc()
	wantApplied := n.fsm.appliedIndex()
	n.close()

	t0 := time.Now()
	n2, err := newBenchNode(dir, 64<<20, 1<<20, lat)
	if err != nil {
		r.Note = "reopen: " + err.Error()
		emitBench(r)
		return
	}
	if err := n2.barrier(); err != nil {
		r.Note = "barrier: " + err.Error()
		emitBench(r)
		n2.close()
		return
	}
	r.RestartMs = float64(time.Since(t0).Microseconds()) / 1000
	r.Integrity = n2.fsm.crc() == wantCRC && n2.fsm.appliedIndex() >= wantApplied
	r.Ops = 1
	n2.close()
	lat.fill(&r)
	emitBench(r)
}
