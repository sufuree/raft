// Command compare benchmarks the RFC-0001 library against pinned baseline
// compositions (HashiCorp Raft + raft-boltdb/bbolt; etcd Raft + WAL adapter)
// under the published workload profiles (docs/benchmarks/profiles.md,
// revision 2). All compositions run the same application FSM (fsm.go) and
// acknowledged-durability semantics: every committed batch is fsynced before
// the client observes success. Single-voter profiles run first (§11).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

// result is one benchmark cell. Latency fields are milliseconds. Extra
// fields carry profile-specific metrics (restart, catch-up, stall window).
type result struct {
	Impl    string `json:"impl"`
	Profile string `json:"profile"`
	Voters  int    `json:"voters"`
	Ops     int    `json:"ops"`
	Errors  int    `json:"errors"`
	// Overloads counts typed admission rejections (library; §8).
	Overloads int     `json:"overloads"`
	P50ms     float64 `json:"p50_ms"`
	P95ms     float64 `json:"p95_ms"`
	P99ms     float64 `json:"p99_ms"`
	TotalSec  float64 `json:"total_sec"`
	OpsSec    float64 `json:"ops_sec"`
	HeapMB    float64 `json:"heap_mb"`
	// RSSMB includes mapped pages (Linux); CacheDeltaMB is the system page
	// cache growth across the cell (Linux). Zero where not observable.
	RSSMB        float64 `json:"rss_mb"`
	CacheDeltaMB float64 `json:"cache_delta_mb"`
	// Fsync* characterize the cell's storage class (write 1 KiB + fsync).
	FsyncP50ms float64 `json:"fsync_p50_ms"`
	FsyncP95ms float64 `json:"fsync_p95_ms"`
	FsyncP99ms float64 `json:"fsync_p99_ms"`
	// Store* are ordered storage-batch durations (write+sync): the library's
	// OnStore observer; etcd's WAL saves; zero for HashiCorp (uninstrumented).
	StoreP50ms float64 `json:"store_p50_ms"`
	StoreP95ms float64 `json:"store_p95_ms"`
	StoreP99ms float64 `json:"store_p99_ms"`
	// Apply* are dispatch->FSM-completion (library OnApply); FSM execution
	// time for baselines (basis labeled in the report).
	ApplyP50ms float64 `json:"apply_p50_ms"`
	ApplyP95ms float64 `json:"apply_p95_ms"`
	ApplyP99ms float64 `json:"apply_p99_ms"`
	// Profile-specific metrics (omitempty).
	ArchiveMB   int     `json:"archive_mb,omitempty"`
	RestartMs   float64 `json:"restart_ms,omitempty"`
	CatchupSec  float64 `json:"catchup_sec,omitempty"`
	RecoverSec  float64 `json:"recover_sec,omitempty"`
	StallP50ms  float64 `json:"stall_p50_ms,omitempty"`
	StallP95ms  float64 `json:"stall_p95_ms,omitempty"`
	StallP99ms  float64 `json:"stall_p99_ms,omitempty"`
	IntegrityOK bool    `json:"integrity_ok,omitempty"`
	Note        string  `json:"note,omitempty"`
}

// node is the proposer handle (always the current leader).
type node interface {
	propose(data []byte) error
	barrier() error
}

// cluster is one composition's deployment: 1 or 3 voters with durable state
// under dir. Stall/restart/snapshot support the P5/P6/P7 profiles.
type cluster interface {
	leader() (node, error)
	stall(id int)
	unstall(id int)
	restart(id int) error
	applied(id int) uint64
	checksum(id int) uint64
	stateCRC(id int) uint32
	diagnose(id int) string
	leaderID() int
	snapshot() error
	shutdown()
}

// knobs carry the profile's resource sizing (profiles.md §8 limit chain).
type knobs struct {
	voters     int
	stateBytes int // FSM slot buffer (application state, not library overhead)
	maxCommand int // bytes; archive profiles raise this
}

func main() {
	impl := flag.String("impl", "all", "ours|hashicorp|etcd|all")
	profile := flag.String("profile", "P1", "P1|P2|P3|P4|P5|P6|P7|P8")
	voters := flag.Int("voters", 1, "1|3")
	dir := flag.String("dir", "", "data directory (fresh per run)")
	seconds := flag.Int("seconds", 60, "steady/pipelined profile duration")
	burst := flag.Int("burst", 1000, "P2 burst count")
	archiveMB := flag.Int("archive-mb", 32, "P4 command size (8 or 32)")
	pipeline := flag.Int("pipeline", 32, "P8 concurrent proposers")
	flag.Parse()
	runtime.GOMAXPROCS(1) // single-vCPU profile (§8)

	switch *profile {
	case "P3", "P5", "P6":
		*voters = 3
	case "P7":
		*voters = 1
	}
	if *voters != 1 && *voters != 3 {
		fmt.Fprintln(os.Stderr, "voters must be 1 or 3")
		os.Exit(1)
	}
	if *dir == "" {
		d, err := os.MkdirTemp("", "raft-compare-")
		if err != nil {
			panic(err)
		}
		*dir = d
		defer os.RemoveAll(*dir)
	}
	impls := []string{"ours", "hashicorp", "etcd"}
	if *impl != "all" {
		impls = []string{*impl}
	}
	var results []result
	for _, name := range impls {
		r := runCell(name, *profile, *voters, *dir, cellOpts{
			seconds: *seconds, burst: *burst, archiveMB: *archiveMB, pipeline: *pipeline,
		})
		results = append(results, r)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(results)
}

type cellOpts struct {
	seconds   int
	burst     int
	archiveMB int
	pipeline  int
}

func runCell(impl, profile string, voters int, dir string, opts cellOpts) (r result) {
	// A failing cell (e.g. a baseline that loses leadership under
	// GOMAXPROCS=1) records its failure and still emits JSON, so one bad
	// cell cannot block the matrix (profiles.md measurement rules).
	defer func() {
		if rec := recover(); rec != nil {
			r.Note = fmt.Sprintf("cell failed: %v", rec)
			r.Errors++
		}
	}()
	k := knobs{voters: voters, stateBytes: 4 << 10, maxCommand: 1 << 20}
	switch profile {
	case "P4":
		k.maxCommand = opts.archiveMB << 20
	case "P6":
		k.stateBytes = 32 << 20
	case "P7":
		k.stateBytes = 64 << 20
	}
	r = result{Impl: impl, Profile: profile, Voters: voters, ArchiveMB: 0}
	if profile == "P4" {
		r.ArchiveMB = opts.archiveMB
	}
	// Storage-class characterization on the cell's own directory (EBS, not
	// tmpfs; profiles.md measurement rules).
	if p50, p95, p99, err := fsyncProbe(dir); err == nil {
		r.FsyncP50ms, r.FsyncP95ms, r.FsyncP99ms = p50, p95, p99
	} else {
		r.Note = "fsync probe: " + err.Error()
	}
	cacheBefore := cachedMB()
	lat := &latCollector{}
	start := time.Now()
	var pr profileRun
	if profile == "P7" {
		pr = runRestartProfile(impl, dir, k, lat)
	} else {
		cl, err := newCluster(impl, dir, k, lat)
		if err != nil {
			panic(fmt.Sprintf("%s setup: %v", impl, err))
		}
		pr = runProfile(profile, cl, opts)
		cl.shutdown()
	}
	r.TotalSec = time.Since(start).Seconds()
	r.Ops, r.Errors, r.Overloads = pr.ops, pr.errs, pr.overloads
	r.P50ms, r.P95ms, r.P99ms = percentiles(pr.lats)
	if r.TotalSec > 0 {
		r.OpsSec = float64(pr.ops) / r.TotalSec
	}
	for k, v := range pr.extra {
		switch k {
		case "restart_ms":
			r.RestartMs = v
		case "catchup_sec":
			r.CatchupSec = v
		case "recover_sec":
			r.RecoverSec = v
		case "stall_p50":
			r.StallP50ms = v
		case "stall_p95":
			r.StallP95ms = v
		case "stall_p99":
			r.StallP99ms = v
		case "integrity":
			r.IntegrityOK = v == 1
		}
	}
	runtime.GC() // settle live heap before reporting library overhead
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	r.HeapMB = float64(ms.HeapAlloc) / (1 << 20)
	r.RSSMB = rssMB()
	r.CacheDeltaMB = cachedMB() - cacheBefore
	r.StoreP50ms, r.StoreP95ms, r.StoreP99ms = lat.storePercentiles()
	r.ApplyP50ms, r.ApplyP95ms, r.ApplyP99ms = lat.applyPercentiles()
	return r
}

func newCluster(impl, dir string, k knobs, lat *latCollector) (cluster, error) {
	switch impl {
	case "ours":
		return newOursCluster(dir, k, lat)
	case "hashicorp":
		return newHCCluster(dir, k, lat)
	case "etcd":
		return newEtcdCluster(dir, k, lat)
	}
	return nil, fmt.Errorf("unknown impl %s", impl)
}

// profileRun is the measured outcome of one profile.
type profileRun struct {
	lats      []float64 // commit latencies, ms
	ops       int
	errs      int
	overloads int
	extra     map[string]float64
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p / 100 * float64(len(sorted)-1))
	return sorted[i]
}

func percentiles(lats []float64) (p50, p95, p99 float64) {
	s := append([]float64(nil), lats...)
	sort.Float64s(s)
	return percentile(s, 50), percentile(s, 95), percentile(s, 99)
}

// latCollector gathers observer latencies off the measured path (§8).
type latCollector struct {
	mu    sync.Mutex
	store []float64
	apply []float64
}

func (c *latCollector) addStore(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.store) < 2<<20 {
		c.store = append(c.store, float64(d.Microseconds())/1000)
	}
}

func (c *latCollector) addApply(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.apply) < 2<<20 {
		c.apply = append(c.apply, float64(d.Microseconds())/1000)
	}
}

func (c *latCollector) storePercentiles() (p50, p95, p99 float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return percentiles(c.store)
}

func (c *latCollector) applyPercentiles() (p50, p95, p99 float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return percentiles(c.apply)
}

// fsyncProbe characterizes the cell's storage: 200 × (write 1 KiB + fsync).
func fsyncProbe(dir string) (p50, p95, p99 float64, err error) {
	path := filepath.Join(dir, "fsync-probe")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, 0, 0, err
	}
	defer os.Remove(path)
	defer f.Close()
	buf := make([]byte, 1024)
	var lats []float64
	for i := 0; i < 200; i++ {
		t0 := time.Now()
		if _, err := f.WriteAt(buf, 0); err != nil {
			return 0, 0, 0, err
		}
		if err := f.Sync(); err != nil {
			return 0, 0, 0, err
		}
		lats = append(lats, float64(time.Since(t0).Microseconds())/1000)
	}
	p50, p95, p99 = percentiles(lats)
	return p50, p95, p99, nil
}
