// Command compare benchmarks the RFC-0001 library against pinned baseline
// compositions (HashiCorp Raft + raft-boltdb/bbolt; etcd Raft + WAL adapter)
// under the published workload profiles (docs/benchmarks/profiles.md).
//
// All compositions run the same application FSM and acknowledged-durability
// semantics: every committed batch is fsynced before the client observes
// success. Single-voter profiles run first (§11).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"time"
)

type result struct {
	Impl     string  `json:"impl"`
	Profile  string  `json:"profile"`
	Ops      int     `json:"ops"`
	Errors   int     `json:"errors"`
	P50ms    float64 `json:"p50_ms"`
	P95ms    float64 `json:"p95_ms"`
	P99ms    float64 `json:"p99_ms"`
	TotalSec float64 `json:"total_sec"`
	OpsSec   float64 `json:"ops_sec"`
	HeapMB   float64 `json:"heap_mb"`
}

type node interface {
	propose(data []byte) error
	barrier() error
	shutdown()
}

func main() {
	impl := flag.String("impl", "all", "ours|hashicorp|etcd|all")
	profile := flag.String("profile", "P1", "P1|P2|P7")
	dir := flag.String("dir", "", "data directory (fresh per run)")
	flag.Parse()
	runtime.GOMAXPROCS(1) // single-vCPU profile
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
		var r result
		if *profile == "P7" {
			// runRestart manages its own node lifecycle (seed, shutdown, reopen).
			r = runRestart(name, *dir)
			results = append(results, r)
			continue
		}
		n, err := newNode(name, *dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: setup: %v\n", name, err)
			os.Exit(1)
		}
		switch *profile {
		case "P1":
			r = runSteady(name, n, 500, 10*time.Second)
		case "P2":
			r = runBurst(name, n, 1000)
		default:
			fmt.Fprintf(os.Stderr, "unknown profile %s\n", *profile)
			os.Exit(1)
		}
		n.shutdown()
		results = append(results, r)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(results)
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p / 100 * float64(len(sorted)-1))
	return sorted[i]
}

func summarize(impl, profile string, lat []float64, errs int, total time.Duration) result {
	sort.Float64s(lat)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	ops := len(lat)
	return result{
		Impl: impl, Profile: profile, Ops: ops, Errors: errs,
		P50ms: percentile(lat, 50), P95ms: percentile(lat, 95), P99ms: percentile(lat, 99),
		TotalSec: total.Seconds(), OpsSec: float64(ops) / total.Seconds(),
		HeapMB: float64(ms.HeapAlloc) / (1 << 20),
	}
}

// runSteady: P1 — fixed-rate 1 KiB proposals.
func runSteady(impl string, n node, rate int, dur time.Duration) result {
	payload := make([]byte, 1024)
	interval := time.Second / time.Duration(rate)
	var lat []float64
	errs := 0
	start := time.Now()
	deadline := start.Add(dur)
	for time.Now().Before(deadline) {
		t0 := time.Now()
		if err := n.propose(payload); err != nil {
			errs++
		} else {
			lat = append(lat, float64(time.Since(t0).Microseconds())/1000)
		}
		if d := interval - time.Since(t0); d > 0 {
			time.Sleep(d)
		}
	}
	return summarize(impl, "P1", lat, errs, time.Since(start))
}

// runBurst: P2 — count proposals as fast as admission allows.
func runBurst(impl string, n node, count int) result {
	payload := make([]byte, 1024)
	var lat []float64
	errs := 0
	start := time.Now()
	for i := 0; i < count; i++ {
		t0 := time.Now()
		if err := n.propose(payload); err != nil {
			errs++
		} else {
			lat = append(lat, float64(time.Since(t0).Microseconds())/1000)
		}
	}
	return summarize(impl, "P2", lat, errs, time.Since(start))
}
