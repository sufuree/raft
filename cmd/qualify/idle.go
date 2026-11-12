package main

// Idle-CPU mode (RFC-0001 §8: the driver must idle near zero CPU). It starts a
// single-voter driver, reaches leadership, then idles with no client traffic
// while the host samples the VMM process CPU over the window. Guest heap and
// runtime mappings are printed at the end.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

func runIdle() {
	secs := benchEnvInt("QUAL_IDLE_SECONDS", 60)
	dir := filepath.Join(workDir, "idle")
	os.RemoveAll(dir)
	lat := &latObs{}
	n, err := newBenchNode(dir, 4<<10, 1<<20, lat)
	if err != nil {
		report("idle-start", err, "")
		return
	}
	defer n.close()
	fmt.Printf("QUAL idle BEGIN seconds=%d\n", secs)
	time.Sleep(time.Duration(secs) * time.Second)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Printf("QUAL idle END heapAlloc=%d heapSys=%d sys=%d numGC=%d goroutines=%d\n",
		ms.HeapAlloc, ms.HeapSys, ms.Sys, ms.NumGC, runtime.NumGoroutine())
	report("idle", nil, fmt.Sprintf("idled %ds with no client traffic", secs))
}
