package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// runRestart: P7 — cold open to serving after a prior run wrote state.
func runRestart(impl, dir string) result {
	// Seed state with a fresh node, shut it down, then time the reopen.
	n, err := newNode(impl, dir)
	if err != nil {
		panic(err)
	}
	payload := make([]byte, 1024)
	for i := 0; i < 1000; i++ {
		if err := n.propose(payload); err != nil {
			panic(err)
		}
	}
	n.shutdown()

	rdir := filepath.Join(dir, "restart")
	os.MkdirAll(rdir, 0o755)
	// Reopen in place: implementations recover from their own data dir.
	t0 := time.Now()
	n2, err := newNode(impl, dir)
	if err != nil {
		panic(err)
	}
	if err := n2.barrier(); err != nil {
		panic(err)
	}
	elapsed := time.Since(t0)
	n2.shutdown()
	return result{
		Impl: impl, Profile: "P7", Ops: 1,
		TotalSec: elapsed.Seconds(), P50ms: float64(elapsed.Microseconds()) / 1000,
	}
}

func newNode(impl, dir string) (node, error) {
	switch impl {
	case "ours":
		return newOurs(dir)
	case "hashicorp":
		return newHashicorp(dir)
	case "etcd":
		return newEtcd(dir)
	}
	return nil, fmt.Errorf("unknown impl %s", impl)
}
