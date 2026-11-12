//go:build !linux

package main

// Memory accounting beyond the Go heap is Linux-observable in this harness;
// other platforms report zero (local runs are smoke tests, not receipts).
func rssMB() float64    { return 0 }
func cachedMB() float64 { return 0 }
