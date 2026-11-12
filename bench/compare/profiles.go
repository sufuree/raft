package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sufuree/raft/driver"
)

// isOverload recognizes typed admission rejection (§8); the overload
// threshold applies to the library. Baseline errors are counted, not typed.
func isOverload(err error) bool { return errors.Is(err, driver.ErrOverload) }

func runProfile(profile string, cl cluster, opts cellOpts) profileRun {
	leader, err := cl.leader()
	if err != nil {
		panic(err)
	}
	switch profile {
	case "P1":
		return sequential(leader, make([]byte, 1024), time.Duration(opts.seconds)*time.Second, 0)
	case "P2":
		return burst(leader, make([]byte, 1024), opts.burst)
	case "P3":
		return sequential(leader, make([]byte, 64<<10), time.Duration(opts.seconds)*time.Second, 0)
	case "P4":
		return sequential(leader, make([]byte, opts.archiveMB<<20), 0, time.Second, 30)
	case "P5":
		return runLagging(cl, leader, opts)
	case "P6":
		return runCatchup(cl, leader)
	case "P8":
		return pipelined(leader, make([]byte, 1024), opts.pipeline, time.Duration(opts.seconds)*time.Second)
	}
	panic("unknown profile " + profile)
}

// sequential is the closed-loop proposer: one outstanding proposal, next
// admission immediately after the previous applied result (profiles.md rev2).
// pace > 0 targets a maximum rate (P4); count > 0 bounds ops instead of time.
func sequential(n node, payload []byte, dur, pace time.Duration, count ...int) profileRun {
	var pr profileRun
	limit := 0
	if len(count) > 0 {
		limit = count[0]
	}
	start := time.Now()
	deadline := start.Add(dur)
	for {
		if limit > 0 && pr.ops+pr.errs >= limit {
			break
		}
		if limit == 0 && !time.Now().Before(deadline) {
			break
		}
		t0 := time.Now()
		err := n.propose(payload)
		if err != nil {
			pr.errs++
			if isOverload(err) {
				pr.overloads++
			}
		} else {
			pr.ops++
			pr.lats = append(pr.lats, float64(time.Since(t0).Microseconds())/1000)
		}
		if pace > 0 {
			if d := pace - time.Since(t0); d > 0 {
				time.Sleep(d)
			}
		}
	}
	return pr
}

// burst: P2 — count proposals as fast as admission allows (sequential).
func burst(n node, payload []byte, count int) profileRun {
	return sequential(n, payload, 0, 0, count)
}

// pipelined: P8 — workers concurrent closed-loop proposers (group commit).
func pipelined(n node, payload []byte, workers int, dur time.Duration) profileRun {
	var mu sync.Mutex
	var pr profileRun
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := append([]byte(nil), payload...)
			var lats []float64
			var ops, errs, overloads int
			for time.Now().Before(deadline) {
				t0 := time.Now()
				err := n.propose(buf)
				if err != nil {
					errs++
					if isOverload(err) {
						overloads++
					}
					continue
				}
				ops++
				lats = append(lats, float64(time.Since(t0).Microseconds())/1000)
			}
			mu.Lock()
			pr.lats = append(pr.lats, lats...)
			pr.ops += ops
			pr.errs += errs
			pr.overloads += overloads
			mu.Unlock()
		}()
	}
	wg.Wait()
	return pr
}

// seedPipeline writes count entries with 8 concurrent proposers (unmeasured
// setup for P6/P7). Transient leadership churn (a baseline characteristic
// under GOMAXPROCS=1) is retried against the current leader.
func seedPipeline(cl cluster, count int) {
	var wg sync.WaitGroup
	per := count / 8
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1024)
			n, err := cl.leader()
			if err != nil {
				panic(fmt.Sprintf("seed leader: %v", err))
			}
			for i := 0; i < per; i++ {
				var lastErr error
				for attempt := 0; attempt < 100; attempt++ {
					if lastErr = n.propose(buf); lastErr == nil {
						break
					}
					time.Sleep(20 * time.Millisecond)
					if ln, e := cl.leader(); e == nil {
						n = ln
					}
				}
				if lastErr != nil {
					panic(fmt.Sprintf("seed: %v", lastErr))
				}
			}
		}()
	}
	wg.Wait()
}

// runLagging: P5 — P1 load with one follower fully stalled 30 s. The majority
// must keep committing within the P1 3-voter bounds; the follower must catch
// up after the stall heals (time recorded).
func runLagging(cl cluster, leader node, opts cellOpts) profileRun {
	stallID := 3
	warm := sequential(leader, make([]byte, 1024), 5*time.Second, 0)
	cl.stall(stallID)
	stalled := sequential(leader, make([]byte, 1024), 30*time.Second, 0)
	cl.unstall(stallID)
	heal := sequential(leader, make([]byte, 1024), 5*time.Second, 0)
	pr := profileRun{extra: map[string]float64{}}
	pr.lats = append(pr.lats, warm.lats...)
	pr.lats = append(pr.lats, stalled.lats...)
	pr.lats = append(pr.lats, heal.lats...)
	pr.ops = warm.ops + stalled.ops + heal.ops
	pr.errs = warm.errs + stalled.errs + heal.errs
	pr.overloads = warm.overloads + stalled.overloads + heal.overloads
	s50, s95, s99 := percentiles(stalled.lats)
	pr.extra["stall_p50"], pr.extra["stall_p95"], pr.extra["stall_p99"] = s50, s95, s99
	// Catch-up after heal: follower applied must reach the leader's.
	target := cl.applied(1)
	t0 := time.Now()
	deadline := time.Now().Add(60 * time.Second)
	for cl.applied(stallID) < target && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if cl.applied(stallID) < target {
		panic("P5: follower did not catch up after unstall")
	}
	pr.extra["recover_sec"] = time.Since(t0).Seconds()
	return pr
}

// runCatchup: P6 — follower restarted after leader compaction; a 32 MiB
// snapshot must stream, install, and verify end to end within the threshold.
func runCatchup(cl cluster, leader node) profileRun {
	pr := profileRun{extra: map[string]float64{}}
	stallID := 3
	seedPipeline(cl, 3000)
	cl.stall(stallID)
	// Advance the majority past the stalled follower, then snapshot+compact.
	post := sequential(leader, make([]byte, 1024), 0, 0, 200)
	if err := cl.snapshot(); err != nil {
		panic(fmt.Sprintf("P6 snapshot: %v", err))
	}
	suffix := sequential(leader, make([]byte, 1024), 0, 0, 100)
	if err := leader.barrier(); err != nil {
		panic(err)
	}
	pr.ops = post.ops + suffix.ops
	pr.errs = post.errs + suffix.errs
	pr.lats = append(pr.lats, post.lats...)
	pr.lats = append(pr.lats, suffix.lats...)
	target := cl.applied(1)
	want := cl.checksum(1)
	cl.unstall(stallID)
	t0 := time.Now()
	if err := cl.restart(stallID); err != nil {
		panic(fmt.Sprintf("P6 restart: %v", err))
	}
	deadline := time.Now().Add(60 * time.Second)
	for cl.applied(stallID) < target && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if cl.applied(stallID) < target {
		panic("P6: follower did not catch up via snapshot")
	}
	pr.extra["catchup_sec"] = time.Since(t0).Seconds()
	if cl.checksum(stallID) == want {
		pr.extra["integrity"] = 1
	} else {
		panic("P6: follower state checksum mismatch after snapshot install")
	}
	return pr
}

// runRestartProfile: P7 — 64 MiB snapshot + 1000-entry suffix, then cold
// open → serving (open + restore + bounded replay + read barrier).
func runRestartProfile(impl, dir string, k knobs, lat *latCollector) profileRun {
	pr := profileRun{extra: map[string]float64{}}
	cl, err := newCluster(impl, dir, k, lat)
	if err != nil {
		panic(err)
	}
	leader, err := cl.leader()
	if err != nil {
		panic(err)
	}
	seedPipeline(cl, 2000)
	if err := cl.snapshot(); err != nil {
		panic(fmt.Sprintf("P7 snapshot: %v", err))
	}
	seedPipeline(cl, 1000)
	if err := leader.barrier(); err != nil {
		panic(err)
	}
	wantCRC := cl.stateCRC(1)
	wantApplied := cl.applied(1)
	cl.shutdown()

	t0 := time.Now()
	cl2, err := newCluster(impl, dir, k, lat)
	if err != nil {
		panic(fmt.Sprintf("P7 reopen: %v", err))
	}
	l2, err := cl2.leader()
	if err != nil {
		panic(err)
	}
	if err := l2.barrier(); err != nil {
		panic(err)
	}
	pr.extra["restart_ms"] = float64(time.Since(t0).Microseconds()) / 1000
	// The restored snapshot plus bounded suffix replay must reproduce the
	// application state exactly; applied may advance past the seed end via
	// the new-leader no-op (RFC §4), so compare buffer contents and verify
	// replay reached at least the pre-shutdown applied index.
	if cl2.stateCRC(1) == wantCRC && cl2.applied(1) >= wantApplied {
		pr.extra["integrity"] = 1
	} else {
		panic("P7: state integrity mismatch after restart")
	}
	cl2.shutdown()
	pr.ops = 1
	return pr
}
