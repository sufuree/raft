# Workload profiles and acceptance thresholds (RFC-0001 §8, §11)

**Status:** published provisional thresholds. Per §11, these are defined
*before* the comparative benchmark runs; release acceptance requires meeting
them on the pinned primary platform. No numerical win is claimed until
measured against the pinned baselines under these profiles.

## Pinned platform profile

- **Primary (qualification target):** Nanos unikernel on QEMU/KVM, 1 vCPU,
  512 MiB guest, arm64 or x86_64 (record which), raw-block WAL binding and
  filesystem binding both qualified. Go/runtime/Nanos/VMM versions pinned in
  the qualification receipt.
- **Reference/fallback:** Amazon Linux 2023 arm64 (EBS gp3) and the local
  development filesystem; same test suite, results reported separately.

Budgets account for the Go heap, mapped pages, and kernel cache within the
guest total. The driver must idle near zero CPU (≤ 1% of one vCPU measured
over 60 s with no client traffic).

## Baselines (pinned at measurement time)

1. HashiCorp Raft + raft-boltdb/bbolt (the incumbent composition).
2. etcd Raft + a bounded journal/storage adapter meeting the same durability
   contract.

Exact module versions, Go version, storage settings, and durability semantics
are recorded in the report. All compositions run the same application FSM and
acknowledged-durability guarantees (fsync per committed batch; no
async-commit shortcuts).

## Workload profiles

| Profile | Voters | Command size | Load shape |
| --- | --- | --- | --- |
| P1 steady-small | 1, then 3 | 1 KiB | 500 ops/s sustained, 60 s |
| P2 burst-small | 1, then 3 | 1 KiB | 1000 ops in 1 s, then idle; repeat 10× |
| P3 steady-medium | 3 | 64 KiB | 100 ops/s sustained, 60 s |
| P4 archive-class | 1, then 3 | 8 MiB and 32 MiB | 1 op/s, 30 ops; whole-state archives |
| P5 lagging-follower | 3 | 1 KiB | P1 with one follower stalled 30 s |
| P6 snapshot-catch-up | 3 | 1 KiB + 32 MiB snapshot | follower restarted after compaction |
| P7 restart | 1 | 64 MiB snapshot + 1000-entry suffix | cold open → serving |

## Acceptance thresholds (provisional, primary platform)

| Metric | Threshold |
| --- | --- |
| P1 commit latency p50 / p95 / p99 | ≤ 2 ms / 5 ms / 10 ms (1 voter, qualified storage) |
| P1 throughput | ≥ 2000 ops/s sustained at 1 KiB |
| P2 burst p99 commit | ≤ 100 ms; zero untyped errors; overload typed when over admission caps |
| P4 archive 32 MiB commit p99 | ≤ 2 s |
| P5 majority commit during stall | unchanged from P1 (lagging follower must not stall majority) |
| P6 catch-up time (32 MiB snapshot, loopback) | ≤ 10 s; integrity verified end to end |
| P7 restart to serving | ≤ 500 ms (open + snapshot restore + bounded replay) |
| Resident/peak memory (library overhead, excl. FSM state) | ≤ 128 MiB across P1–P4; mappings + cache accounted |
| Idle CPU | ≤ 1% of one vCPU |
| Overload behavior | typed rejection before allocation; heap growth ≤ configured budgets + 25% |
| fsync latency reporting | p50/p95/p99 recorded per profile |

Library overhead is reported separately from the host application and its
state-machine footprint. Restart time is a first-class metric (on-demand
guest activation). Regressions and uncertainty are recorded; a faster
happy-path benchmark cannot waive a failed safety gate.

## Measurement rules

- Same FSM, same command payloads, same durability semantics across all
  compositions; payload distribution and seeds recorded.
- Commit latency = proposal admission → applied result at the caller.
  Apply latency = dispatch → FSM completion. Both reported.
- Memory includes mappings and page cache where observable; on unikernels,
  guest-total accounting.
- Each run pins: implementation versions, Go version, hardware/guest,
  filesystem/storage path, transport, and limit configuration.
- One-voter unikernel profile runs first; three-voter comparisons follow.
