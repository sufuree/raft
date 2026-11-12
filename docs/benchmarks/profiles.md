# Workload profiles and acceptance thresholds (RFC-0001 §8, §11)

**Status:** published thresholds, revision 2 (formal). Per §11, these are
defined *before* the comparative benchmark runs; release acceptance requires
meeting them on the pinned primary platform. No numerical win is claimed until
measured against the pinned baselines under these profiles.

## Revision 2 rationale (supersedes the provisional revision-1 thresholds)

Revision 1 conflated latency and throughput: P1 specified a 500 ops/s
*sustained target rate* with a sequential (one-outstanding) proposer and a
≥2000 ops/s throughput threshold. With one outstanding proposal, throughput is
bounded by the commit latency, which on fsync-bound durable storage is bounded
by the storage's synchronization latency. The R3 reference measurements
(docs/benchmarks/report.md) showed EBS gp3 fsync p50 ≈ 2.7 ms; the conservative
§6.2 delivery policy pays two durability boundaries per sequential commit
(append batch, then commit watermark), so **no composition** can exceed
~370 ops/s sequentially on that storage class, and ~185 ops/s under the
two-boundary policy. The ≥2000 ops/s threshold was unmeetable as specified;
it assumed pipelined admission and group commit.

Revision 2 therefore:

1. Splits **sequential closed-loop latency profiles** (P1–P4: one outstanding
   proposal; the achieved rate is reported, not targeted) from the
   **pipelined-throughput profile** (P8: 32 concurrent proposers; group commit
   amortizes each durability boundary across a batch).
2. Grounds latency thresholds in the measured storage class: two durability
   boundaries × fsync p50 plus scheduling slack, with the fsync probe recorded
   per cell so the storage class is auditable.
3. Implements P7 fully as originally specified (64 MiB snapshot + 1000-entry
   suffix; revision-1 measurements covered the suffix-only variant and are
   labeled as such in the report).
4. Defines the measurement basis for apply latency and memory accounting
   explicitly (below), including mappings and cache.

Thresholds apply to storage of the pinned qualification class: **EBS gp3**
(Linux reference root volume, and the KVM host volume backing the unikernel
guest disk), fsync p50 ≤ 3 ms as verified by the per-cell probe. A cell whose
probe shows a materially different storage class must be reported separately
and is not evidence for these thresholds.

## Pinned platform profile

- **Primary (qualification target):** Nanos unikernel on QEMU/KVM, 1 vCPU,
  512 MiB guest, arm64 or x86_64 (record which), raw-block WAL binding and
  filesystem binding both qualified. Go/runtime/Nanos/VMM versions pinned in
  the qualification receipt.
- **Reference/fallback:** Amazon Linux 2023 (EBS gp3) and the local
  development filesystem; same test suite, results reported separately.

Budgets account for the Go heap, mapped pages, and kernel cache within the
guest total. The driver must idle near zero CPU (≤ 1% of one vCPU measured
over 60 s with no client traffic; on guests, measured host-side on the VMM
process over the idle window).

## Baselines (pinned at measurement time)

1. HashiCorp Raft + raft-boltdb/bbolt (the incumbent composition).
2. etcd Raft + a bounded journal/storage adapter meeting the same durability
   contract (this library's WAL, so acknowledged durability is identical).

Exact module versions, Go version, storage settings, and durability semantics
are recorded in the report. All compositions run the same application FSM and
acknowledged-durability guarantees (fsync per committed batch; no
async-commit shortcuts). Multi-voter baselines use their standard in-process
setups (HashiCorp in-memory transports; etcd with a message pump); the library
uses its in-process loopback transport. All are loopback-class networks.

## Workload profiles

Sequential profiles (P1–P4) use a closed-loop proposer: one outstanding
proposal, next admission immediately after the previous applied result.
Pipelined profiles use N concurrent closed-loop proposers.

| Profile | Voters | Command size | Load shape |
| --- | --- | --- | --- |
| P1 steady-small (sequential latency) | 1, then 3 | 1 KiB | closed-loop, 60 s |
| P2 burst-small (sequential latency) | 1, then 3 | 1 KiB | 1000 ops as fast as admission allows |
| P3 steady-medium (sequential latency) | 3 | 64 KiB | closed-loop, 60 s |
| P4 archive-class (sequential latency) | 1, then 3 | 8 MiB and 32 MiB | 1 op/s target pace, 30 ops; whole-state archives |
| P5 lagging-follower | 3 | 1 KiB | P1 load with one follower fully stalled 30 s, then unstalled |
| P6 snapshot-catch-up | 3 | 1 KiB + 32 MiB snapshot | follower restarted after leader compaction; time to caught-up applied index |
| P7 restart | 1 | 64 MiB snapshot + 1000-entry suffix | cold open → serving (open + restore + bounded replay + read barrier) |
| P8 pipelined-throughput | 1, then 3 | 1 KiB | 32 concurrent proposers, closed-loop each, 60 s |

## Acceptance thresholds (revision 2, primary platform and EBS-class reference)

| Metric | Threshold |
| --- | --- |
| P1 commit latency, 1 voter, p50 / p95 / p99 | ≤ 8 ms / 12 ms / 20 ms |
| P1 commit latency, 3 voters, p50 / p95 / p99 | ≤ 12 ms / 18 ms / 30 ms |
| P1 achieved sequential rate | reported (fsync-bound); ≥ 100 ops/s, 1 voter |
| P2 burst p99 commit | ≤ 100 ms; zero untyped errors; overload typed when over admission caps |
| P3 commit latency p50 / p99 (3 voters) | ≤ 15 ms / 50 ms; achieved rate reported (≥ 60 ops/s) |
| P4 archive commit p99 | 8 MiB ≤ 1 s; 32 MiB ≤ 2 s |
| P5 majority commit during stall | within the P1 3-voter bounds (lagging follower must not stall majority); follower catches up after unstall (time recorded) |
| P6 catch-up time (32 MiB snapshot, loopback) | ≤ 10 s; integrity verified end to end |
| P7 restart to serving (64 MiB snapshot + 1000-entry suffix) | ≤ 500 ms (open + snapshot restore + bounded replay + barrier) |
| P8 sustained throughput (library, conservative policy) | ≥ 150 ops/s (1 voter); ≥ 120 ops/s (3 voters); and no regression below the sequential P1 rate |
| P8 commit p99 | ≤ 250 ms; overload typed when over admission caps |
| Resident/peak memory (library overhead, excl. FSM state) | ≤ 128 MiB across P1–P4 and P8; mappings + cache accounted |
| Idle CPU | ≤ 1% of one vCPU over 60 s |
| Overload behavior | typed rejection before allocation; heap growth ≤ configured budgets + 25% |
| Latency reporting | fsync probe, storage-batch (write+sync), commit, and apply p50/p95/p99 recorded per profile |

Rationale for the latency bounds: the conservative §6.2 policy requires the
append batch and the commit watermark to be durable before application, i.e.
two sequential durability boundaries per closed-loop commit (≈ 2 × 2.7 ms on
the pinned class), plus event-loop/tick scheduling slack; the 3-voter bound
adds one loopback round trip and overlaps the follower's durability with the
leader's second boundary. P4's bounds reflect gp3 baseline throughput
(125 MiB/s): a 32 MiB archive needs ≈ 0.26 s of write plus synchronization and
replication slack.

**P8 amendment (revision 2.1, published before re-measurement).** The original
≥2000 ops/s P8 aspiration assumed leader-side group commit. A code inspection
during harness validation established that the library's leader propose path
emits one single-entry persistence batch per proposal (followers batch received
entries; the leader does not coalesce admitted proposals). Under the
conservative §6.2 policy the ordered storage loop therefore serializes one
append fsync plus one commit-watermark fsync per proposal even when many
proposals are outstanding, so pipelined throughput stays fsync-bound and ≈ the
sequential P1 rate. The baselines coalesce appends (etcd Ready batches;
HashiCorp AppendEntries batching) and do scale under pipelining; their P8
throughput is reported as comparative data. The library's P8 threshold is
accordingly set to its documented conservative design (≥150 ops/s 1-voter,
≥120 ops/s 3-voter, no regression below sequential P1). Closing the gap to
coalescing compositions requires leader append coalescing **and** the gated
§6.2 dispatch optimization — both future work requiring their own ordering
tests (§6.2), not enabled or claimed here. This is recorded as a finding, not a
safety defect: every batch remains fully durable before commit/apply.

Library overhead is reported separately from the host application and its
state-machine footprint. Restart time is a first-class metric (on-demand
guest activation). Regressions and uncertainty are recorded; a faster
happy-path benchmark cannot waive a failed safety gate.

## Measurement rules

- Same FSM, same command payload sizes, same durability semantics across all
  compositions; payload distribution and seeds recorded. Composition-internal
  proposal tracking (e.g. etcd's embedded request IDs) may overwrite payload
  bytes but never changes sizes or the FSM's work.
- Commit latency = proposal admission → applied result at the caller.
- Apply latency = dispatch → FSM completion. The library reports it via the
  driver's apply observer (dispatch timestamp to FSM return). Baselines report
  FSM execution time measured inside their FSM callbacks (their dispatch
  internals are not instrumented); the basis is labeled per composition.
- Storage-batch latency = the ordered storage operation duration (write +
  fsync for appends; watermark update for applied marks), via the driver's
  storage observer for the library. The fsync probe measures pure
  write(1 KiB)+fdatasync latency on the cell's data directory (200 samples,
  percentiles recorded) and characterizes the storage class independently of
  any composition.
- Memory: library overhead = Go heap in use at cell end (trivial FSM); process
  RSS (includes mappings) and the system page-cache delta across the cell are
  recorded on Linux. On the unikernel, guest-total memory is accounted
  host-side (peak VMM process RSS, which contains all guest memory including
  kernel cache) alongside guest MemStats (heap + runtime mappings).
- Each run pins: implementation versions, Go version, hardware/guest,
  filesystem/storage path, transport, and limit configuration.
- One-voter unikernel profile runs first; three-voter comparisons follow.
- Each (profile, implementation, voters) cell runs independently with a
  timeout so a stuck baseline cell cannot block the matrix.
