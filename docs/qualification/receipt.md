# Platform qualification receipt (RFC-0001 §11 platform gate)

Status: **platform gate passed (R4, pinned QEMU/KVM)**. This receipt records
measured evidence; gates are not claimed passed without it. Simulation and Go
tests are not proof of real filesystem durability; the power-loss tests below
are. The pinned primary platform (Nanos on QEMU/KVM, 1 vCPU, both substrates)
is qualified in R4; Linux reference evidence is R2/R3/R4.

## Pinned versions

| Component | Version | Notes |
| --- | --- | --- |
| Go | go1.27.4 | linux/amd64, linux/arm64, darwin/arm64 |
| Nanos | 0.1.56 (arm64 + x86_64 kernels) | ops 0.1.47 (brew) / ops cli linux builds |
| QEMU | 11.1.3 (macOS dev), 11.0.0 (AL2023) | |
| Linux reference | Amazon Linux 2023, kernel 6.1 | x86_64 (c7i.large, c7i.metal-24xl) and arm64 (m8g.large), EBS gp3 |
| KVM host (R4) | c7i.metal-24xl bare metal, Intel Xeon Platinum 8488C | us-west-1, AL2023 kernel 6.1.190, vmx, /dev/kvm; guest 1 vCPU / 512 MiB |
| Acceleration | see per-run table | R1/R3 TCG; R4 KVM (pinned receipt) |

## Runs

### R1 — macOS dev loop (QEMU 11.1.3, TCG, arm64)

HVF cannot run Nanos arm64 (no GICv2 emulation); TCG used for iteration.
Guest: 1 vCPU, 512 MiB. Volume: 128 MiB ops volume (TFS), virtio-blk.

| Test | Result |
| --- | --- |
| wal-file (write/sync/reopen/snapshot/reclaim) | PASS |
| wal-block (fixed region, no-op truncate, ENOSPC) | PASS |
| locking (flock advisory) | PASS |
| single-voter: 100 commits | PASS (~67 ms) |
| single-voter: restart-to-leader | PASS (~24 ms) |
| 3-voter in-guest TCP cluster + read barrier | PASS |
| resources (heap ~10 MiB, GOMAXPROCS=1) | PASS |
| power-loss: kill -9 QEMU, reboot same volume | PASS (durable prefix intact, unsynced tail discarded) |

### R2 — Linux reference, x86_64 (c7i.large, AL2023, EBS gp3)

| Test | Result |
| --- | --- |
| go test ./... (all packages) | PASS |
| go test -race ./... | PASS |
| native qualify, /data (EBS) | PASS — 100 commits 572 ms cold; restart-to-leader 3.5 ms |
| native qualify, tmp | PASS — 100 commits 1.1 ms; restart 1.2 ms |
| /dev/kvm | **absent** (c7i.large exposes no vmx; nested virtualization not available on this instance) |

### R3 — Linux reference, arm64 (m8g.large, AL2023, EBS gp3)

| Test | Result |
| --- | --- |
| go test ./... (all packages) | PASS |
| go test -race ./... | PASS |
| native qualify, /data (EBS) | PASS — 100 commits 563 ms; restart-to-leader 2.9 ms |
| native qualify, tmp | PASS — 100 commits 1.2 ms; restart 0.9 ms |
| /dev/kvm | **absent** (Graviton4: "HYP mode not available"; no EL2 exposed) |
| Nanos QEMU/TCG full suite (1 vCPU, 512 MiB, virtio-blk volume) | PASS — wal-file, wal-block, locking, single-voter (100 commits 425 ms; restart-to-leader 47.6 ms), 3-voter cluster, resources (heap ~10 MiB) |
| Nanos QEMU/TCG power-loss crash test | PASS — kill -9 QEMU, reboot same volume: durable prefix intact, unsynced tail discarded |
| Comparative benchmarks (EBS gp3, GOMAXPROCS=1) | recorded in [../benchmarks/report.md](../benchmarks/report.md) |

## Resource teardown (verified)

All qualification resources were torn down and verified after each run. R1–R3:
both instances `terminated`, transfer bucket 404, IAM role and instance profile
`NoSuchEntity`, security group deleted, no `project=raft-qual` instances
remaining. R4 (bare metal): the c7i.metal-24xl instance `terminated`, transfer
bucket 404, IAM role and instance profile `NoSuchEntity`, security group
`InvalidGroup.NotFound`, no `project=raft-qual` instances in any non-terminal
state, no `raft-qual-*` buckets or roles remaining. The vCPU quota increase
(request `b80acde3329847d49871f2f3a28792dfLweDhDId`, 128 vCPU) was approved and
enabled the R4 bare-metal run; a quota creates no resources by itself.

## R4 — pinned QEMU/KVM unikernel receipt (bare metal, x86_64)

The RFC pins QEMU/KVM. Neither c7i.large nor m8g.large exposes nested
virtualization (verified in R2/R3: no vmx / no EL2). The vCPU quota increase
(Service Quotas request `b80acde3329847d49871f2f3a28792dfLweDhDId`, 128 vCPU)
was **approved**, enabling a bare-metal run with guaranteed vmx.

Host: c7i.metal-24xl (Intel Xeon Platinum 8488C, 96 vCPU, 192 GiB), us-west-1,
AL2023 kernel 6.1.190, `/dev/kvm` present (`kvm_intel`, vmx). Guest: Nanos
0.1.56 x86_64 kernel, QEMU 11.0.0 `-machine q35 -accel kvm -cpu host`, 1 vCPU,
512 MiB, virtio-scsi root + volume, Go go1.27.4 linux/amd64, ops 0.1.47.
Artifacts: `artifacts/r4-*`.

### Linux reference on the metal host (GOMAXPROCS=2)

| Test | Result |
| --- | --- |
| go test ./... (all packages) | PASS |
| go test -race ./... | PASS |
| native qualify, EBS (`/root/qual-data`) | PASS — wal-file, wal-block, locking, fssub-\*, single-voter (100 commits 243 ms; restart-to-leader 2.2 ms), 3-voter cluster, resources (heap ~11 MiB) |

GOMAXPROCS is pinned to 2 for the test runs: the library targets single-vCPU
operation (§5, §8) and the reference receipt (R2/R3) ran on 2-vCPU instances.
On this 96-vCPU host, `-race` scheduling jitter overshoots the tests' election
timeouts (timing-sensitive assertions, **not** data races — no `DATA RACE`
reports); `TestLargeSnapshotStreamedCatchUp` was also hardened to the 5 ms
qualify tick cadence. The race detector itself is clean.

### Nanos full suite under KVM (1 vCPU, 512 MiB) — `r4-kvm-full.log`

| Test | Result |
| --- | --- |
| wal-file (TFS: write/sync/reopen/snapshot/reclaim) | PASS |
| wal-block (fixed raw region, no-op truncate, ENOSPC) | PASS |
| locking (flock advisory) | PASS |
| fssub-flush (write+fsync+reopen) | PASS |
| fssub-rename (write+fsync+rename-over) | PASS |
| fssub-dirsync (directory fsync) | PASS (accepted) |
| fssub-create-remove | PASS |
| single-voter: 100 commits | PASS (224 ms; restart-to-leader 2.9 ms) |
| 3-voter in-guest cluster + read barrier | PASS |
| resources (GOMAXPROCS=1) | PASS (heap ~10 MiB) |

Both substrate bindings are qualified on the unikernel under KVM. The §6.3
filesystem primitives are recorded explicitly: TFS honors write+fsync, atomic
rename-over, directory fsync, and create/remove; flock is advisory (not an
ownership fence — exclusive volume attachment is the fence, §9).

### Power-loss crash test under KVM — `r4-crash-test.log`

`kill -9` of QEMU after durable + unsynced writes, reboot the same volume:
**PASS** — durable prefix survived, unsynced tail discarded (`crashverify PASS`,
`CRASHTEST PASS`). QEMU drive cache is writeback; guest flushes are honored by
the host, so synced data survives and unsynced data may disappear (§11 crash
model).

### In-guest benchmark profiles under KVM — `r4-kvm-bench.log`

Single voter, 1 KiB unless noted, closed-loop sequential (P8 pipelined, 32
proposers). Thresholds: [../benchmarks/profiles.md](../benchmarks/profiles.md).

| Profile | ops | p50 ms | p99 ms | ops/s | Threshold | Met |
| --- | --- | --- | --- | --- | --- | --- |
| P1 steady-small | 19327 | 3.07 | 4.17 | 322 | p50≤8/p99≤20; ≥100 ops/s | yes |
| P2 burst-small | 1000 | 3.07 | 4.00 | 323 | p99≤100 | yes |
| P4 archive 8 MiB | 30 | 22.8 | 30.3 | 1 | p99≤1 s | yes |
| P4 archive 32 MiB | 30 | 78.0 | 95.1 | 1 | p99≤2 s | yes |
| P8 pipelined | 24750 | 78.6 | 127.3 | 412 | ≥150 ops/s; p99≤250; no regression vs P1 | yes |
| P7 restart (64 MiB snap + 1000 suffix) | 1 | — | — | — | ≤500 ms; integrity | yes (213 ms, integrity verified) |

The guest fsync probe reports ≈0 ms (TFS acknowledges virtio flushes quickly);
the commit latencies above reflect the real durability cost, and the power-loss
test proves synced data survives. P8 confirms the conservative no-coalescing
policy stays fsync-bound under pipelining (412 vs 322 ops/s; profiles.md rev2.1
finding) — not a safety defect.

### Guest-total memory accounting — `r4-kvm-bench.rss`

Peak host VMM RSS during the bench = **561784 KiB ≈ 548 MiB** for the 512 MiB
guest (512 MiB guest address space + ~36 MiB QEMU overhead). All guest memory —
Go heap, runtime mappings, and TFS kernel cache — lives in the VMM RSS. Guest
Go runtime mappings (`MemStats.Sys`) peaked at ~277 MiB during P7 (64 MiB FSM
state + snapshot publication buffers); live heap stayed ≤0.1 MiB after GC for
the trivial-FSM profiles. The guest fits its 512 MiB budget; TFS cache fills
available guest memory. Library overhead (heap, excl. FSM state) is far below
the 128 MiB threshold; archive/profile peak mappings are application-data-driven
and reported transparently.

### Idle CPU under KVM — `r4-kvm-idle.log`, `r4-kvm-idle.cpu`

Single-voter driver at leadership, 60 s with no client traffic: host-sampled
VMM CPU = **0 ticks / 50 s (0%)** ≤ 1% threshold. Guest heap ~3.8 MiB, 3
goroutines. The driver idles near zero CPU (§8).

### Incumbent storage probe under KVM — `r4-kvm-bboltprobe.log`

bbolt + HashiCorp Raft, step by step: open (flock + initial mmap), write+fsync,
mmap growth (1000 puts), read-only View, close/reopen (flock reacquire),
raft-boltdb store, NewRaft, single-voter election, 100 commits (254 ms), read
barrier, shutdown — **all PASS**. bbolt's memory-mapping growth, file locking,
and synchronization are **not** blockers on Nanos 0.1.56/KVM. (The HashiCorp P7
hang recorded in the benchmark report is a Linux GOMAXPROCS=1+EBS scheduling
interaction, not a Nanos incompatibility.)

### Platform gate status

The pinned QEMU/KVM unikernel platform is qualified on **both substrates at one
virtual CPU**, with flush/rename/locking recorded (§6.3), real power-loss
recovery, guest-total memory and idle-CPU accounting, and the in-guest benchmark
profiles meeting the published thresholds. R1–R3 remain valid TCG/native
evidence; R4 is the pinned KVM receipt.

## R3 addendum — Nanos on Linux/TCG

AL2023 QEMU 11.0.0 ships without slirp (user-mode networking); the qualify
suite uses the in-process LoopNetwork, so the guest runs without a NIC.
Nanos 0.1.56-arm boots under `-machine virt,gic-version=2 -accel tcg
-cpu cortex-a72` with virtio-blk root + volume. All in-guest gates and the
two-phase power-loss test pass (see R3 table). This is QEMU/TCG evidence,
not the pinned QEMU/KVM receipt.

## Failure primitives observed

- macOS HVF: GICv2 unsupported → Nanos arm64 cannot boot under HVF. This is a
  host-accelerator limitation, not a Nanos or library incompatibility.
- No library primitive failures observed on Nanos to date: filesystem
  writes+fsync, flock, in-process transport, virtio-blk volumes, and process
  restart all behave per contract in R1 and R3.
- AL2023 QEMU lacks slirp: guest NIC unavailable with `-netdev user`; not a
  library or Nanos incompatibility (suite needs no external network).
- HashiCorp Raft v1.8 + bbolt baseline hangs under GOMAXPROCS=1 on EBS in the
  P7 profile (benchmark finding, not a library failure).

## Method notes

- Power loss = `kill -9` of the QEMU process while the guest hangs after
  writing durable + unsynced state; recovery boots the same volume image.
  QEMU drive cache is default (writeback): guest flushes are honored by the
  host, so synced data survives and unsynced data may disappear — exactly
  the §11 crash model.
- The qualify program runs identically on Linux and Nanos (cmd/qualify);
  scripts/nanos-crash-test*.sh orchestrate the two-phase crash test.

## Known limitations (non-blocking, recorded for release honesty)

A headless code review of the release PR (three passes) confirmed the §11
evidence and surfaced the following defense-in-depth gaps. None is a safety
defect or a release blocker; each is recorded rather than silently left:

- **Adapter epoch is not persisted.** `wal.Load`/`MemStorage.Load` return
  `Epoch == 0`, so the driver computes epoch 1 on every reopen (§5 intends an
  increment per restart). Stale-completion rejection currently rests on each
  runtime instance owning fresh event channels and work-ID maps, which the
  single-owner driver guarantees; the epoch is a secondary guard not yet
  realized end-to-end in the reference adapters.
- **`DurableIndex` does not decrease on suffix replacement.** `onPersisted`
  only advances it, so after an uncommitted truncation it can overstate the
  recoverable prefix until re-appended. Dispatch remains safe (gated on
  `PersistedCommitIndex` and the ordered store loop); §7 permits the decrease
  and a clamp at truncation-persist completion is future work.
- **Immediate append-ack under a new term.** The first append-ack after term
  adoption can be sent (`After == 0`) while the entries' persist is still in
  flight; it asserts only the already-durable prefix, and commitment requires a
  current-term durable entry. Vote grants and candidate messages are gated on
  hard-state persistence. This is the same durability class as the
  `DurableIndex` item and is pre-existing.

These are tracked as future hardening, not qualifications claimed. The
conservative §6.2 dispatch optimization and leader append coalescing (the
throughput gap in the benchmark report) likewise remain gated future work.
