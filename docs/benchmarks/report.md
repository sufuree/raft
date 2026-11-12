# Comparative benchmark report — Linux reference platform (R3)

**Status:** reference-platform measurements. The pinned primary platform
(Nanos on QEMU/KVM) receipt is pending bare-metal availability; unikernel
TCG results are in [../qualification/receipt.md](../qualification/receipt.md).
No numerical win is claimed. Profiles and provisional thresholds:
[profiles.md](profiles.md).

## Composition pins

| Component | Version |
| --- | --- |
| This library | commit at measurement (module `github.com/sufuree/raft`) |
| HashiCorp Raft | v1.8.0 |
| raft-boltdb/v2 | v2.4.2 (bbolt v1.4.1) |
| etcd Raft | go.etcd.io/raft/v3 v3.7.0 |
| Go | go1.27.4 linux/arm64 |
| Host | AWS m8g.large (Graviton4), 2 vCPU allocated, **GOMAXPROCS=1** |
| OS / kernel | Amazon Linux 2023, 6.12.114-139.210.amzn2023.aarch64 |
| Storage | EBS gp3, xfs (`/root`); `/tmp` is tmpfs and was **not** used |
| Durability | fsync per committed batch for all compositions |

All compositions run the same trivial apply FSM, single voter, sequential
proposer (one outstanding proposal), 1 KiB payloads. The etcd composition is
backed by this library's WAL so acknowledged durability is identical;
HashiCorp uses its standard bolt log + stable store. Commit latency =
admission → applied result observed by the caller.

## Results

### P1 — steady 1 KiB (target 500 ops/s, 10 s)

| Composition | ops | errors | p50 ms | p95 ms | p99 ms | ops/s | heap MB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ours | 1820 | 0 | 5.49 | 5.65 | 5.76 | 182.0 | 3.6 |
| hashicorp | 3524 | 0 | 2.57 | 2.90 | 2.98 | 352.4 | 3.2 |
| etcd (+ our WAL) | 1823 | 0 | 5.48 | 5.63 | 5.73 | 182.3 | 3.6 |

### P2 — burst 1000 × 1 KiB (sequential)

| Composition | ops | errors | p50 ms | p95 ms | p99 ms | ops/s | heap MB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ours | 1000 | 0 | 5.49 | 5.64 | 5.73 | 181.9 | 5.5 |
| hashicorp | 1000 | 0 | 2.60 | 2.88 | 2.98 | 430.7 | 3.1 |
| etcd (+ our WAL) | 1000 | 0 | 5.48 | 5.64 | 5.74 | 182.4 | 2.2 |

### P7 — restart to serving (1000-entry suffix, cold reopen + barrier)

| Composition | restart-to-barrier ms |
| --- | --- |
| ours | 86.8 |
| hashicorp | **did not complete** (180 s timeout; see findings) |
| etcd (+ our WAL) | 75.4 |

## Findings

1. **Conservative durability costs one extra fsync per commit.** The RFC §6.2
   initial policy persists the commit watermark before dispatch, so each
   sequential proposal pays two EBS fsyncs (~2.7 ms each) versus one for the
   HashiCorp composition. This fully explains the ~2× per-op latency gap
   (5.5 ms vs 2.6 ms). The §6.2 gated optimization (dispatch at known-committed
   plus durable prefix) is the documented path to close it; it requires its
   own ordering tests before enablement and is not enabled here.
2. **The provisional P1 threshold (≥2000 ops/s, p99 ≤ 10 ms) is not met on
   EBS gp3 by any composition with a sequential proposer** — the threshold
   assumed pipelined admission. With one outstanding proposal, throughput is
   bounded by fsync latency. The threshold must be revised to distinguish
   sequential-latency and pipelined-throughput profiles before release
   acceptance; this report does not waive it.
3. **HashiCorp P7 hang:** under GOMAXPROCS=1 on EBS, the HashiCorp composition
   hung during the seeded proposal loop and on reopen (bbolt flock wait);
   it never completed within 180 s. Our library and the etcd composition
   complete the same profile. This is recorded as a baseline observation, not
   a claim of superiority; the exact scheduling interaction was not fully
   diagnosed.
4. **Memory:** all compositions stay ≤ 6 MB heap for these profiles (library
   overhead only; the FSM is trivial). Mapped pages and page cache are not
   separately accounted in this run; the unikernel receipt will account guest
   totals.
5. Restart time (≤ 500 ms threshold) is met by ours (86.8 ms) and etcd
   (75.4 ms) for a 1000-entry suffix on EBS.

## Reproduction

```sh
cd bench/compare
go run . -profile P1 -impl all -dir /path/on/durable/storage
go run . -profile P2 -impl all -dir ...
go run . -profile P7 -impl all -dir ...
```

`scripts/aws-bench-matrix.sh` runs the full matrix with per-cell timeouts on
the qualification instance. Raw JSON: S3 transfer bucket (recorded in the
qualification receipt; torn down after capture — cells reproduced above).
