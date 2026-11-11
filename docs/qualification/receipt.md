# Platform qualification receipt (RFC-0001 §11 platform gate)

Status: **in progress**. This receipt records measured evidence; gates are
not claimed passed without it. Simulation and Go tests are not proof of real
filesystem durability; the power-loss tests below are.

## Pinned versions

| Component | Version | Notes |
| --- | --- | --- |
| Go | go1.27.4 | linux/amd64, linux/arm64, darwin/arm64 |
| Nanos | 0.1.56 (arm64 kernel) | ops 0.1.47 (brew) / ops cli linux builds |
| QEMU | 11.1.3 (macOS dev), 11.0.0 (AL2023) | |
| Linux reference | Amazon Linux 2023, kernel 6.1 | x86_64 (c7i.large) and arm64 (m8g.large), EBS gp3 |
| Acceleration | see per-run table | KVM receipt pending (below) |

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
| Nanos QEMU/TCG + comparative benchmarks | pending (run in progress) |

## KVM gate (pending)

The RFC pins QEMU/KVM. Neither c7i.large nor m8g.large exposes nested
virtualization in this account/region (verified: no vmx / no EL2, kvm_intel
"no hardware support", kvm "HYP mode not available"). A vCPU quota increase
for c7i.metal-24xl (bare metal, guaranteed vmx) is requested
(Service Quotas case opened). The KVM receipt will be appended when the
metal run completes. Until then the platform gate is **not claimed passed**;
R1–R3 are real-guest evidence under TCG/native execution, recorded honestly
as such.

## Failure primitives observed

- macOS HVF: GICv2 unsupported → Nanos arm64 cannot boot under HVF. This is a
  host-accelerator limitation, not a Nanos or library incompatibility.
- No library primitive failures observed on Nanos to date: filesystem
  writes+fsync, flock, TCP loopback, virtio-blk volumes, and process restart
  all behave per contract in R1.

## Method notes

- Power loss = `kill -9` of the QEMU process while the guest hangs after
  writing durable + unsynced state; recovery boots the same volume image.
  QEMU drive cache is default (writeback): guest flushes are honored by the
  host, so synced data survives and unsynced data may disappear — exactly
  the §11 crash model.
- The qualify program runs identically on Linux and Nanos (cmd/qualify);
  scripts/nanos-crash-test*.sh orchestrate the two-phase crash test.
