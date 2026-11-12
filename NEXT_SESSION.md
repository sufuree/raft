You are continuing **RFC-0001, a deterministic Go Raft library**, from a
near-release-candidate state to **100%: every RFC §11 acceptance gate has
recorded evidence, including the pinned QEMU/KVM unikernel receipt**. The
implementation is done and heavily tested; what remains is qualification
evidence, benchmark completion, and release sign-off. Do not rewrite working
code. Read this whole brief before acting.

## 1. Context and sources of truth

- Repo: `/Users/dndungu/Code/sufuree/raft`, module `github.com/sufuree/raft`.
  **21 commits ahead of `origin/main`, nothing pushed** (working agreement:
  do not push or open PRs without asking, except the pre-approved sibling PR
  below). Working tree clean at handover.
- Spec: `docs/rfc/0001-deterministic-go-raft.md` (unikernel-first: pinned
  Nanos/QEMU-KVM is the initial qualification target; Linux reference/fallback;
  contracts platform-neutral). Read §4–§12, especially §6.2 (gated dispatch
  optimization), §8 (budgets), §11 (acceptance matrix + benchmark design).
- Format spec: `docs/spec/wal-format.md` (WAL v2, both substrate bindings,
  upgrade mechanism). Qualification receipt: `docs/qualification/receipt.md`
  (+ `artifacts/`). Benchmark profiles: `docs/benchmarks/profiles.md`;
  reference-platform report: `docs/benchmarks/report.md`.
- Sibling consumer: `/Users/dndungu/Code/sufuree/sufuree` (local checkout
  stale; work from `origin/main`). **PR #27 is OPEN** (`fix/raftnode-outcome-unknown`,
  ErrOutcomeUnknown vs ErrNotLeader) — pre-approved to open, **do not merge
  without review**. Do not make this library a dependency of sufuree.
- Go go1.27.x. AWS CLI configured (account 394824061124, us-west-1). AWS use
  authorized **only with guaranteed teardown** (§7 below).
- Local tools installed: QEMU 11.1.3 (brew), ops 0.1.47 (`brew install
  nanovms/ops/ops`), Nanos 0.1.56 kernels in `~/.ops/`. **macOS HVF cannot
  boot Nanos arm64 (no GICv2)** — local iteration uses TCG.

## 2. What exists and is verified (do not redo)

Packages: root `raft` (deterministic single-owner Core, five watermarks,
seeded rng, sorted map iteration), `wal` (v2: checkpoint slots, compaction,
streamed snapshot publication, FixedRegionDevice, Preflight), `driver`
(ordered storage loop, one-completion-per-iteration event loop, bounded
workers, staging files), `transport` (TCP+mTLS, authenticated handshake with
incarnation + limits, per-peer budgets, admission ≠ membership), `bootstrap`
(Create/Open/DisasterRecover + FencingAck), `sim` (deterministic simulator,
per-tick invariants, Wing-Gong linearizability checker), `cmd/qualify`
(in-guest platform suite), `bench/compare` (separate module: HashiCorp
v1.8+raft-boltdb v2.4.2/bbolt v1.4.1, etcd raft v3.7 backed by our WAL).

Verified at handover: gofmt/vet/build clean; all tests pass incl. `-race`;
150-seed×400-tick stress identical across runs; core determinism ×3; fuzz
targets (message/conf/batch decode, WAL recovery) clean with corpus in
testdata. Platform evidence R1–R3 in the receipt: Nanos full suite + real
power-loss crash tests PASS under QEMU/TCG on macOS and Linux; Linux
reference (x86_64 c7i + arm64 m8g, AL2023/EBS) full tests + native qualify
PASS. **The QEMU/KVM receipt is the one missing platform gate.**

Benchmarks measured (Linux arm64 EBS gp3, GOMAXPROCS=1, single voter,
sequential proposer): P1/P2 ours p50 5.5ms vs HashiCorp 2.6ms vs etcd 5.5ms
(ours/etcd pay the §6.2 conservative extra commit-watermark fsync — this is
the documented cost, not a bug); P7 restart-to-barrier ours 87ms / etcd 75ms
(≤500ms threshold met); HashiCorp P7 **hangs** under GOMAXPROCS=1+EBS
(baseline finding, recorded, not fully diagnosed). The provisional P1
throughput threshold (≥2000 ops/s) is **not met by any composition with a
sequential proposer** — it conflated latency and throughput; the report
flags it for formal revision.

## 3. Non-negotiable invariants (violating these reintroduces fixed bugs)

1. **Determinism (§5):** core does no I/O, no wall clock, no goroutines;
   never iterate maps into output order (use `sortedProgress()` /
   `sortedProposalIDs()`); randomness only from seeded rng. Stress outcomes
   must be identical across runs.
2. **Single-owner core:** only the driver event loop touches Core. Accessors
   read the atomic stateSnapshot. `DebugProgress` is documented post-Stop only.
3. **`leaderMatch`:** followers commit only through the leader-verified
   matching prefix, never raw lastIndex; reset on leader/term change and
   snapshot install. Heartbeats carry the follower's commit (informational)
   so the leader can probe lagging watermarks — commitment still advances
   only via verified AppendEntries.
4. **Conservative durability (§6.2):** CommitIndex ≤ DurableIndex; dispatch
   only to PersistedCommitIndex; persist commit watermark before apply;
   votes/acks gated via `After` work IDs. **Relaxing this is the gated
   optimization — see §4.2; it requires its own ordering tests first.**
5. **Stale-snapshot guards (§6.4):** at receive AND at install completion;
   watermarks monotonic per instance (except DurableIndex on suffix replace).
6. **Disruption guard (§4.1):** refuse (pre-)vote and refuse step-down for
   higher-term votes while a leader was heard within the election timeout.
7. **Storage generation (§9):** Open rejects stored < expected; disaster
   recovery ADVANCES via wal.AdvanceGeneration and requires FencingAck.
8. **Terminal Failed (§6.2):** any failed write/sync (incl. ENOSPC) or FSM
   apply failure → Failed; recovery = reopen+validate under fresh epoch.
9. **Flow control (hard-won):** one append in flight per peer with
   election-timeout retransmit; cooldown-gated commit probes; idempotent
   appends with no new commit hint ack WITHOUT persisting; event loop
   processes ONE worker completion per iteration (batch draining starves
   network/ticks); streamed snapshots fast-forward on restart via staged-byte
   acks. Regression tests exist for each — keep them passing.
10. **WAL v2 crash safety:** checkpoints are segment-aligned (compaction
    copies must preserve the padding grid); snapshot publications are
    self-contained (hard state + suffix + applied ride with the manifest);
    stale tail = valid-CRC record with seq ≤ lastSeq; dual checkpoint slots
    alternate (never overwrite the newest valid slot).
11. `gofmt`, `go vet`, `-race` stay clean. Add a regression test for every
    bug you fix.

## 4. Remaining work — prioritized

### 4.1 The KVM platform gate [the big one]
- Quota request **already open**: Service Quotas request
  `b80acde3329847d49871f2f3a28792dfLweDhDId`, quota `L-1216C47A` (Running
  On-Demand Standard instances), desired 128 vCPU, us-west-1, status
  CASE_OPENED at handover. Check:
  `aws service-quotas get-requested-service-quota-change --request-id b80acde3329847d49871f2f3a28792dfLweDhDId --region us-west-1`.
- Findings so far: c7i.large exposes **no vmx**; m8g.large (Graviton4) **no
  EL2** ("HYP mode not available"); nested virt needs **bare metal**
  (c7i.metal-24xl, 96 vCPUs — hence the quota). If approved: launch
  c7i.metal-24xl AL2023 x86_64 (AMI via describe-images; reuse §7 pattern),
  run `scripts/aws-qualify.sh` (architecture-aware; installs Go 1.27.4, ops
  from `https://storage.googleapis.com/cli/linux/<arch>/ops`, builds Nanos,
  runs KVM suite + `scripts/nanos-crash-test-x86.sh`). Append the KVM receipt
  to `docs/qualification/receipt.md` with pins (Go/Nanos/QEMU/kernel/arch).
- If quota is denied or stalls: try another region's metal availability, or
  ask the user about a limit increase / different account. **Do not silently
  substitute TCG for KVM** — the RFC requires reporting. A documented
  deviation decision belongs to the user.
- Also qualify the **filesystem substrate on the unikernel** explicitly
  (R1/R3 used an ops volume; record flush/rename/locking behavior per §6.3)
  and guest-total memory accounting (heap + mappings + cache) at 1 vCPU /
  512 MiB.

### 4.2 Benchmark completion + thresholds (release acceptance)
- **Revise profiles formally** (edit `docs/benchmarks/profiles.md` with
  rationale): separate sequential-latency profiles from pipelined-throughput
  profiles; the current P1 ≥2000 ops/s threshold is unmeetable sequentially
  on EBS by ANY composition (fsync-bound). Publish revised thresholds BEFORE
  re-measuring.
- **Measure the missing profiles**: P3 (64 KiB), P4 archive-class (8/32 MiB),
  P5 lagging follower, P6 snapshot catch-up time, and a pipelined-throughput
  profile (multiple outstanding proposals). Extend `bench/compare` as needed
  (keep it a separate module; same FSM/durability across compositions).
- **Three-voter comparisons** (§11 requires 1 and 3 voters). Our driver +
  transport support it; for baselines use their standard multi-node setups
  (HashiCorp inmem transports; etcd with a simple message pump).
- **One-voter unikernel profile first** (§11): run at least our library's
  P1/P2/P7 inside Nanos (extend `cmd/qualify` or a bench mode; serial JSON
  out). Attempt HashiCorp+bbolt inside Nanos as a qualification-risk data
  point (mmap/locking); if it fails, record the failing primitive precisely.
- **Optional, gated:** the §6.2 dispatch optimization (dispatch at
  known-committed + durable prefix, relaxing only CommitIndex ≤ DurableIndex)
  to close the 2× fsync gap. Requires dedicated ordering tests (crash at
  every boundary, no application ahead of local durability) before enabling;
  keep it off by default until qualified. Only pursue if thresholds demand it.
- Update `docs/benchmarks/report.md`; **no numerical win claims** unless
  measured against published thresholds.

### 4.3 Release sign-off
- Update the README gate table with final evidence; every §11 row must point
  at recorded evidence. Declare API stability status honestly (frozen vs
  under qualification).
- Optional/deferred per RFC (do NOT add before gates pass): leadership
  transfer (§4.5), follower/stale reads, resumable snapshot transfer.
- Final full verification (§6), commit. Ask the user before pushing.

## 5. Definition of done (100%)

All §11 gates have recorded evidence **including the pinned QEMU/KVM
unikernel receipt on both substrates at 1 vCPU**; comparative report covers
1-voter unikernel + 1/3-voter Linux, steady/bursty/archive profiles, restart,
memory (incl. mappings+cache), fsync and commit/apply p50/p95/p99, overload —
against published thresholds that are met or formally revised with rationale;
gofmt/vet/-race clean; determinism checks pass; README + receipt + report
updated; work committed; sibling PR #27 remains open for review (not merged).

## 6. Verification commands (run after every change)

```bash
cd /Users/dndungu/Code/sufuree/raft
gofmt -l .                      # must be empty (bench module included)
go vet ./...
go build ./...
go test ./... -count=1 -timeout 300s
go test -race ./... -count=1 -timeout 400s
go test ./sim/ -run TestStress -count=1 -v   # 150 seeds, all PASS
go test . -run TestCoreDeterminism -count=3
go test ./... -run xxx -bench . -benchtime 1000x
(cd bench/compare && go build ./...)         # separate module
```
Determinism: run stress twice, strip timings, diff per-seed outcomes.
Local Nanos iteration (TCG): see `scripts/nanos-crash-test.sh` usage; image
build via `ops build --arch arm64 -c config.json qualify` (configs in
`scripts/aws-nanos-tcg-bench.sh` show the pattern; volume mount
`"Mounts": {"qualvol": "/data"}`, mode via `Env QUAL_MODE`).

## 7. AWS procedure (MANDATORY teardown)

Prior runs tore down completely; **no resources exist at handover**. Pattern:
1. IAM role (trust ec2) + AmazonSSMManagedInstanceCore + **scoped S3 inline
   policy (create it BEFORE the first S3 call — IAM propagation takes
   ~30–60s)**; instance profile; temporary bucket; launch with IMDSv2
   required, public IP, default VPC/subnet, tag `project=raft-qual`.
2. `COPYFILE_DISABLE=1 tar --exclude=.git -czf` (macOS tar otherwise injects
   `._*` AppleDouble files that break Go fuzz corpora); upload; SSM
   AWS-RunShellScript with `HOME=/root GOCACHE=... GOPATH=... PATH+=/usr/local/go/bin`;
   **extract to a CLEAN directory** (tar over-extraction leaves stale files).
3. AL2023 gotchas: `/tmp` is **tmpfs** (benchmarks must use EBS paths);
   QEMU packages have **no slirp** (`-netdev user` fails — the qualify suite
   needs no NIC); `qemu-system-*-core` may lack features, install full
   `qemu-system-*`; ops installer insists on x86 QEMU even on arm64 — install
   the binary directly from `https://storage.googleapis.com/cli/linux/<arch>/ops`.
4. SSM gotchas: `pkill -f <pattern>` matches the SSM shell's own cmdline —
   use `pkill -x`; command Status can report TimedOut/Failed while the work
   actually completed — check the on-instance logs; pass complex commands via
   `--parameters file://...` (local shell eats `$VARS` in inline JSON).
5. **Teardown + verify**: terminate → `wait instance-terminated` → empty +
   delete bucket → remove role from profile → delete profile → detach/delete
   role policies → delete role → delete SG. Verify: terminated, bucket 404,
   role/profile NoSuchEntity, no `project=raft-qual` instances in any
   non-terminal state. Never leave resources running; tear down even on
   failure.

## 8. Working agreements

- Commit early and often; clear messages; **do not push or open PRs without
  asking** (sibling PR #27 already open — do not merge it).
- Preserve all §3 invariants; regression test every fix; keep the core
  deterministic and single-owner; contracts platform-neutral.
- Report honest status; simulation ≠ real durability; no gate claimed
  without evidence; no performance win claimed without measurement against
  published thresholds.
- Prefer extending existing packages; match the dense RFC-referencing doc
  comment style.
- Known baseline quirk (do not chase as a library bug): HashiCorp v1.8+bbolt
  hangs in the P7 seed/reopen under GOMAXPROCS=1 on EBS; bench cells are
  individually timed (`scripts/aws-bench-matrix.sh`) so one stuck cell cannot
  block the matrix.

Start by checking the quota request status, then proceed down §4.
