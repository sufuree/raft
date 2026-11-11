You are continuing implementation of **RFC-0001, a deterministic Go Raft
library**. A prior session built a working, well-tested v1. Your job is to take
it to a **release candidate that passes the RFC's acceptance matrix on the
primary platform**, not to rewrite it. Read this whole brief before acting.

## 1. Context and sources of truth

- Repo: `/Users/dndungu/Code/sufuree/raft`, module `github.com/sufuree/raft`.
- The spec is `docs/rfc/0001-deterministic-go-raft.md` (already amended to be
  **unikernel-first**: pinned Nanos/QEMU-KVM is the initial qualification target,
  Linux is reference/fallback; contracts stay platform-neutral). Read §4–§12.
- Sibling consumer: `/Users/dndungu/Code/sufuree/sufuree`. Its local checkout is
  stale; the real implementation is on `origin/main` (use `git show
  origin/main:<path>`). It uses HashiCorp Raft + bbolt under
  `docs/adr/0001-control-plane-raft-library.md`. Do **not** make this library a
  dependency of sufuree; the ADR governs adoption.
- Go toolchain: go1.27.x. AWS CLI is configured (account 394824061124, default
  region us-west-1). The user has authorized AWS use **only with guaranteed
  teardown** (see §7).

## 2. What already exists and is validated (do not redo)

~5,300 lines of Go, 33 tests, all passing incl. `-race`, a 150-seed×400-tick
adversarial stress suite, and a real Amazon Linux 2023 arm64 / EBS run on AWS
(since torn down). Nothing is committed yet — **commit early** (see §8).

Packages:
- Root `raft`: deterministic single-owner `Core`. `NewCore(Config, InitState)`,
  `Step(Event) Outputs`. Events: `EvTick`, `EvReceive`, `EvPropose`,
  `EvProposeConfChange`, `EvReadBarrier`, `EvPersisted`, `EvStorageFailed`,
  `EvApplied`, `EvApplyFailed`, `EvSnapshotInstalled`, `EvSnapshotCreated`,
  `EvSnapshotRequest`. Outputs: `Messages` (with `After` work-ID prerequisite),
  `Work` (`WorkPersist`/`WorkApply`/`WorkSnapshotInstall`/`WorkSnapshotCreate`),
  `Reads`, `Proposals`, `Status`. Five watermarks via `Watermarks()`.
- `wal`: segmented WAL over a `Device` interface (`FileDevice`, crashable
  `MemDevice`). Checksummed atomic batch records; crash-safe `Open` recovery;
  `HasState`, `ReadIdentity`, `AdvanceGeneration`. Implements `raft.Storage`.
- `driver`: reference runtime. `New(Config, Storage, FSM, Transport)`, `Start`,
  `Stop`, `Propose`, `Read`, `ConfChange`, `Snapshot`. Single-owner event loop,
  bounded workers (semaphore), atomic `stateSnapshot` for accessors. `FSM` and
  `Transport` interfaces; `LoopNetwork`/`LoopTransport` test transport.
- `bootstrap`: `Create`, `Open`, `DisasterRecover(opts, FencingAck)` enforcing §9.
- `sim`: deterministic simulator (`New(Config)`, `Run`, `Propose`, `Read`,
  `ConfChange`, `Partition`, `Crash`, `Restart`, `Snapshot`) with per-tick safety
  invariant checks. Doubles as a deterministic driver.

Verified gates: election safety, log matching, no divergent commits, membership
(joint + crash), linearizable read barrier, snapshot catch-up, disruption guard,
crash recovery, core determinism, WAL differential + torn/unsynced/corruption.

## 3. Non-negotiable invariants and hard-won gotchas

Violating these reintroduces bugs the prior session already fixed:

1. **Determinism (§5, §11 gate).** The core does no I/O, no wall clock, no
   goroutines. **Never iterate a map in a way that affects output order** — use
   `sortedProgress()` / `sortedProposalIDs()`. Randomness only from the seeded
   `rng`. `TestCoreDeterminism` and identical stress outcomes across runs must
   hold.
2. **Single-owner core.** Only the driver event loop calls `Core.Step`/reads
   core state. Accessors read the atomic `stateSnapshot`, never the core
   directly. Workers do I/O and post completion events; they never touch core.
3. **`leaderMatch` invariant (the big safety fix).** A follower commits only
   through the prefix verified to match the leader via AppendEntries
   (`leaderMatch`), **never** raw `lastIndex`. Heartbeats must not let a follower
   commit a divergent suffix. Reset `leaderMatch` on leader/term change and
   snapshot install.
4. **Conservative durability (§6.2).** `CommitIndex <= DurableIndex`; dispatch
   only up to `PersistedCommitIndex`; persist commit watermark before apply;
   votes/acks gated on persistence via `After` work IDs. The durable-watermark-
   before-dispatch rule is intentional (relaxing it is a gated optimization).
5. **Stale-snapshot guard (§6.4).** Ignore `InstallSnapshot` with index <=
   commit/applied. Watermarks (except `DurableIndex`) are monotonic per instance.
6. **Disruption guard (§4.1).** Refuse (pre-)vote AND refuse to step down for a
   higher-term vote while a leader was heard within the election timeout.
7. **Storage generation semantics (§9).** Normal `Open` rejects `stored <
   expected` (rollback). Disaster recovery must **advance** via
   `wal.AdvanceGeneration` (new > stored) and requires `FencingAck.Acknowledged`.
   These look numerically opposite — don't conflate them.
8. **Terminal `Failed` (§6.2).** Any failed write/sync (incl. ENOSPC) or FSM
   apply failure → `Failed`; stop all success output; recovery = reopen+validate
   under a fresh epoch (supervisor-automatable), never silent empty restart.
9. **`gofmt`, `go vet`, `-race` must stay clean.**

## 4. Remaining work — prioritized

Work in phases; after each, run the full verification (§6) and commit.

### Phase A — production transport + resource budgets (§8) [highest value]
- Replace the loopback stand-in with a real **authenticated, framed transport**:
  member identities bound to cluster incarnation, explicit wire version, bounded
  frame decode, peer authorization distinct from log membership (admit learners
  / transition peers). Keep `Transport` interface; add a TCP+mTLS implementation.
- **Cluster-wide limit negotiation at every handshake** (join, conf change,
  reconnect): max command+framing <= append batch <= every peer's decode limit;
  reject incompatible peers with a typed error. Limits travel inside the
  authenticated handshake (no unauthenticated downgrade).
- Complete budgets: per-peer in-flight bytes/messages, decode allocation cap,
  snapshot chunk/concurrency caps, read-waiter cap, uncommitted-log growth cap.
  Enforce before attacker-controlled allocation. Typed overload on admission.
- Tests: oversized frame rejected; incompatible-limit peer rejected at handshake;
  lagging follower cannot stall majority; burst stays within byte budget.

### Phase B — storage completeness (§6.3, §6.4)
- **Segment disk reclamation** tied to snapshot retention (currently append-only,
  grows forever). Reclaim only after a complete recoverable snapshot is published
  and required catch-up history is retained; never delete committed recovery
  state. Budget preflight reserves recovery space; ENOSPC → terminal `Failed`.
- **Raw block-device binding test**: exercise `Device` on a real block device
  (Linux loop device on AWS, or a fixed-region file emulating a block device with
  no filesystem assumptions). Qualify flush semantics.
- **Bounded snapshot streaming** for large FSM snapshots: chunked transfer to
  staging files with end-to-end integrity, atomic manifest publication, then
  reclamation. (WAL currently stores snapshots inline, bounded by MaxBatchBytes;
  add a streaming path for snapshots larger than a batch.)
- Tests: reclamation preserves recoverability; interrupted staging never becomes
  a snapshot; disk-full is terminal; large-snapshot catch-up works.

### Phase C — verification depth (§11)
- **Linearizability checker** for read/write histories (the sim checks safety
  invariants but not full linearizability). Record invoke/return histories incl.
  lost replies and leader changes; check with a standard algorithm (e.g.
  Wing-Gong / linearizability via sequential specification). Add to `sim`.
- **Fuzz targets**: WAL recovery (arbitrary bytes), message decode, conf decode,
  batch decode. Run `go test -fuzz` briefly; corpus any crashes.
- More scenarios: leader removal, 1→3 voter transition, learner promotion with
  durable catch-up, PreVote×CheckQuorum interactions, reads arriving after a
  confirmation round was sent.
- Define **numeric workload profiles + thresholds** before benchmarking.

### Phase D — primary-platform qualification (§11 platform gate) [the big one]
- **Nanos unikernel**: build an image with `ops` (install ops + qemu), run the
  library on QEMU/KVM (locally if available, else on an AWS instance). Qualify
  flush, locking, restart, and resource behavior on **both substrates at 1 vCPU**,
  512 MiB-class guest. Pin Go/runtime/Nanos/VMM/arch. Record a receipt.
- If Nanos proves incompatible, document the failing primitive precisely (the
  RFC requires reporting, not silently swapping the platform).
- **Comparative benchmarks** vs pinned **HashiCorp Raft + raft-boltdb/bbolt** and
  **etcd Raft** compositions: one-voter unikernel profile first, then three
  voters; steady/bursty + archive-class (≤32 MiB) commands; restart time,
  resident/peak memory incl. mappings+cache, fsync latency, p50/p95/p99 commit &
  apply latency, throughput, overload. Report library overhead separately. No
  numerical win is claimed until measured against published thresholds.

### Phase E — API, docs, release (§7, §10, §12)
- Freeze the public Go API; thorough doc comments; runnable examples that do NOT
  imply leadership fences external resources.
- Write the **WAL/manifest format spec** + on-disk upgrade mechanism (RFC §6.3
  requires a separate spec before durable implementation is "accepted").
- Update `README.md` (currently says design-only) to reflect the implemented,
  qualified state and the open gates.
- Optional/deferred per RFC: leadership transfer (§4.5), follower/stale reads,
  resumable snapshot transfer. Add only after recovery/membership qualify.

### Phase F — sibling consumer fix (separate repo)
- In `/Users/dndungu/Code/sufuree/sufuree` (work from `origin/main`), fix
  `internal/raftnode/node.go`: `wrapLeader` conflates `raft.ErrLeadershipLost` /
  `raft.ErrRaftShutdown` (outcome **unknown**, may be committed) with
  `ErrNotLeader` (not admitted). Introduce a distinct "outcome unknown" error and
  map those cases to it; keep `ErrNotLeader` only for genuine not-leader-at-
  admission. Add a test. This is a sufuree bug, independent of this library.
  Open a PR; do not merge without review.

## 5. Definition of done (release candidate)

All RFC §11 acceptance gates have recorded evidence: core determinism;
persistence (boundary crash tests, ambiguous corruption stops recovery);
progress; membership; reads/application; client uncertainty; resource bounds;
bootstrap/recovery; **platform support on the pinned unikernel**; consumer
contract docs. Comparative benchmark report exists with pinned baselines and
published thresholds. `go vet`/`gofmt`/`-race` clean. README + format spec
updated. Work committed (and sibling fix PR opened).

## 6. Verification commands (run after every phase)

```bash
cd /Users/dndungu/Code/sufuree/raft
gofmt -l .                      # must be empty
go vet ./...
go build ./...
go test ./... -count=1 -timeout 180s
go test -race ./... -count=1 -timeout 300s
go test ./sim/ -run TestStress -count=1 -v   # 150 seeds, all PASS
go test . -run TestCoreDeterminism -count=3  # identical outputs
go test ./... -run xxx -bench . -benchtime 1000x
```
Determinism check: run the stress suite twice; the per-seed PASS/FAIL outcomes
must be identical (strip timing before hashing).

## 7. AWS qualification procedure (MANDATORY teardown)

The prior session qualified on Linux/EBS via SSM (no SSH keys). Reuse this
pattern for block-device and Nanos work. **Always tag resources
`project=raft-qual`, record IDs, and tear down + verify.** A teardown script
pattern that worked:

1. Create IAM role (trust ec2) + `AmazonSSMManagedInstanceCore` + scoped S3
   policy; instance profile; a temporary S3 transfer bucket; launch `t4g.micro`
   AL2023 arm64 (AMI via `describe-images`), default VPC/subnet, public IP,
   IMDSv2 required.
2. Upload a source tarball (`tar --exclude=.git -czf`) to S3; `aws ssm
   send-command` (AWS-RunShellScript) to download, install Go, and run tests.
   **Set `HOME=/root GOCACHE=... GOPATH=... CGO_ENABLED=1`** (SSM env is minimal;
   `-race` needs `dnf install -y gcc`). Poll `get-command-invocation`.
3. **Teardown**: terminate instance → `aws ec2 wait instance-terminated` → empty
   + delete bucket → remove role from profile → delete profile → delete role
   policies → delete role. **Verify**: instance `terminated`, bucket 404, role &
   profile `NoSuchEntity`, no `project=raft-qual` instances running.

Never leave resources running. If a step fails, still tear down what exists.

## 8. Working agreements

- **Commit early and often** (nothing is committed yet). Make the first commit
  the current validated baseline before changing anything, so progress is safe.
  Use clear messages; do not push or open PRs without asking (except the sibling
  fix PR in Phase F, which the user has pre-approved to open, not merge).
- Preserve all §3 invariants; add a regression test for every bug you fix.
- Keep the core deterministic and single-owner; keep contracts platform-neutral.
- Report honest status against the acceptance matrix; do not claim a gate passes
  without evidence. Simulation ≠ proof of real filesystem durability.
- Prefer extending existing packages over new abstractions; match the existing
  code style (dense doc comments referencing RFC sections).

Start by committing the baseline, then begin Phase A.
