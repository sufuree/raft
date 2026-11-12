# RFC-0001: A deterministic Go Raft library

**Status:** Implemented and qualified. The library described by this RFC is built, and every §11 acceptance gate has recorded evidence — including the pinned QEMU/KVM unikernel receipt on both storage substrates at one virtual CPU, and a comparative benchmark matrix against the pinned baselines meeting the published thresholds. This document remains the design specification; the as-built evidence lives in the [qualification receipt](../qualification/receipt.md), the [benchmark report](../benchmarks/report.md), and the test suite. No correctness *proof* is claimed (the evidence is simulation, fuzzing, and real crash/restart testing, not a formal proof); no performance *win* is claimed (the conservative durability policy's costs are reported against the baselines).

**Date:** November 1, 2026. **Revised:** November 10, 2026 (unikernel-first qualification, Phase-0 decision gate, review fixes); November 12, 2026 (implementation qualified, §11 gates passed, release sign-off). **Scope:** An independent, general-purpose Go library implementing Raft, with a reference storage adapter and runtime driver. Contracts are platform-neutral; the initial qualification target is a pinned Go unikernel (Nanos on QEMU/KVM), with Linux as a reference and fallback platform.

## 1. Proposal and decision status

This RFC specifies the new-core option, which was built and qualified under the section 12 decision gate. The library is a small Raft implementation whose protocol core is deterministic, whose persistence dependencies are explicit, and whose resource consumption is bounded. It retains established Raft safety rules and specializes the library architecture and integration contracts rather than inventing a new consensus algorithm. The build-or-compose decision is resolved in favor of the new core for the qualified scope; the comparative evidence against the pinned existing compositions is recorded in the [benchmark report](../benchmarks/report.md), and the deferred optimizations (section 6.2 dispatch overlap, leader append coalescing, leadership transfer, follower/stale reads, resumable snapshot transfer) remain future work.

The proposed design consists of:

- A single-owner protocol state machine driven by messages, logical time, proposals, and completion events.
- An explicit durability contract for term/vote updates, log replacement, commit progress, and snapshots.
- Separate committed, dispatched, and completed application progress.
- Byte-based admission and replication limits, with bounded snapshot streaming.
- A reference segmented write-ahead log using explicit writes and synchronization. Memory mapping is an optional read optimization.
- Deterministic failure simulation and reproducible benchmarks before making performance claims.
- Platform-neutral contracts qualified first on the pinned unikernel: a raw-block storage path, single-vCPU scheduling, and small recorded memory budgets.
- A Phase-0 decision gate (section 12): a pinned existing core plus driver and conformance harness must be shown insufficient before a new core is built.

This is a new implementation of the established Raft protocol. Its cost includes maintaining storage recovery, membership transitions, wire formats, compatibility, and adversarial tests.

This repository owns the library, its public contracts, reference adapters, and qualification suite. Its design and release criteria are independent of any consumer. Naming an initial qualification platform does not make the release criteria consumer-specific. Applications may adopt it through supported Go interfaces; their deployment and migration decisions remain outside this RFC.

## 2. Workload and boundaries

Target applications need an ordered, durable sequence of commands applied consistently across replicas. Examples include replicated key-value stores, metadata services, coordination systems, and control planes. Commands are opaque to the library; consumers decide which operations require consensus. The design prioritizes predictable resource use, explicit durability, and recoverable application state.

Support one voter for embedded use, development, and deployments that accept a single failure domain. It provides neither failover nor replicated-data protection. Three voters require two to commit and can tolerate one voter loss; larger configurations use the same majority rules. Consumers choose membership and failure-domain placement. Scaling application workers or clients does not implicitly change voting membership.

The library owns consensus and the ordering of committed opaque commands. Consumers own command schemas, deterministic application state machines, operation deduplication, and any external effects. Application policy, credentials, and resource ownership stay outside the library. It is not a scheduler, database engine, credential broker, or public gateway.

Assume crash/recovery failures, delayed or duplicated messages, network partitions, and storage failures. Peers are authenticated and trusted to follow the protocol. Byzantine behavior is outside scope. Safety must not require bounded message delay or synchronized wall clocks; progress requires an eventually communicating majority, usable storage, and sufficient execution time. Undetected rollback of a member's previously durable state is outside this failure model: matching local identity alone cannot detect it. Detect rollback through an externally maintained storage generation or treat restoration of an older image as disaster recovery; never silently resume it as the same member identity. An older image may rejoin only as a new member through a membership change, or through disaster recovery. In the target unikernel deployment, restoring an older VM or volume image is a routine operational action, so external generation detection is a primary requirement rather than an edge case.

The replicated log protects the commands and state covered by its durability contract. It does not automatically replicate external files or fence external writers; applications that manage those resources must supply their own guarantees.

## 3. Design rationale

The design makes protocol decisions, durability, and application progress explicit so consumers can reason about failure and resource use through documented contracts.

| Design goal | Proposed mechanism | Intended benefit |
| --- | --- | --- |
| Reproducible protocol behavior | Deterministic core with injected time, randomness, and I/O completions. | Reproduce failures and separate protocol correctness from runtime scheduling. |
| Explicit persistence dependencies | Identify which messages and application actions depend on each durable write batch. | Support safe concurrency while preserving durable quorum requirements. |
| Recoverable hard state | Atomic persistent term/vote state with versioned completion receipts. | Make adapter obligations and crash behavior easier to reason about. |
| Precise application progress | Separate dispatch progress from completed application and expose a wait operation. | Make read and effect-dispatch preconditions precise. |
| Bounded resource consumption | Enforced byte budgets as well as count limits. | Bound memory during bursts, slow application, and follower lag. |
| Honest client outcomes | Structured admission/outcome semantics and application-owned operation identity. | Prevent callers from interpreting a timeout as proof that nothing happened. |
| Unikernel-first operation | Platform-neutral contracts, raw-block storage option, single-vCPU scheduling, small recorded budgets. | Run on tiny always-on guests without assuming POSIX semantics. |

Pre-vote, quorum checks, batching, learners, snapshots, and failure testing form part of the proposed implementation. Replication pipelining and overlapping local persistence with replication require separate ordering tests before enablement. These are established techniques; the proposal concerns their composition and public contracts. Intended benefits remain unmeasured until the acceptance criteria are exercised.

## 4. Protocol

Use leader-based Raft with randomized election timeouts, pre-vote, log matching, the up-to-date voting rule, and majority commitment. A leader advances commitment by counting replicated entries from its current term; commitment of preceding entries follows the Raft rule. Followers advance only through a leader-confirmed matching prefix that is locally durable. Never overwrite committed entries. A new leader appends a no-op entry in its current term so idle clusters can establish current-term commitment and complete read barriers.

Required extensions and choices:

1. **Pre-vote:** Probe election viability before increasing the persistent term. A disconnected member should not unnecessarily disrupt a healthy leader when it reconnects. Pre-vote alone is insufficient: a member that raised its term while partitioned can still disrupt on rejoin. Ignore pre-vote and vote requests when a leader heartbeat was received within the current election timeout, and test higher-term rejoining members explicitly (section 11).
2. **Quorum checks:** Step down after insufficient quorum contact for the configured interval. This is a liveness/leadership mechanism, not an external writer fence.
3. **Linearizable reads:** Use a quorum-confirmed read barrier, not clock-based read leases. Establish a current-term committed entry, confirm leadership with a unique read context, and wait for completed application through the resulting safe index before reading a synchronized application view. Capture the read index from the commit index after invocation, once a current-term entry has committed. Count only acknowledgements for a confirmation round sent after capture. A read may join a batch only before that round is sent; acknowledgements from an earlier round cannot establish its barrier.
4. **Membership:** Use joint consensus with explicit joint and final configuration entries. Require separate old/new majorities while joint; persist and recover the transitional configuration. Serialize transitions, add peers as learners, and require authenticated durable catch-up before promotion. No direct assignment of an in-memory voter list.
5. **Leadership transfer:** Defer until the base implementation passes recovery and membership qualification. Ordinary elections are sufficient for the first version.

Read-lease optimizations, multi-group consensus, sharding, multi-region active-active operation, and Byzantine tolerance are outside the first version. The design assumes no wire compatibility with other Raft implementations.

## 5. Library architecture

```mermaid
flowchart LR
    Client[Application proposals and reads] --> Driver[Runtime driver]
    Peer[Authenticated peer transport] --> Driver
    Driver --> Core[Deterministic Raft core]
    Core --> Work[Typed work and dependencies]
    Work --> Disk[Storage adapter]
    Work --> Peer
    Work --> FSM[Application state machine]
    Disk -->|Durable or failed| Driver
    FSM -->|Applied or failed| Driver
```

The core has one mutation owner. It performs no filesystem access, networking, wall-clock reads, or goroutine creation. Timers are logical ticks; election randomness is supplied from a reproducible seed/stream and included in simulation traces. Identical initial state and ordered inputs must yield identical outputs.

Proposed events include `Tick`, `Receive`, `Propose`, `ReadBarrier`, `Persisted`, `StorageFailed`, `Applied`, `ApplyFailed`, and snapshot-transfer completions. Proposed outputs include persistence batches, outbound messages with prerequisites, committed entries eligible for application, read results, and status changes. These names describe contracts, not a frozen Go API.

Each asynchronous output carries an epoch and monotonically increasing work ID. The epoch identifies one runtime instance over an opened store and increments on every reopen or restart. Completions must match outstanding work and the current epoch; stale, duplicate, or invalid completions cannot advance state. Payload ownership is explicit: the driver retains immutable bytes until all consumers release them, or copies them under a charged budget. Applications cannot mutate proposed bytes after admission.

The reference driver supplies bounded workers, transport, metrics, and shutdown behavior. Start with one ordered persistence batch in flight per node. Disk, network, and application work may overlap only where dependencies permit. Separate them so slow snapshot/application work does not occupy the protocol event loop. Provide scheduling capacity for control traffic even when proposal admission is saturated. The driver must run correctly with a single virtual CPU (GOMAXPROCS=1): bounded workers, no busy waiting, and control-traffic priority under load. It must not rely on subprocesses, fork/exec, or platform signals; shutdown is an explicit event.

This follows the deterministic-core approach demonstrated by [etcd Raft](https://github.com/etcd-io/raft); it is an architectural choice, not an originality claim. Ship a usable reference driver so every consumer need not implement safety-critical orchestration themselves.

## 6. Persistence and recovery contract

### 6.1 Durable state

Persist cluster/member/storage identity, format version, current term and vote, the recoverable log, a safe commit watermark, and the authoritative snapshot reference. Use append-time configuration activation consistently: a member's effective configuration is the latest configuration entry in its log, committed or not, or otherwise the snapshot's configuration. Suffix replacement restores the preceding configuration. While joint, elections, pre-vote, commitment, read barriers, and quorum checks require separate majorities of both voter sets. The leader may append the final configuration only after the joint configuration commits; another membership transition waits until the final configuration commits. Recovery reconstructs these rules from the snapshot and surviving log, including uncommitted configuration entries.

Term and vote form one atomic hard-state update. Commit metadata must never reference entries missing from the recoverable log/snapshot. A persisted commit watermark may lag known commitment; entries above it are not assumed committed during recovery. Before handing newly committed entries to the application, durably advance the watermark through the applicable prefix. This is a conservative choice, not a safety requirement: safety follows from quorum durability and deterministic replay. The rule buys a locally provable committed prefix at recovery, at the cost of an extra synchronization when commitment advances without new entries. Relaxing dispatch to "known committed plus durable prefix" is a later optimization requiring its own ordering tests; it must never release application work ahead of local durability.

An atomic storage batch may contain hard-state changes, a conflicting-suffix replacement, new entries, and commit metadata. Recovery exposes either the previous complete batch or the next complete batch, never a partially applied suffix replacement. Snapshot files may be staged separately, but their authoritative publication must obey the same recoverability rules.

### 6.2 Ordering rules

| Action | Required prerequisite |
| --- | --- |
| Send an affirmative vote | Persist the relevant term and vote. |
| Send a message whose safety relies on a new persistent term | Complete the required hard-state update. |
| Acknowledge follower append success | Persist the matching entries and required term/log changes. |
| Count the leader's local log toward quorum | Complete local persistence through that index. |
| Apply committed entries | Establish commitment and persist the matching local prefix and commit watermark. |
| Return application success | Complete ordered FSM application and retain the result under the application's documented recovery contract. |
| Acknowledge snapshot installation | Durably publish its metadata/data and finish restoring the matching application state. |
| Reclaim log history | Publish a complete recoverable snapshot and retain the history required by configured catch-up policy. |

The initial conservative delivery policy persists before sending dependent replication work and does not advance local `CommitIndex` beyond `DurableIndex`. A later optimization may send eligible AppendEntries while the leader's same-batch append runs, after prerequisite hard state and earlier batches are durable. Followers still acknowledge only durable data. Initially require the leader's local copy to be durable before its own application/client completion even when follower acknowledgements form a majority. Test this optimization separately; do not infer permission to reorder all messages.

Storage errors and local FSM execution failures transition the node to a terminal `Failed` state for that runtime instance; deterministic domain rejection remains an application result. Stop proposal admission, voting, success acknowledgements, and newly released application work. Relinquish leadership, stop sending AppendEntries, heartbeats, and snapshots, and fail pending read barriers. Late completions cannot release further successful responses. Report pending outcomes conservatively. Already transmitted messages and already dispatched external effects cannot be retracted. Recovery requires reopening and validating the store under a fresh epoch; it is not an automatic empty-state restart. A supervisor (for example a host agent that restarts guests) may automate this restart once validation succeeds; ambiguous corruption, identity mismatch, and disaster recovery still require operator intervention. Completions from the previous epoch are rejected.

### 6.3 Reference storage adapter

Use a segmented append-only WAL with versioned records, lengths, checksums, batch boundaries, and sequence numbers. Encode logical truncation/replacement in the recovery protocol so a crash cannot expose an ambiguous combination of old and new suffixes. Persist metadata with the same transactional discipline. Avoid a general-purpose query engine in this adapter.

Use explicit file writes and platform-qualified synchronization. Bound group-commit bytes and delay; all requests in a batch wait for the durability boundary. Include directory durability when creating, renaming, publishing, or deleting files. A successful write call or rename alone is not the durable receipt.

The WAL specification must define two substrate bindings: a file substrate, where directory durability and atomic publication rules apply, and a raw block-device substrate with a fixed region, explicit flush semantics, and no filesystem assumptions. The raw-block binding is the safe default on unikernels; a unikernel filesystem's synchronization, rename, and locking semantics must be qualified rather than assumed. File locking is never an ownership fence; on single-process guests, exclusive volume attachment is the fence.

Recovery validates identities, format, sequence, checksums, snapshot consistency, and the committed prefix. Only an incomplete final batch that the format proves disposable may be ignored. Corruption in a committed prefix, a checksum mismatch with uncertain provenance, a missing referenced segment, or an unsupported version requires recovery intervention. Do not silently truncate arbitrary corruption and continue.

Memory mapping is not required. Start with explicit reads and a bounded cache. Consider read-only mapping of immutable sealed segments only after measuring benefit and testing mapping lifetime, reclamation, faults, and the target operating system. Account mapped resident pages and page cache alongside the Go heap. An optional bbolt adapter is also reasonable if it satisfies the same conformance contract; backend choice must not change protocol semantics. On unikernels, its memory-mapping growth, file locking, and synchronization behavior are qualification risks; the explicit-write path remains the reference.

The WAL format, exact synchronization primitives, and on-disk upgrade mechanism require a separate specification before durable implementation is accepted. Designing a log does not establish its crash safety.

### 6.4 Snapshots

Capture an application view at an exact completed index and term. Include membership as of the snapshot index (including a joint configuration when applicable), schema/version information, and application-owned deduplication/ownership state. Require an immutable or consistently isolated view; copying the whole FSM is not the default memory strategy.

Stream snapshots in bounded chunks to staging files with end-to-end integrity checks. Persist and validate the complete snapshot before publishing its authoritative manifest. Then reclaim eligible log segments. Installation must serialize against conflicting FSM application and log changes. A snapshot whose index is at or below the receiver's commit or applied index is stale and must be ignored, never installed: a follower may have applied beyond the leader's recorded match. Installation advances the dispatched/applied watermarks through the same accounting as ordinary application; restore is an apply. Restore failure leaves the node unavailable rather than serving partially restored state.

Snapshot transfer is authenticated, size-limited, and cancellable. Interrupted staging files are never treated as completed snapshots. Resumable transfers are optional later work; the first version may restart a bounded streaming transfer. Snapshot retention cannot exhaust disk indefinitely. Budget preflight rejects new proposals before exhaustion and reserves space for hard-state updates and snapshot-based reclamation. Any failed durable write or synchronization, including ENOSPC, is terminal `Failed`; a follower that cannot persist an append must not acknowledge it.

## 7. Application and read API

Expose these separate watermarks:

- `DurableIndex`: the locally recoverable prefix: the maximum of the published snapshot index and the durable log end.
- `CommitIndex`: the prefix known committed by Raft.
- `PersistedCommitIndex`: the committed prefix whose watermark and supporting log/snapshot are locally durable.
- `DispatchedIndex`: the prefix handed to the FSM worker.
- `AppliedIndex`: the prefix whose ordered FSM execution has completed.

Within a runtime instance, every policy requires `AppliedIndex <= DispatchedIndex <= PersistedCommitIndex <= CommitIndex` and `PersistedCommitIndex <= DurableIndex`. The initial conservative delivery policy in section 6.2 additionally requires `CommitIndex <= DurableIndex`. The later overlap optimization may relax only that last relation; it cannot release application work ahead of local durability. `CommitIndex`, `PersistedCommitIndex`, `DispatchedIndex`, and `AppliedIndex` are monotonically non-decreasing within a runtime instance. Uncommitted suffix replacement may reduce `DurableIndex`, but never below the published snapshot floor or `PersistedCommitIndex`. Do not treat index values alone as identity across disaster recovery.

Provide `WaitApplied(index)` and a linearizable read operation that combines a safe read barrier with completed application. `WaitApplied` must return a typed error when the node enters `Failed`, shuts down, or local log replacement removes the waited index before it committed; cancellation stops waiting without implying rollback. It must never report success for an index whose ordered application did not complete. Waits on uncommitted indices may fail on leadership loss; committed entries remain applicable by any leader. A bare leadership flag or locally observed commit index is insufficient. Application reads must use a synchronized view at or beyond the barrier index. Follower/stale reads, if added, use a separately named API with explicit consistency semantics.

Provide structured proposal outcomes:

| Outcome | Meaning |
| --- | --- |
| Rejected before admission | This invocation was not admitted; a prior invocation with the same operation identity may still exist. |
| Applied | The committed command completed FSM execution; include index/term and application result. |
| Outcome unknown | Admission occurred or cannot be ruled out, but final application is not known to this caller. |

Context cancellation stops waiting; it does not undo a committed or potentially admitted command. Leadership loss, process failure, or lost replies can produce an unknown outcome. Do not fabricate a durable operation-status service in the consensus library.

Applications requiring retry deduplication store operation ID, request digest, outcome, and retention metadata in their deterministic state machine. Duplicate IDs with equal content return the retained result; mismatched content is rejected. Snapshot/restore preserves these records. Define expiry and backup-rollback semantics before advertising retry safety. Log index alone is not an end-to-end operation ID.

FSM application is deterministic and ordered. Domain rejection is an application result; inability to apply because of local storage failure stops application/proposal success. A persistent FSM must atomically store its mutation, result/deduplication record, and applied watermark. An in-memory FSM reconstructs from its snapshot and committed replay before serving. The consensus core never dispatches cloud or database effects from inside FSM replay.

## 8. Resource and transport contracts

Require finite configurable limits for command bytes, pending proposal count/bytes, uncommitted log growth, append batch bytes/count, per-peer in-flight bytes/messages, decode allocation, snapshot chunks/concurrency, and read waiters. Charge admitted payloads until their references are released. Size and rate limits must apply before attacker-controlled allocation. Validate compatible cluster-wide limits: the maximum encoded command plus framing must fit an append batch, and the maximum encoded batch must fit every participating peer's decode limit. Reject incompatible limits at join, configuration changes, and every peer handshake, including reconnect after restart. Advertised limits travel inside the authenticated handshake and cannot be downgraded by an unauthenticated peer. A snapshot that exceeds a receiver's advertised limit produces a typed catch-up failure, not an unbounded allocation.

Admission returns a typed overload result or waits within the caller's deadline. A lagging follower cannot retain unlimited memory or prevent the healthy majority from progressing. Snapshot catch-up and log retention interact through an explicit policy; neither permits unsafe deletion of committed recovery state. If storage preflight cannot reserve the required recovery space, reject new proposals and surface the condition. Actual persistence failure follows the terminal failure rule in section 6.2; admission limits cannot substitute for follower persistence.

Transport uses authenticated member identities bound to cluster identity, bounded framed messages, explicit wire versions, and peer authorization. Admission authorization is distinct from a node's current log configuration: support explicitly admitted learners and configuration-transition peers even when the receiver's local membership view lags. Process their authenticated messages under the protocol's term and membership rules; do not filter all traffic solely by the receiver's current voter list. Authentication does not itself grant voting membership. Reject incompatible formats before protocol participation; address reuse must not imply member identity reuse. Trust/key configuration stays outside the core, and credentials must not enter logs or traces.

Expose queue bytes, durable/commit/applied lag, fsync latency, per-peer progress, election reasons, snapshot progress, storage errors, and overload counts. Metrics cardinality is bounded. Debug traces omit command payloads by default and can be replayed with synthetic or appropriately protected data.

Numeric defaults remain open until measured across representative command sizes, workloads, and deployment resource budgets. A prototype must nevertheless choose and record finite bounds before execution; unspecified limits cannot mean unlimited. The initial recorded profile is the unikernel target: one virtual CPU, a 512 MiB-class guest, and whole-state archive-class commands up to 32 MiB. Budgets must account for the Go heap, mapped pages, and kernel cache within the guest's total memory, and the driver must idle near zero CPU. These are provisional bounds to be measured, not achieved results.

## 9. Bootstrap, membership, and operational recovery

Separate explicit creation, ordinary restart, peer join, and disaster recovery APIs. Creation requires provisioned cluster/member identities, authenticated peer trust, and an explicitly initialized storage identity. Initial cluster creation generates and persists a unique cluster incarnation, which joining members adopt and peers validate on every protocol exchange. Independent creations must not reuse an incarnation even if given the same logical cluster name; provision the designated incarnation through authenticated join/configuration. Restart requires existing matching state. A missing mount, empty directory, lock failure, or mismatched volume must not trigger bootstrap.

Bind the manifest to an expected externally supplied storage identity; a manifest copied with the wrong volume is not sufficient proof of correct attachment. The runtime supplies qualified exclusive access and platform attachment checks, and exposes the externally maintained storage generation at open; state whose generation regressed is rejected. Where VM or volume image restore is routine, this check is mandatory. A local file lock alone is not a distributed ownership fence.

For expansion, admit authenticated learners, verify durable catch-up and resource capacity, and perform one administrative membership transition at a time. A one-to-two voter transition temporarily requires both members; only the healthy three-voter configuration meets the one-failure target. Recovery reconstructs and completes a pending joint transition rather than silently replacing it.

With quorum lost, stop new authoritative mutations. Do not force reconfiguration after an election timeout. Single-member recovery requires its durable volume or an explicitly governed backup restore. Total durable-volume loss is not covered by surviving-volume durability.

Disaster recovery must fence the old cluster authority, establish a new recovery incarnation, and validate backup and storage identity. The consumer/operator must stop or isolate old members and fence any external authority before activating recovery. The recovery API requires an explicit operator fencing acknowledgement. The library rejects peers from old cluster incarnations; a new incarnation alone cannot stop the old cluster from serving its existing clients. Consumers must preserve their application recovery metadata and reconcile any external effects newer than the restored state. Migration between libraries is out of scope.

## 10. Consumer integration contract

Consumers implement a deterministic FSM, choose storage and transport adapters that meet the library contracts, configure finite resource limits, and define their application's recovery semantics. The reference driver handles protocol orchestration; it does not impose domain commands or deployment topology.

For applications whose committed state drives external actions, a typical integration is:

1. Commit and apply an operation intent with an application-owned identity.
2. Dispatch the action through an executor with the required authority and fencing.
3. Reconcile uncertain outcomes using the same operation identity.
4. Commit the observed result or unresolved state.

This pattern is optional; a replicated data structure may have no external executor. Where external actions exist, consumers must reconcile unfinished work before conflicting dispatch after leadership changes. A Raft term, commit index, or successful linearizable read does not revoke credentials or stop a previously authorized external writer.

Snapshots must retain the application state needed for recovery. The library guarantees only its documented consensus, persistence, and application-delivery behavior. External execution, resource fencing, and end-to-end retry semantics require consumer-specific validation and are not conditions for unrelated consumers to use the library.

## 11. Verification and comparative acceptance

### Protocol and storage evidence

Build a deterministic simulator with virtual time, seeded elections, scheduled network delivery, and a crashable storage model. Inject dropped, duplicated, delayed, and reordered messages; partitions; pauses; restarts; stale completions; partial writes; reordering of unsynchronized writes; failed synchronization; disk-full conditions; and interrupted snapshot publication. Save failing seeds/traces and minimize counterexamples.

Assert election safety, log matching, leader completeness, no divergent committed commands at an index, no loss of acknowledged committed data within the stated surviving-storage failure model, ordered application, and membership safety across joint transitions. Check linearizable operation histories for reads/writes, including lost replies and leader changes. Model pending unsynchronized writes as potentially surviving or disappearing, subject to the specified atomic format.

Combine protocol scenario tests, fuzzing, and deterministic simulation. Simulation, a protocol model, and passing Go tests are not proof of real filesystem durability. Run crash/restart tests against every supported storage adapter and its declared operating-system/filesystem/storage path, including virtualization where applicable.

### Acceptance matrix

| Gate | Required evidence |
| --- | --- |
| Core determinism | Identical initial state, seed, and event trace produce identical outputs; no hidden time or I/O dependencies. |
| Persistence | Boundary-by-boundary crash tests preserve votes, committed prefixes, suffix replacement, and snapshot publication; ambiguous corruption stops recovery. Failed writes/syncs, including disk-full, stop protocol output and successful read/application completion. |
| Progress | With a communicating majority, usable storage, and recorded bounds on delivery and scheduling, elect a leader and commit commands within a recorded simulated bound. A reconnecting isolated member, including one carrying a higher term, does not depose a healthy leader; a leader without quorum steps down; a lagging or failed minority does not stop majority commit. |
| Membership | Crashes and partitions during learner catch-up, joint entry, final entry, and leader removal preserve quorum rules and recoverable configuration. |
| Reads/application | Slow FSM execution cannot produce premature `Applied` success or a stale linearizable read. Local FSM failure enters `Failed` and stops leadership traffic and successful read completion. Test reads arriving after a confirmation round was sent. Stale snapshots (index at or below commit/applied) are ignored without regressing watermarks; `WaitApplied` fails cleanly on `Failed`, shutdown, and truncation. |
| Client uncertainty | Lost replies/cancellation never imply rollback; a reference FSM demonstrates deduplication with retained operation IDs. |
| Resource bounds | Burst proposals, a stalled FSM, slow peers, oversized frames, and snapshot transfer stay within recorded byte budgets and return bounded overload behavior. A peer restarted with incompatible lower limits is rejected at handshake. |
| Bootstrap/recovery | Missing mount, repeated creation, wrong identity, and incompatible formats never create an accidental authoritative cluster. Exercise externally detected rollback, explicit disaster recovery, and old-incarnation rejection; verify recovery refuses to proceed without the operator's required fencing acknowledgement. |
| Platform support | Each claimed platform pins Go/runtime, filesystem, storage adapter, transport, and storage path and passes flush, locking, restart, and resource tests. The initial platform is the pinned unikernel (Nanos/QEMU/KVM), qualified on both raw-block and filesystem substrates at one virtual CPU; Linux is a reference platform. |
| Consumer contract | API documentation distinguishes consensus/application completion from external execution; examples do not imply that leadership fences external resources. |

### Benchmark design

Each comparative report names and pins every implementation and composition it measures, including the baseline. Use two pinned baselines from section 12: the incumbent HashiCorp Raft + raft-boltdb/bbolt composition and an etcd-based composition. Start with the one-voter unikernel profile before multi-voter comparisons. Record storage settings, Go version, hardware/guest, filesystem, payload distribution, and durability semantics. Compare one voter and three voters, steady and bursty command loads including whole-state archive-class commands near the maximum command size, healthy and lagging followers, and snapshot/recovery workloads. Restart time is a first-class metric: the target deployment activates guests on demand, so store open, snapshot restore, and bounded log replay must fit the recorded activation budget. Add larger configurations before making scaling claims about them. Use the same application commands and comparable acknowledged-durability guarantees.

Measure resident/peak memory including mappings and kernel cache where observable, allocations, idle CPU, disk bytes/syncs, commit/application p50/p95/p99 latency, sustained throughput, restart time, catch-up time, and overload behavior. Report library overhead separately from the host application and its state-machine footprint.

No numerical win is asserted here. Define supported workload profiles and their resource/latency thresholds before running the comparison. Release acceptance requires meeting those published thresholds and passing correctness/recovery gates. A consumer evaluating replacement must additionally justify its own migration and maintenance costs. Record regressions and uncertainty; a faster happy-path benchmark cannot waive a failed safety gate.

## 12. Alternatives and unresolved choices

| Alternative | Assessment |
| --- | --- |
| Use HashiCorp Raft with bbolt/file-snapshot adapters (the incumbent composition) | Already integrated by consumers for one-voter leadership fencing. Single-server membership changes and Barrier-based reads differ from this RFC's joint-consensus and read-barrier contracts; qualify on the unikernel before assuming fit. |
| Embed etcd Raft with a small driver/storage adapter | Already supplies a deterministic protocol core and safe-read machinery. Evaluate before implementing a new core; it may satisfy the main goals with less protocol ownership. |
| New Raft core and reference runtime | Gives control over contracts and resource accounting, but creates substantial correctness and maintenance obligations. This RFC specifies that option. |
| Change the consensus algorithm | No workload evidence currently justifies doing so. |
| Require an mmap-backed database | Unnecessary coupling of protocol to storage. Compare adapters under the same durability contract instead. |

Open choices before implementation acceptance: exact Go/module API, WAL/manifest specification (both substrate bindings), format upgrades, transport encoding, platforms beyond the pinned unikernel target, numerical budgets, snapshot creation/retention policy, the storage-generation hook, reference adapters for persistent and reconstructed FSMs, and whether a new core offers enough benefit over the pinned compositions to warrant ownership. Selecting a core with apply-time membership activation requires adopting its complete companion rules and amending section 6.1; do not combine those rules with this proposal's append-time scheme. The incumbent HashiCorp composition uses single-server membership changes rather than joint consensus; migrating to or from it is a membership-model change, not only an API swap. The pinned unikernel is the initial qualification target, not an optional extra; additional platforms remain optional qualification work and do not change the platform-neutral contracts.

**Phase-0 gate (before new-core implementation):** build the deterministic driver and conformance harness around a pinned existing core, plus a bounded one-voter journal on the pinned unikernel, and run the section 11 correctness/recovery gates against it. Proceed to a new core only with written evidence that the pinned composition cannot meet a required contract — for example watermark separation, byte-budget enforcement, honest outcome semantics, read-barrier cost, or unikernel qualification — and that the benefit outweighs ownership cost. Consumer adoption additionally requires an implemented, released module with passing library gates, a concrete unmet need, and a written migration justification. Phase-0 can invalidate the need for a new core. Three-voter recovery and membership remain library qualification gates; external-effect qualification belongs to consumers that perform such effects.

**Phase-0 outcome (recorded):** the new core was built and passed the section 11 gates on the pinned unikernel and the Linux reference. The comparative matrix ([benchmark report](../benchmarks/report.md)) records where the pinned compositions differ from this RFC's contracts: the incumbent HashiCorp composition uses single-server membership (not joint consensus) and a Barrier read (not the five-watermark read barrier), and under the conservative §6.2 durability policy this library pays an extra commit-watermark fsync per sequential commit and does not coalesce leader appends (so pipelined throughput is fsync-bound where the baselines scale). These are recorded costs and contract differences, not a claim of superiority; the ownership decision rests on the explicit durability/watermark/outcome contracts and the qualified unikernel fit. The gated §6.2 dispatch optimization and leader append coalescing remain the documented path to closing the throughput gap, each requiring its own ordering tests before enablement.

## 13. Sources and evidence status

- [Raft paper](https://raft.github.io/raft.pdf): protocol, safety, replication, and joint consensus.
- [Ongaro dissertation](https://github.com/ongardie/dissertation/blob/master/stanford.pdf): protocol extensions and implementation considerations.
- [etcd Raft](https://github.com/etcd-io/raft): deterministic core, safe reads, and documented persistence/message ordering. Design reference, not a selected dependency or qualification receipt.
- [HashiCorp Raft](https://github.com/hashicorp/raft) and [raft-boltdb](https://github.com/hashicorp/raft-boltdb): incumbent composition reference for comparative qualification, not a selected dependency.
- [Nanos](https://nanos.org/thebook): initial unikernel qualification target; capability constraints must be pinned and tested, not assumed.
- [bbolt](https://github.com/etcd-io/bbolt) and [SQLite mmap discussion](https://www.sqlite.org/mmap.html): storage alternatives and mapping trade-offs, not platform qualification receipts.

The mechanisms in this document are implemented and qualified for the stated scope; the design text is retained as the specification. Evidence is reported separately, not asserted here: simulator results and the Wing-Gong linearizability checker, fuzzing, and boundary crash tests (test suite); real filesystem durability and power-loss recovery on the pinned unikernel and Linux/EBS ([qualification receipt](../qualification/receipt.md), R1–R4); and the comparative benchmark matrix against published thresholds ([benchmark report](../benchmarks/report.md)). Library release acceptance is recorded; consumer deployment acceptance remains consumer-specific and is not claimed. A formal correctness proof is not claimed, and no performance win is asserted beyond the measured threshold results.
