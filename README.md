# Raft

A deterministic Go Raft library with explicit durability contracts and bounded
resource use, implemented per [RFC-0001](docs/rfc/0001-deterministic-go-raft.md).

Raft is for applications that need a durable, agreed-upon sequence of commands
across replicas: metadata services, replicated key-value stores, coordination
systems, control planes. Commands and state machines are application-defined.

## Status

**Implemented and under qualification.** The protocol core, reference WAL,
runtime driver, bootstrap/recovery discipline, authenticated TCP+mTLS
transport, and deterministic simulator are implemented and tested. Release
acceptance follows the RFC §11 matrix; the table below reports honest gate
status, not claims.

| Gate | Status | Evidence |
| --- | --- | --- |
| Core determinism | passing | identical outputs across runs; 150-seed adversarial stress identical across runs |
| Persistence | passing (simulated + real-fs) | boundary crash tests, torn/unsynced/corruption tests, power-loss test on Nanos and Linux/EBS |
| Progress | passing | elections, disruption guard, CheckQuorum step-down, lagging-minority commits |
| Membership | passing | joint transitions, crashes mid-transition, leader removal, 1→3 voters, learner promotion |
| Reads/application | passing | quorum read barrier, slow-FSM safety, stale-snapshot guards, linearizability checker |
| Client uncertainty | passing | structured outcomes; lost replies never imply rollback |
| Resource bounds | passing | byte budgets, typed overload, bounded decode, in-flight append window, streamed snapshots |
| Bootstrap/recovery | passing | creation/restart/disaster-recovery discipline, generation checks, fencing acknowledgement |
| Platform support | in progress | Linux reference (x86_64 + arm64, EBS) passing; Nanos unikernel suite + power-loss passing under QEMU/TCG on macOS and Linux; pinned QEMU/KVM receipt pending bare-metal availability ([receipt](docs/qualification/receipt.md)) |
| Consumer contract | passing | API docs distinguish consensus completion from external execution |

Comparative benchmarks against pinned HashiCorp Raft + bbolt and etcd Raft
compositions run under published profiles
([docs/benchmarks/profiles.md](docs/benchmarks/profiles.md)); no numerical win
is claimed until measured against those thresholds.

## Packages

- `raft` — deterministic single-owner protocol core. No I/O, no wall clock, no
  goroutines; time is logical ticks, randomness is seeded, I/O arrives as
  completion events. Five watermarks: durable, committed, persisted-commit,
  dispatched, applied.
- `wal` — segmented write-ahead log over a `Device` (file or fixed raw-block
  region). Checksummed atomic batches, crash-safe recovery, snapshot
  publication with streaming and reclamation. Format pinned in
  [docs/spec/wal-format.md](docs/spec/wal-format.md).
- `driver` — reference runtime: single-owner event loop, ordered persistence,
  bounded workers, proposal/read waiters, structured outcomes.
- `transport` — TCP+mTLS peer transport: authenticated handshake binding
  member identity to the cluster incarnation, limit negotiation on every
  connection, per-peer byte/message budgets, admission distinct from log
  membership.
- `bootstrap` — explicit `Create`, `Open`, and `DisasterRecover` (operator
  fencing acknowledgement required).
- `sim` — deterministic simulator with per-tick safety invariants and a
  Wing-Gong linearizability checker over client histories.

## Quick start (single voter, embedded)

```go
dev, _ := wal.OpenFile("node.wal")
store, _ := wal.Open(dev, wal.Options{
    ClusterID: 1, MemberID: 1, Generation: 1,
    InitialConfig: raft.Configuration{Voters: raft.NodeSet{1}},
})
d, _ := driver.New(driver.Config{ID: 1, TickInterval: 5 * time.Millisecond},
    store, myFSM, driver.NewLoopNetwork().NewTransport(1))
d.Start()
defer d.Stop()

res, err := d.Propose(ctx, []byte("command"))
// res.Outcome is Applied, Rejected (not admitted), or Unknown.
// Unknown means the command may still commit: reconcile by application-owned
// operation identity; a timeout is never proof of rollback.
idx, err := d.Read(ctx) // linearizable barrier; read your FSM at >= idx
```

See `driver/example_test.go` for a runnable version. Multi-voter clusters use
`transport.New` with mTLS certificates and `bootstrap.Create`/`Open`.

## Application boundaries

Applications own command schemas, deterministic state machines, retry
deduplication, and external actions. **Consensus ordering does not fence
external writers**: a Raft term, commit index, or successful read does not
revoke credentials or stop a previously authorized external executor. Where
committed state drives external effects, dispatch through an executor with
its own authority and fencing, and reconcile uncertain outcomes by
application-owned operation identity (RFC §10).

A single voter provides ordering and durability for one failure domain — no
failover, no replicated-data protection. Three voters tolerate one loss.

## Verification

```sh
go test ./... -count=1
go test -race ./... -count=1
go test ./sim/ -run TestStress -count=1 -v   # 150 seeded adversarial schedules
go test . -run TestCoreDeterminism -count=3
```

Platform qualification: `cmd/qualify` runs the in-guest suite (both WAL
substrates, locking, single/three-voter profiles, restart timing, resources);
`scripts/nanos-crash-test*.sh` inject power loss by killing QEMU and verify
recovery on the same volume.

## License

[Apache License 2.0](LICENSE).
