# Contributing

Thanks for your interest in improving this library. It implements
[RFC-0001](docs/rfc/0001-deterministic-go-raft.md); the design contracts there
are the source of truth for behavior.

## Ground rules

- **Preserve determinism (RFC §5).** The protocol core does no I/O, reads no
  wall clock, and spawns no goroutines; time is logical ticks, randomness is
  seeded, and map iteration never drives output order. Identical inputs must
  produce identical outputs. The simulator stress test must be byte-identical
  across runs.
- **Single-owner core.** Only the driver event loop mutates the core; accessors
  read the published atomic snapshot.
- **Conservative durability (RFC §6.2).** Do not relax the ordering rules
  (commit ≤ durable, dispatch only to the persisted-commit prefix, persist the
  commit watermark before apply) without the dedicated ordering tests the RFC
  requires. The dispatch-overlap optimization is gated future work.
- **Regression-test every fix.** A bug fix lands with a test that fails before
  and passes after. Keep `gofmt`, `go vet`, and `-race` clean.
- **Honest evidence.** No gate is claimed without recorded evidence; simulation
  is not real durability, and no performance win is claimed without measurement
  against the published thresholds.

## Verification (run before sending a change)

```sh
gofmt -l .                      # must be empty (bench module included)
go vet ./...
go build ./...
go test ./... -count=1 -timeout 300s
go test -race ./... -count=1 -timeout 400s
go test ./sim/ -run TestStress -count=1 -v   # 150 seeded schedules
go test . -run TestCoreDeterminism -count=3
(cd bench/compare && go build ./...)         # separate module
```

Determinism check: run the stress test twice, strip timings, and diff the
per-seed outcomes — they must be identical.

## Layout

- Root package `raft`: deterministic core, watermarks, messages, wire codec.
- `wal`: segmented write-ahead log over a `Device` (file or raw-block region);
  format pinned in [docs/spec/wal-format.md](docs/spec/wal-format.md).
- `driver`: reference runtime (event loop, ordered storage, bounded workers).
- `transport`: TCP+mTLS peer transport. `bootstrap`: create/open/recover.
- `sim`: deterministic simulator + linearizability checker.
- `cmd/qualify`: in-guest platform qualification suite (Linux and Nanos).
- `bench/compare`: separate module; comparative benchmarks vs pinned baselines.
- `docs/`: RFC, WAL format spec, qualification receipt, benchmark profiles/report.

## Platform qualification

`cmd/qualify` runs the in-guest suite (both WAL substrates, locking, filesystem
primitives, single/three-voter profiles, restart, resources, benchmarks, idle).
`scripts/` holds the QEMU/KVM and Linux runners; they require a `BUCKET` env
(temporary S3 transfer bucket) and **guaranteed teardown** of any cloud
resources. See the [qualification receipt](docs/qualification/receipt.md).

## License

By contributing, you agree your contributions are licensed under the
[Apache License 2.0](LICENSE).
