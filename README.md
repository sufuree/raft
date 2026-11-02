# Raft

A proposed Go consensus library focused on deterministic execution, explicit durability, and bounded resource use.

Raft is intended for applications that need a durable, agreed-upon sequence of commands across replicas, such as metadata services, replicated key-value stores, and coordination systems. It is an independent library with application-defined commands and state machines.

## Status

**Design stage.** This repository currently contains the architecture RFC. There is no library implementation, installable Go package, or stable API yet. Performance and platform compatibility have not been measured or qualified.

Start with [RFC-0001: A deterministic Go Raft library](docs/rfc/0001-deterministic-go-raft.md) for the proposed contracts, alternatives, open decisions, and acceptance criteria.

## Design goals

- **Deterministic core:** explicit inputs for messages, time, proposals, and completed I/O make protocol behavior reproducible.
- **Explicit durability:** persistence dependencies define when voting, replication acknowledgements, and application work may proceed.
- **Precise application progress:** distinguish durable, committed, dispatched, and applied entries, with read barriers that wait for completed application.
- **Bounded resources:** enforce byte and count limits for proposals, replication, decoding, and snapshot transfer.
- **Replaceable adapters:** keep storage and transport separate from the protocol core, with a reference runtime driver.
- **Testable recovery:** use deterministic failure simulation, storage crash tests, and reproducible benchmarks to evaluate the design.

The proposal uses the established Raft protocol with pre-vote, quorum checks, and joint-consensus membership changes. A segmented write-ahead log is the proposed reference storage adapter; memory mapping is an optional read optimization.

## Application boundaries

Applications own their command schemas, state machines, retry deduplication, and external actions. Consensus establishes command ordering; it does not automatically fence external writers or guarantee exactly-once execution of external effects.

The design supports a single voter for deployments that accept a single failure domain, and majority-based replication for multi-voter clusters. A single voter provides no failover or replicated-data protection.

## Design feedback

Issues and pull requests discussing the RFC are welcome. Useful feedback includes concrete failure scenarios, ambiguous contracts, resource constraints, and ways to make the acceptance criteria more precise. Implementation choices and public APIs remain open as described in the RFC.

## License

[Apache License 2.0](LICENSE).
