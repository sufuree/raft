package raft

// EventKind classifies a core input (§5).
type EventKind uint8

const (
	EvTick EventKind = iota
	EvReceive
	EvPropose
	EvReadBarrier
	EvPersisted
	EvStorageFailed
	EvApplied
	EvApplyFailed
	EvSnapshotInstalled
	EvSnapshotCreated
	EvSnapshotRequest
	// EvProposeConfChange proposes a membership transition.
	EvProposeConfChange
	// EvSnapshotStaged completes one streamed snapshot chunk write (§6.4).
	EvSnapshotStaged
)

// Event is one ordered input to the core.
type Event struct {
	Kind EventKind

	Message Message // EvReceive
	Data    []byte  // EvPropose
	ReadCtx []byte  // EvReadBarrier

	// Completions reference outstanding work by epoch and ID.
	Epoch  uint64
	WorkID uint64

	// EvProposeConfChange carries the target configuration.
	Config Configuration

	// EvSnapshotCreated carries the captured snapshot (with FSM data).
	Snapshot SnapshotMeta

	// Failure detail.
	Err error
}
