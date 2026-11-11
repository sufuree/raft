package raft

// WorkKind classifies an asynchronous output (§5).
type WorkKind uint8

const (
	// WorkPersist is an atomic durability batch.
	WorkPersist WorkKind = iota
	// WorkApply hands committed entries to the FSM.
	WorkApply
	// WorkSnapshotInstall restores the FSM from a snapshot.
	WorkSnapshotInstall
	// WorkSnapshotCreate captures an FSM view.
	WorkSnapshotCreate
)

// PersistBatch is one atomic storage batch (§6.1). Recovery exposes either the
// previous complete batch or the next complete batch, never a partial suffix
// replacement.
type PersistBatch struct {
	// HardState, when set, is stored atomically with the batch.
	HardState *HardState
	// TruncateAt, when non-zero, discards log entries with index >= TruncateAt
	// before appending Entries (conflicting-suffix replacement).
	TruncateAt Index
	// Entries are appended after any truncation.
	Entries []Entry
	// Commit, when set, advances the durable commit watermark.
	Commit *Index
	// Snapshot, when set, is the authoritative publication.
	Snapshot *SnapshotMeta
	// SnapshotStream, when set by a storage adapter, describes snapshot data
	// published as chunk records instead of inline Data (§6.4). The core never
	// sets it; adapters use it for bounded streaming publication.
	SnapshotStream *SnapshotStreamRef
}

// SnapshotStreamRef references streamed snapshot data by size and end-to-end
// checksum (§6.4).
type SnapshotStreamRef struct {
	Size uint64
	CRC  uint32
}

// Work is an asynchronous output carrying an epoch and monotonic ID (§5).
type Work struct {
	ID    uint64
	Epoch uint64
	Kind  WorkKind

	Persist  PersistBatch
	Apply    []Entry
	Snapshot SnapshotMeta
}

// OutMessage is an outbound message with an optional durability prerequisite.
// After is the work ID that must complete before the message may be sent; zero
// means send immediately.
type OutMessage struct {
	Message Message
	After   uint64
}

// ReadResult delivers a confirmed linearizable read barrier (§4.3, §7). A
// non-nil Err reports a failed barrier (not leader, node failed, etc.).
type ReadResult struct {
	Ctx   []byte
	Index Index
	Err   error
}

// Status reports a role/leadership/health change.
type Status struct {
	Role   Role
	Term   Term
	Leader NodeID
	Reason string
}

// Outputs accumulates everything one Step produces.
type Outputs struct {
	Messages  []OutMessage
	Work      []Work
	Reads     []ReadResult
	Proposals []ProposalResult
	Status    *Status
}

func (o *Outputs) msg(m Message, after uint64) {
	o.Messages = append(o.Messages, OutMessage{Message: m, After: after})
}

func (o *Outputs) work(w Work) { o.Work = append(o.Work, w) }

// pendingWork records the effects a completion will apply.
type pendingWork struct {
	kind         WorkKind
	durableIndex Index
	commit       Index
	hasCommit    bool
	appliedIndex Index
	snapshot     SnapshotMeta
}
