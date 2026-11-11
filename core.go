package raft

import (
	"errors"
	"math/rand"
	"sort"
)

// ErrNotLeader is returned for operations requiring leadership.
var ErrNotLeader = errors.New("raft: not leader")

// Config holds deterministic core parameters. Time is logical ticks; randomness
// is seeded. No wall clock or I/O is referenced.
type Config struct {
	ID            NodeID
	ElectionTick  int
	HeartbeatTick int
	PreVote       bool
	CheckQuorum   bool
	// Seed drives election randomness reproducibly (§5).
	Seed int64
	// MaxUncommittedBytes caps log bytes above the commit index (§8). Zero
	// selects the default; admission beyond the cap returns OutcomeOverload.
	MaxUncommittedBytes int
	// SnapshotChunkBytes bounds one streamed snapshot chunk (§6.4). Snapshots
	// at or below this size transfer inline. Zero selects the default.
	SnapshotChunkBytes int
	// MaxSnapshotBytes caps an inbound streamed snapshot (§8). Zero selects
	// the default; larger transfers produce a typed catch-up rejection.
	MaxSnapshotBytes int
	// MaxAppendBatchBytes bounds the payload of one AppendEntries batch (§8).
	// Zero selects the default. Replication continues across batches as
	// followers acknowledge.
	MaxAppendBatchBytes int
	// Trace enables bounded protocol decision tracing for diagnostics.
	Trace bool
}

func (c *Config) validate() {
	if c.ElectionTick <= 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick <= 0 {
		c.HeartbeatTick = 1
	}
	if c.HeartbeatTick >= c.ElectionTick {
		c.HeartbeatTick = c.ElectionTick / 2
		if c.HeartbeatTick < 1 {
			c.HeartbeatTick = 1
		}
	}
	if c.MaxUncommittedBytes <= 0 {
		c.MaxUncommittedBytes = 64 << 20
	}
	if c.SnapshotChunkBytes <= 0 {
		c.SnapshotChunkBytes = 1 << 20
	}
	if c.MaxSnapshotBytes <= 0 {
		c.MaxSnapshotBytes = 64 << 20
	}
	if c.MaxAppendBatchBytes <= 0 {
		c.MaxAppendBatchBytes = 8 << 20
	}
}

// InitState is the recovered durable state used to construct a core.
type InitState struct {
	HardState    HardState
	Snapshot     SnapshotMeta
	Entries      []Entry // durable entries after the snapshot
	Config       Configuration
	AppliedIndex Index
	DurableIndex Index
	Epoch        uint64
}

// Outcome is a structured proposal outcome (§7).
type Outcome uint8

const (
	// OutcomeRejected: not admitted; a prior invocation may still exist.
	OutcomeRejected Outcome = iota
	// OutcomeApplied: committed and FSM-applied.
	OutcomeApplied
	// OutcomeUnknown: admitted or cannot be ruled out; final application unknown.
	OutcomeUnknown
	// OutcomeOverload: rejected before admission because a resource budget
	// is exhausted (§8). Like OutcomeRejected, this invocation was not
	// admitted; a prior invocation with the same identity may still exist.
	OutcomeOverload
)

// ProposalResult reports a proposal's outcome to its caller.
type ProposalResult struct {
	ID      uint64
	Outcome Outcome
	Index   Index
	Term    Term
}

// Core is the deterministic single-owner protocol state machine.
type Core struct {
	id    NodeID
	cfg   Config
	epoch uint64

	term   Term
	vote   NodeID
	role   Role
	leader NodeID

	log *raftLog

	// uncommittedBytes tracks log payload bytes above commitIndex (§8
	// uncommitted-log growth cap). Pure accounting: no I/O, deterministic.
	uncommittedBytes int

	// Watermarks (§7).
	durableIndex    Index
	commitIndex     Index
	persistedCommit Index
	dispatchedIndex Index
	appliedIndex    Index

	// Configuration (append-time activation).
	config    Configuration
	confBase  Configuration
	confStack []confRecord

	// Election state.
	electionElapsed           int
	heartbeatElapsed          int
	randomizedElectionTimeout int
	rng                       *rand.Rand
	votes                     map[NodeID]bool
	preVotes                  map[NodeID]bool
	leaderSeenElapsed         int

	// Leader state.
	progress             map[NodeID]*Progress
	leaderNoopIndex      Index
	currentTermCommitted bool
	quorumElapsed        int
	leaderCommit         Index // follower: last leader commit hint
	// leaderMatch is the highest index verified to match the current leader's
	// log via AppendEntries. A follower may commit only through this prefix,
	// never through an unverified (possibly divergent) suffix (§4).
	leaderMatch Index

	// Reads.
	pendingReads []pendingRead
	readRounds   []*readRound
	readRoundSeq uint64

	// Membership transition target awaiting finalization.
	pendingConfTarget *Configuration

	// Work tracking.
	nextWorkID uint64
	pending    map[uint64]pendingWork

	// Proposals awaiting application.
	proposals map[uint64]proposalTrack

	// Snapshot install in progress.
	installingSnapshot *SnapshotMeta
	// staging tracks an inbound streamed snapshot (§6.4).
	staging *snapshotStaging
	// snapCRC caches the end-to-end checksum of the current snapshot data.
	snapCRCIndex Index
	snapCRC      uint32

	out Outputs

	trace []TraceEvent
}

// TraceEvent records a protocol decision for diagnostics.
type TraceEvent struct {
	Kind   string
	Term   Term
	Index  Index
	Detail string
}

// Trace returns recorded protocol decisions.
func (c *Core) Trace() []TraceEvent { return c.trace }

func (c *Core) record(kind string, idx Index, detail string) {
	if !c.cfg.Trace {
		return
	}
	c.trace = append(c.trace, TraceEvent{Kind: kind, Term: c.term, Index: idx, Detail: detail})
	if len(c.trace) > 8192 {
		c.trace = c.trace[len(c.trace)-4096:]
	}
}

type proposalTrack struct {
	index Index
	term  Term
}

// NewCore constructs a core from recovered state.
func NewCore(cfg Config, init InitState) *Core {
	cfg.validate()
	c := &Core{
		id:        cfg.ID,
		cfg:       cfg,
		epoch:     init.Epoch,
		term:      init.HardState.Term,
		vote:      init.HardState.Vote,
		role:      Follower,
		log:       newLog(init.Snapshot),
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		progress:  make(map[NodeID]*Progress),
		votes:     make(map[NodeID]bool),
		preVotes:  make(map[NodeID]bool),
		pending:   make(map[uint64]pendingWork),
		proposals: make(map[uint64]proposalTrack),
	}
	c.confBase = init.Snapshot.Config.Clone()
	if len(init.Snapshot.Config.Voters) == 0 {
		c.confBase = init.Config.Clone()
	}
	// Replay durable entries into the log and configuration tracking.
	for _, e := range init.Entries {
		c.log.append([]Entry{e})
		if e.Type == EntryConfChange {
			if cfg, err := confDecode(e.Data); err == nil {
				c.confStack = append(c.confStack, confRecord{index: e.Index, cfg: cfg})
			}
		}
	}
	c.config = c.effectiveConfig()
	c.durableIndex = init.DurableIndex
	if c.durableIndex < init.Snapshot.Index {
		c.durableIndex = init.Snapshot.Index
	}
	c.commitIndex = init.HardState.Commit
	c.persistedCommit = init.HardState.Commit
	c.appliedIndex = init.AppliedIndex
	c.dispatchedIndex = init.AppliedIndex
	c.leaderCommit = init.HardState.Commit
	c.leaderMatch = init.Snapshot.Index
	for _, e := range init.Entries {
		if e.Index > c.commitIndex {
			c.uncommittedBytes += len(e.Data)
		}
	}
	c.resetElectionTimeout()
	return c
}

// ID returns the node's identifier.
func (c *Core) ID() NodeID { return c.id }

// Role returns the current role.
func (c *Core) Role() Role { return c.role }

// Term returns the current term.
func (c *Core) Term() Term { return c.term }

// Leader returns the known leader, or zero.
func (c *Core) Leader() NodeID { return c.leader }

// Vote returns the current vote, or zero.
func (c *Core) Vote() NodeID { return c.vote }

// Watermarks returns the five progress watermarks (§7).
func (c *Core) Watermarks() (durable, commit, persistedCommit, dispatched, applied Index) {
	return c.durableIndex, c.commitIndex, c.persistedCommit, c.dispatchedIndex, c.appliedIndex
}

// Config returns the effective configuration.
func (c *Core) Config() Configuration { return c.config.Clone() }

// DebugProgress returns a snapshot of leader replication state (§8 metrics).
func (c *Core) DebugProgress() map[NodeID]Progress {
	out := make(map[NodeID]Progress, len(c.progress))
	for _, id := range c.sortedProgress() {
		out[id] = *c.progress[id]
	}
	return out
}

// DebugPending returns the number of outstanding work items (diagnostics).
func (c *Core) DebugPending() int { return len(c.pending) }

// DebugLog returns a copy of the accepted log entries (for diagnostics).
func (c *Core) DebugLog() []Entry {
	return append([]Entry(nil), c.log.ents[1:]...)
}

// DebugState returns a diagnostic snapshot of protocol state.
func (c *Core) DebugState() (role Role, term Term, commit, durable, persisted, applied Index) {
	return c.role, c.term, c.commitIndex, c.durableIndex, c.persistedCommit, c.appliedIndex
}

// sortedProgress returns progress keys in a deterministic order so output
// ordering never depends on map iteration (§5 determinism).
func (c *Core) sortedProgress() []NodeID {
	ids := make([]NodeID, 0, len(c.progress))
	for id := range c.progress {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// sortedProposalIDs returns pending proposal IDs in deterministic order.
func (c *Core) sortedProposalIDs() []uint64 {
	ids := make([]uint64, 0, len(c.proposals))
	for id := range c.proposals {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (c *Core) resetElectionTimeout() {
	c.electionElapsed = 0
	c.randomizedElectionTimeout = c.cfg.ElectionTick + c.rng.Intn(c.cfg.ElectionTick)
}

// Step processes one event and returns the outputs it produces.
func (c *Core) Step(ev Event) Outputs {
	c.out = Outputs{}
	if c.role == Failed {
		return c.out
	}
	switch ev.Kind {
	case EvTick:
		c.tick()
	case EvReceive:
		c.receive(ev.Message)
	case EvPropose:
		c.propose(ev.WorkID, ev.Data)
	case EvProposeConfChange:
		c.proposeConfChange(ev.WorkID, ev.Config)
	case EvReadBarrier:
		c.readBarrier(ev.ReadCtx)
	case EvPersisted:
		c.onPersisted(ev)
	case EvStorageFailed:
		c.fail("storage: " + errString(ev.Err))
	case EvApplied:
		c.onApplied(ev)
	case EvApplyFailed:
		c.fail("apply: " + errString(ev.Err))
	case EvSnapshotInstalled:
		c.onSnapshotInstalled(ev)
	case EvSnapshotCreated:
		c.onSnapshotCreated(ev)
	case EvSnapshotRequest:
		c.requestSnapshot()
	case EvSnapshotStaged:
		c.onSnapshotStaged(ev)
	}
	if c.role != Failed {
		c.maybeDispatch()
		c.flushReads()
		c.maybeCompleteReads()
	}
	return c.out
}

func errString(err error) string {
	if err == nil {
		return "unknown"
	}
	return err.Error()
}

func (c *Core) tick() {
	if c.role == Leader {
		c.heartbeatElapsed++
		c.quorumElapsed++
		// Restart stalled snapshot transfers (§6.4): a crashed or stalled
		// receiver must not hold the pending marker forever. Active streams
		// reset this on every acknowledged chunk.
		for _, id := range c.sortedProgress() {
			p := c.progress[id]
			p.ProbeElapsed++
			if p.AppendInflight {
				p.InflightElapsed++
				if p.InflightElapsed > c.cfg.ElectionTick {
					p.AppendInflight = false // lost append: allow retransmit
					p.InflightElapsed = 0
				}
			}
			if p.PendingSnapshot > 0 {
				p.StreamElapsed++
				// Generous relative to elections: a live transfer can span many
				// election timeouts; only a truly stalled receiver restarts.
				if p.StreamElapsed > 10*c.cfg.ElectionTick {
					p.PendingSnapshot = 0
					p.StreamOffset = 0
					p.StreamElapsed = 0
				}
			}
		}
		if c.heartbeatElapsed >= c.cfg.HeartbeatTick {
			c.heartbeatElapsed = 0
			c.broadcastHeartbeat()
		}
		if c.cfg.CheckQuorum && c.quorumElapsed >= c.cfg.ElectionTick {
			if !c.quorumRecentlyActive() {
				c.becomeFollower(c.term, 0)
				c.emitStatus("check-quorum: no quorum contact")
			}
		}
		return
	}
	c.electionElapsed++
	c.leaderSeenElapsed++
	if c.electionElapsed >= c.randomizedElectionTimeout {
		c.electionElapsed = 0
		if c.cfg.PreVote {
			c.campaignPreVote()
		} else {
			c.becomeCandidate()
		}
	}
}

// quorumRecentlyActive reports whether a majority of every voter set contacted
// the leader within the quorum interval.
func (c *Core) quorumRecentlyActive() bool {
	active := map[NodeID]bool{c.id: true}
	for id, p := range c.progress {
		if p.RecentActive {
			active[id] = true
		}
	}
	return c.config.QuorumActive(active)
}

func (c *Core) emitStatus(reason string) {
	c.out.Status = &Status{Role: c.role, Term: c.term, Leader: c.leader, Reason: reason}
}

// newWork allocates a work ID and records its completion effects.
func (c *Core) newWork(kind WorkKind, pw pendingWork) uint64 {
	c.nextWorkID++
	id := c.nextWorkID
	pw.kind = kind
	c.pending[id] = pw
	return id
}

// persistHardState emits an atomic hard-state batch and returns its work ID.
func (c *Core) persistHardState() uint64 {
	hs := HardState{Term: c.term, Vote: c.vote, Commit: c.persistedCommit}
	id := c.newWork(WorkPersist, pendingWork{})
	c.out.work(Work{ID: id, Epoch: c.epoch, Kind: WorkPersist, Persist: PersistBatch{HardState: &hs}})
	return id
}

// persistEntries emits a batch appending entries (with optional truncation and
// commit watermark) and returns its work ID.
func (c *Core) persistEntries(trunc Index, entries []Entry, commit *Index) uint64 {
	durable := c.durableIndex
	if len(entries) > 0 {
		durable = entries[len(entries)-1].Index
	} else if trunc > 0 {
		durable = trunc - 1
	}
	if durable < c.log.snapshot.Index {
		durable = c.log.snapshot.Index
	}
	pw := pendingWork{durableIndex: durable}
	if commit != nil {
		pw.commit = *commit
		pw.hasCommit = true
	}
	id := c.newWork(WorkPersist, pw)
	c.out.work(Work{ID: id, Epoch: c.epoch, Kind: WorkPersist, Persist: PersistBatch{
		TruncateAt: trunc, Entries: entries, Commit: commit,
	}})
	return id
}

func (c *Core) onPersisted(ev Event) {
	pw, ok := c.pending[ev.WorkID]
	if !ok || ev.Epoch != c.epoch || pw.kind != WorkPersist {
		return // stale/invalid completion cannot advance state (§5)
	}
	delete(c.pending, ev.WorkID)
	if pw.durableIndex > c.durableIndex {
		c.durableIndex = pw.durableIndex
	}
	if pw.hasCommit {
		if pw.commit > c.persistedCommit {
			c.persistedCommit = pw.commit
		}
		c.setCommit(pw.commit)
	}
	c.maybeCommit()
	c.maybeDispatch()
}

func (c *Core) onApplied(ev Event) {
	pw, ok := c.pending[ev.WorkID]
	if !ok || ev.Epoch != c.epoch || pw.kind != WorkApply {
		return
	}
	delete(c.pending, ev.WorkID)
	if pw.appliedIndex > c.appliedIndex {
		c.appliedIndex = pw.appliedIndex
	}
	c.resolveProposals()
	c.maybeCompleteReads()
}

// maybeDispatch releases committed, durable entries to the FSM (§6.2, §7).
func (c *Core) maybeDispatch() {
	if c.installingSnapshot != nil {
		return
	}
	target := c.persistedCommit
	if target > c.dispatchedIndex {
		entries := c.log.entries(c.dispatchedIndex+1, target+1)
		id := c.newWork(WorkApply, pendingWork{appliedIndex: target})
		c.out.work(Work{ID: id, Epoch: c.epoch, Kind: WorkApply, Apply: entries})
		c.dispatchedIndex = target
	}
}

// appendLog appends entries and maintains the uncommitted-byte budget (§8).
// Truncated suffix bytes are released; appended bytes are charged.
func (c *Core) appendLog(es []Entry) (Index, []Entry) {
	if conflict := c.log.conflictIndex(es); conflict > 0 {
		for _, e := range c.log.entries(conflict, c.log.lastIndex()+1) {
			c.uncommittedBytes -= len(e.Data)
		}
	}
	trunc, appended := c.log.append(es)
	for _, e := range appended {
		c.uncommittedBytes += len(e.Data)
	}
	return trunc, appended
}

// setCommit advances the commit watermark, releasing newly committed bytes
// from the uncommitted budget (§8). Entries below the snapshot floor were
// already released at compaction (compaction only removes committed history).
func (c *Core) setCommit(idx Index) {
	if idx <= c.commitIndex {
		return
	}
	lo := c.commitIndex + 1
	if lo < c.log.firstIndex() {
		lo = c.log.firstIndex()
	}
	for _, e := range c.log.entries(lo, idx+1) {
		c.uncommittedBytes -= len(e.Data)
	}
	c.commitIndex = idx
}

// fail transitions to the terminal Failed state (§6.2).
func (c *Core) fail(reason string) {
	c.role = Failed
	c.leader = 0
	// Fail pending read barriers.
	for _, r := range c.readRounds {
		for _, pr := range r.reads {
			c.out.Reads = append(c.out.Reads, ReadResult{Ctx: pr.ctx, Err: errors.New("raft: node failed")})
		}
	}
	for _, pr := range c.pendingReads {
		c.out.Reads = append(c.out.Reads, ReadResult{Ctx: pr.ctx, Err: errors.New("raft: node failed")})
	}
	c.readRounds = nil
	c.pendingReads = nil
	// Report admitted proposals as unknown.
	for _, id := range c.sortedProposalIDs() {
		p := c.proposals[id]
		c.out.Proposals = append(c.out.Proposals, ProposalResult{ID: id, Outcome: OutcomeUnknown, Index: p.index, Term: p.term})
	}
	c.proposals = make(map[uint64]proposalTrack)
	c.emitStatus("failed: " + reason)
}
