package raft

import (
	"errors"
	"fmt"
)

func (c *Core) becomeFollower(term Term, leader NodeID) {
	c.role = Follower
	c.leader = leader
	c.leaderSeenElapsed = 0
	if term != c.term {
		c.term = term
		c.vote = 0
		c.persistHardState()
	}
	c.votes = make(map[NodeID]bool)
	c.preVotes = make(map[NodeID]bool)
	c.pendingConfTarget = nil
	c.leaderMatch = c.log.snapshot.Index
	c.resetElectionTimeout()
	// Admitted-but-unapplied proposals become unknown on leadership loss (§7).
	if len(c.proposals) > 0 {
		for _, id := range c.sortedProposalIDs() {
			p := c.proposals[id]
			c.out.Proposals = append(c.out.Proposals, ProposalResult{ID: id, Outcome: OutcomeUnknown, Index: p.index, Term: p.term})
		}
		c.proposals = make(map[uint64]proposalTrack)
	}
	c.emitStatus("became follower")
}

func (c *Core) becomePreCandidate() {
	c.role = PreCandidate
	c.leader = 0
	c.preVotes = map[NodeID]bool{c.id: true}
	c.votes = make(map[NodeID]bool)
	c.resetElectionTimeout()
	c.emitStatus("became pre-candidate")
}

func (c *Core) becomeCandidate() {
	c.role = Candidate
	c.leader = 0
	c.term++
	c.vote = c.id
	c.votes = map[NodeID]bool{c.id: true}
	c.preVotes = make(map[NodeID]bool)
	c.resetElectionTimeout()
	hsID := c.persistHardState()
	c.emitStatus("became candidate")
	// Single voter: self is a quorum.
	if c.config.QuorumActive(c.votes) {
		c.becomeLeader()
		return
	}
	for _, set := range c.config.VoterSets() {
		for _, id := range set {
			if id == c.id {
				continue
			}
			c.out.msg(Message{
				Type:         MsgVote,
				From:         c.id,
				To:           id,
				Term:         c.term,
				LastLogIndex: c.log.lastIndex(),
				LastLogTerm:  c.log.lastTerm(),
			}, hsID)
		}
	}
}

func (c *Core) campaignPreVote() {
	c.becomePreCandidate()
	if c.config.QuorumActive(c.preVotes) {
		c.becomeCandidate()
		return
	}
	for _, set := range c.config.VoterSets() {
		for _, id := range set {
			if id == c.id {
				continue
			}
			c.out.msg(Message{
				Type:         MsgPreVote,
				From:         c.id,
				To:           id,
				Term:         c.term + 1,
				LastLogIndex: c.log.lastIndex(),
				LastLogTerm:  c.log.lastTerm(),
			}, 0)
		}
	}
}

func (c *Core) becomeLeader() {
	c.record("leader", c.commitIndex, fmt.Sprintf("inheritedCommit=%d last=%d", c.commitIndex, c.log.lastIndex()))
	c.role = Leader
	c.leader = c.id
	c.leaderCommit = c.commitIndex
	c.quorumElapsed = 0
	c.heartbeatElapsed = 0
	c.currentTermCommitted = false
	c.progress = make(map[NodeID]*Progress)
	next := c.log.lastIndex() + 1
	for _, set := range c.config.VoterSets() {
		for _, id := range set {
			if id == c.id {
				continue
			}
			c.progress[id] = &Progress{Next: next, Match: 0, RecentActive: true}
		}
	}
	for _, id := range c.config.Learners {
		if _, ok := c.progress[id]; !ok {
			c.progress[id] = &Progress{Next: next, Match: 0, RecentActive: true}
		}
	}
	// Append a no-op in the current term so idle clusters establish current-term
	// commitment and can complete read barriers (§4).
	noop := Entry{Term: c.term, Index: c.log.lastIndex() + 1, Type: EntryNormal}
	c.log.append([]Entry{noop})
	c.leaderNoopIndex = noop.Index
	c.persistEntries(0, []Entry{noop}, nil)
	c.emitStatus("became leader")
	c.broadcastAppend()
}

// propose admits a command on the leader.
func (c *Core) propose(id uint64, data []byte) {
	if c.role != Leader {
		c.out.Proposals = append(c.out.Proposals, ProposalResult{ID: id, Outcome: OutcomeRejected})
		return
	}
	e := Entry{Term: c.term, Index: c.log.lastIndex() + 1, Type: EntryNormal, Data: data}
	c.log.append([]Entry{e})
	c.proposals[id] = proposalTrack{index: e.Index, term: e.Term}
	c.persistEntries(0, []Entry{e}, nil)
	c.broadcastAppend()
}

// proposeConfChange begins a serialized joint-consensus transition (§4.4).
func (c *Core) proposeConfChange(id uint64, target Configuration) {
	if c.role != Leader {
		c.out.Proposals = append(c.out.Proposals, ProposalResult{ID: id, Outcome: OutcomeRejected})
		return
	}
	if c.config.InJoint() || c.pendingConfTarget != nil {
		// Serialize transitions: reject until the current one finalizes.
		c.out.Proposals = append(c.out.Proposals, ProposalResult{ID: id, Outcome: OutcomeRejected})
		return
	}
	target = target.Clone()
	joint := Configuration{
		Voters:   target.Voters,
		Learners: target.Learners,
		Joint:    &JointConfig{OldVoters: append(NodeSet(nil), c.config.Voters...)},
	}
	c.pendingConfTarget = &target
	e := Entry{Term: c.term, Index: c.log.lastIndex() + 1, Type: EntryConfChange, Data: confEncode(joint)}
	c.log.append([]Entry{e})
	c.applyConfigDelta(0, []Entry{e})
	c.proposals[id] = proposalTrack{index: e.Index, term: e.Term}
	c.persistEntries(0, []Entry{e}, nil)
	c.broadcastAppend()
}

// finalizeConfChange appends the final configuration once joint commits.
func (c *Core) finalizeConfChange() {
	if c.pendingConfTarget == nil || !c.config.InJoint() {
		return
	}
	// Only after the joint configuration entry commits.
	jointIdx := c.confStack[len(c.confStack)-1].index
	if c.commitIndex < jointIdx {
		return
	}
	target := *c.pendingConfTarget
	c.pendingConfTarget = nil
	e := Entry{Term: c.term, Index: c.log.lastIndex() + 1, Type: EntryConfChange, Data: confEncode(target)}
	c.log.append([]Entry{e})
	c.applyConfigDelta(0, []Entry{e})
	c.persistEntries(0, []Entry{e}, nil)
	c.broadcastAppend()
}

// readBarrier captures a linearizable read index (§4.3).
func (c *Core) readBarrier(ctx []byte) {
	if c.role != Leader {
		c.out.Reads = append(c.out.Reads, ReadResult{Ctx: ctx, Err: ErrNotLeader})
		return
	}
	if !c.currentTermCommitted {
		// Wait for a current-term entry to commit; queue the read.
		c.pendingReads = append(c.pendingReads, pendingRead{ctx: ctx, index: 0})
		return
	}
	c.pendingReads = append(c.pendingReads, pendingRead{ctx: ctx, index: c.commitIndex})
}

// maybeCommit advances commitment per the Raft rules (§4, §6.2).
func (c *Core) maybeCommit() {
	if c.role == Leader {
		match := make(map[NodeID]Index)
		for id, p := range c.progress {
			match[id] = p.Match
		}
		// Self counts only when locally durable through that index (§6.2).
		q := quorumIndex(c.config.VoterSets(), match, c.id, c.durableIndex)
		if q > c.commitIndex && q <= c.durableIndex && c.log.matchTerm(q, c.term) {
			c.record("commit", q, fmt.Sprintf("selfDurable=%d match=%v", c.durableIndex, match))
			c.commitIndex = q
			if q >= c.leaderNoopIndex {
				c.currentTermCommitted = true
			}
			c.persistCommitWatermark(q)
			c.finalizeConfChange()
			c.resolveProposals()
		}
		return
	}
	// Follower: advance only through the leader-verified matching prefix that
	// is locally durable (conservative policy: commit <= durable). Bounding by
	// leaderMatch (not lastIndex) prevents committing a divergent suffix that
	// arrived via a heartbeat before the correcting AppendEntries (§4).
	candidate := c.leaderCommit
	if candidate > c.leaderMatch {
		candidate = c.leaderMatch
	}
	if candidate > c.durableIndex {
		candidate = c.durableIndex
	}
	if candidate > c.commitIndex {
		c.record("adopt", candidate, fmt.Sprintf("leaderCommit=%d last=%d durable=%d", c.leaderCommit, c.log.lastIndex(), c.durableIndex))
		c.commitIndex = candidate
		c.persistCommitWatermark(candidate)
	}
}

func (c *Core) persistCommitWatermark(idx Index) {
	commit := idx
	id := c.newWork(WorkPersist, pendingWork{commit: idx, hasCommit: true})
	c.out.work(Work{ID: id, Epoch: c.epoch, Kind: WorkPersist, Persist: PersistBatch{Commit: &commit}})
}

// resolveProposals emits Applied results for proposals at or below appliedIndex.
func (c *Core) resolveProposals() {
	for _, id := range c.sortedProposalIDs() {
		p := c.proposals[id]
		if p.index <= c.appliedIndex {
			c.out.Proposals = append(c.out.Proposals, ProposalResult{ID: id, Outcome: OutcomeApplied, Index: p.index, Term: p.term})
			delete(c.proposals, id)
		}
	}
}

func (c *Core) broadcastHeartbeat() {
	for _, id := range c.sortedProgress() {
		c.out.msg(Message{
			Type:   MsgHeartbeat,
			From:   c.id,
			To:     id,
			Term:   c.term,
			Commit: c.commitIndex,
		}, 0)
	}
}

// broadcastAppend sends AppendEntries (or snapshots) to each peer.
func (c *Core) broadcastAppend() {
	for _, id := range c.sortedProgress() {
		c.sendAppend(id, c.progress[id])
	}
}

func (c *Core) sendAppend(to NodeID, p *Progress) {
	if p.PendingSnapshot > 0 {
		return // snapshot in flight
	}
	prevIndex := p.Next - 1
	if prevIndex < c.log.snapshot.Index {
		// Follower needs compacted history: send a snapshot (§6.4).
		c.sendSnapshot(to, p)
		return
	}
	prevTerm := c.log.term(prevIndex)
	entries := c.log.entries(p.Next, c.log.lastIndex()+1)
	// Entries beyond local durability are not yet sent under the conservative
	// policy; the leader replicates only its durable prefix.
	var durableEntries []Entry
	for _, e := range entries {
		if e.Index <= c.durableIndex {
			durableEntries = append(durableEntries, e)
		}
	}
	c.out.msg(Message{
		Type:     MsgAppend,
		From:     c.id,
		To:       to,
		Term:     c.term,
		LogIndex: prevIndex,
		LogTerm:  prevTerm,
		Entries:  durableEntries,
		Commit:   c.commitIndex,
	}, 0)
}

func (c *Core) sendSnapshot(to NodeID, p *Progress) {
	snap := c.log.snapshot
	if snap.Empty() {
		return
	}
	p.PendingSnapshot = snap.Index
	c.out.msg(Message{
		Type:     MsgSnapshot,
		From:     c.id,
		To:       to,
		Term:     c.term,
		Snapshot: snap,
	}, 0)
}

// onSnapshotInstalled completes a follower snapshot restore (§6.4).
func (c *Core) onSnapshotInstalled(ev Event) {
	pw, ok := c.pending[ev.WorkID]
	if !ok || ev.Epoch != c.epoch || pw.kind != WorkSnapshotInstall {
		return
	}
	delete(c.pending, ev.WorkID)
	snap := pw.snapshot
	c.installingSnapshot = nil
	c.log.applySnapshot(snap)
	c.resetConfigToSnapshot(snap)
	if snap.Index > c.durableIndex {
		c.durableIndex = snap.Index
	}
	if snap.Index > c.commitIndex {
		c.commitIndex = snap.Index
	}
	if snap.Index > c.persistedCommit {
		c.persistedCommit = snap.Index
	}
	if snap.Index > c.dispatchedIndex {
		c.dispatchedIndex = snap.Index
	}
	if snap.Index > c.appliedIndex {
		c.appliedIndex = snap.Index
	}
	c.leaderCommit = c.commitIndex
	c.leaderMatch = snap.Index
	c.resolveProposals()
	c.emitStatus("snapshot installed")
	// Acknowledge the snapshot to the leader.
	if c.leader != 0 {
		c.out.msg(Message{
			Type:       MsgSnapshotResp,
			From:       c.id,
			To:         c.leader,
			Term:       c.term,
			MatchIndex: snap.Index,
		}, 0)
	}
}

// onSnapshotCreated publishes a captured snapshot and reclaims eligible history
// (§6.4). The snapshot is authoritative only after the durable publication.
func (c *Core) onSnapshotCreated(ev Event) {
	pw, ok := c.pending[ev.WorkID]
	if !ok || ev.Epoch != c.epoch || pw.kind != WorkSnapshotCreate {
		return
	}
	delete(c.pending, ev.WorkID)
	snap := pw.snapshot
	if !ev.Snapshot.Empty() {
		snap = ev.Snapshot
	}
	if snap.Index <= c.log.snapshot.Index {
		return
	}
	commit := snap.Index
	id := c.newWork(WorkPersist, pendingWork{durableIndex: snap.Index, commit: commit, hasCommit: true})
	c.out.work(Work{ID: id, Epoch: c.epoch, Kind: WorkPersist, Persist: PersistBatch{
		Snapshot: &snap, Commit: &commit,
	}})
	// Reclaim history covered by the snapshot, retaining the suffix above it.
	c.log.compact(snap)
	c.compactConf(snap)
}

// compactConf resets configuration tracking to a snapshot, retaining conf
// entries still present above the snapshot index.
func (c *Core) compactConf(snap SnapshotMeta) {
	c.confBase = snap.Config.Clone()
	var keep []confRecord
	for _, r := range c.confStack {
		if r.index > snap.Index {
			keep = append(keep, r)
		}
	}
	c.confStack = keep
	c.config = c.effectiveConfig()
}

// RequestSnapshot asks the driver to capture an FSM view at the applied index.
func (c *Core) RequestSnapshot() Outputs {
	c.out = Outputs{}
	c.requestSnapshot()
	return c.out
}

func (c *Core) requestSnapshot() {
	if c.appliedIndex <= c.log.snapshot.Index {
		return
	}
	snap := SnapshotMeta{
		Index:  c.appliedIndex,
		Term:   c.log.term(c.appliedIndex),
		Config: c.config.Clone(),
	}
	id := c.newWork(WorkSnapshotCreate, pendingWork{snapshot: snap})
	c.out.work(Work{ID: id, Epoch: c.epoch, Kind: WorkSnapshotCreate, Snapshot: snap})
}

var errSnapshotStale = errors.New("raft: stale snapshot ignored")
