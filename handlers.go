package raft

import "fmt"

func (c *Core) receive(m Message) {
	switch m.Type {
	case MsgPreVote:
		c.handlePreVote(m)
		return
	case MsgPreVoteResp:
		c.handlePreVoteResp(m)
		return
	}
	if m.Term > c.term {
		// Disruption guard (§4.1): a node following a live leader refuses to
		// step down for a higher-term vote request from a rejoining member.
		if m.Type == MsgVote && c.leaderRecentlySeen() {
			c.out.msg(Message{Type: MsgVoteResp, From: c.id, To: m.From, Term: c.term, Reject: true}, 0)
			return
		}
		switch m.Type {
		case MsgVote:
			c.becomeFollower(m.Term, 0)
		default:
			c.becomeFollower(m.Term, m.From)
		}
	} else if m.Term < c.term {
		if m.Type == MsgVote {
			c.out.msg(Message{Type: MsgVoteResp, From: c.id, To: m.From, Term: c.term, Reject: true}, 0)
		}
		return
	}
	switch m.Type {
	case MsgVote:
		c.handleVote(m)
	case MsgVoteResp:
		c.handleVoteResp(m)
	case MsgAppend:
		c.handleAppend(m)
	case MsgAppendResp:
		c.handleAppendResp(m)
	case MsgHeartbeat:
		c.handleHeartbeat(m)
	case MsgHeartbeatResp:
		c.handleHeartbeatResp(m)
	case MsgSnapshot:
		c.handleSnapshot(m)
	case MsgSnapshotResp:
		c.handleSnapshotResp(m)
	case MsgSnapshotChunk:
		c.handleSnapshotChunk(m)
	case MsgSnapshotChunkAck:
		c.handleSnapshotChunkAck(m)
	}
}

// leaderRecentlySeen is the disruption guard (§4.1): a node that has heard from
// a leader within the election timeout refuses (pre-)vote requests.
func (c *Core) leaderRecentlySeen() bool {
	return c.leader != 0 && c.leaderSeenElapsed < c.cfg.ElectionTick
}

func (c *Core) handlePreVote(m Message) {
	grant := !c.leaderRecentlySeen() &&
		m.Term >= c.term &&
		c.log.upToDate(m.LastLogIndex, m.LastLogTerm)
	c.out.msg(Message{Type: MsgPreVoteResp, From: c.id, To: m.From, Term: c.term, Reject: !grant}, 0)
}

func (c *Core) handlePreVoteResp(m Message) {
	if c.role != PreCandidate {
		return
	}
	if m.Reject {
		return
	}
	c.preVotes[m.From] = true
	if c.config.QuorumActive(c.preVotes) {
		c.becomeCandidate()
	}
}

func (c *Core) handleVote(m Message) {
	if c.leaderRecentlySeen() {
		c.out.msg(Message{Type: MsgVoteResp, From: c.id, To: m.From, Term: c.term, Reject: true}, 0)
		return
	}
	canGrant := (c.vote == 0 || c.vote == m.From) && c.log.upToDate(m.LastLogIndex, m.LastLogTerm)
	if canGrant {
		c.vote = m.From
		c.electionElapsed = 0
		hsID := c.persistHardState()
		c.out.msg(Message{Type: MsgVoteResp, From: c.id, To: m.From, Term: c.term, Reject: false}, hsID)
	} else {
		c.out.msg(Message{Type: MsgVoteResp, From: c.id, To: m.From, Term: c.term, Reject: true}, 0)
	}
}

func (c *Core) handleVoteResp(m Message) {
	if c.role != Candidate || m.Term != c.term {
		return
	}
	if m.Reject {
		return
	}
	c.votes[m.From] = true
	if c.config.QuorumActive(c.votes) {
		c.becomeLeader()
	}
}

func (c *Core) handleAppend(m Message) {
	if c.role == Candidate || c.role == PreCandidate {
		c.becomeFollower(c.term, m.From) // leader contact at own term (§4)
	}
	c.leader = m.From
	c.leaderSeenElapsed = 0
	c.electionElapsed = 0
	c.leaderCommit = m.Commit

	if !c.log.matchTerm(m.LogIndex, m.LogTerm) {
		hint := c.log.lastIndex()
		if m.LogIndex < c.log.firstIndex() {
			hint = c.log.snapshot.Index
		}
		c.out.msg(Message{
			Type: MsgAppendResp, From: c.id, To: m.From, Term: c.term,
			Reject: true, RejectHint: hint,
		}, 0)
		return
	}

	// Never overwrite committed entries (§4): refuse a conflicting suffix
	// that would truncate the committed prefix, before mutating the log.
	if conflict := c.log.conflictIndex(m.Entries); conflict > 0 && conflict <= c.commitIndex {
		c.out.msg(Message{
			Type: MsgAppendResp, From: c.id, To: m.From, Term: c.term,
			Reject: true, RejectHint: c.commitIndex,
		}, 0)
		return
	}
	lastNew := m.LogIndex + Index(len(m.Entries))
	trunc, appended := c.appendLog(m.Entries)
	// Defense in depth: the pre-check above makes this unreachable.
	if trunc > 0 && trunc <= c.commitIndex {
		return
	}
	c.applyConfigDelta(trunc, appended)
	// The prefix through lastNew is now verified to match the leader.
	if lastNew > c.leaderMatch {
		c.leaderMatch = lastNew
	}

	commitHint := m.Commit
	if commitHint > lastNew {
		commitHint = lastNew
	}
	if len(appended) == 0 && trunc == 0 && commitHint <= c.persistedCommit {
		// Nothing new to persist: acknowledge immediately. Re-persisting
		// idempotent appends would amplify probe traffic into storage load.
		c.out.msg(Message{
			Type: MsgAppendResp, From: c.id, To: m.From, Term: c.term,
			Reject: false, MatchIndex: lastNew,
		}, 0)
		c.maybeCommit()
		return
	}
	if len(m.Entries) > 0 || trunc > 0 {
		var cp *Index
		if commitHint > c.persistedCommit {
			cp = &commitHint
		}
		pid := c.persistEntries(trunc, m.Entries, cp)
		c.out.msg(Message{
			Type: MsgAppendResp, From: c.id, To: m.From, Term: c.term,
			Reject: false, MatchIndex: lastNew,
		}, pid)
	} else {
		if commitHint > c.persistedCommit {
			c.persistCommitWatermark(commitHint)
		}
		c.out.msg(Message{
			Type: MsgAppendResp, From: c.id, To: m.From, Term: c.term,
			Reject: false, MatchIndex: lastNew,
		}, 0)
	}
	c.maybeCommit()
}

func (c *Core) handleAppendResp(m Message) {
	if c.role != Leader {
		return
	}
	p := c.progress[m.From]
	if p == nil {
		return
	}
	p.RecentActive = true
	c.quorumElapsed = 0
	p.AppendInflight = false
	p.InflightElapsed = 0
	if m.Reject {
		p.Next = m.RejectHint + 1
		if p.Next < 1 {
			p.Next = 1
		}
		c.sendAppend(m.From, p)
		return
	}
	if m.MatchIndex > p.Match {
		c.record("match", m.MatchIndex, fmt.Sprintf("from=%d prevMatch=%d", m.From, p.Match))
		p.Match = m.MatchIndex
	}
	p.Next = p.Match + 1
	c.maybeCommit()
	if p.Next <= c.log.lastIndex() {
		c.sendAppend(m.From, p)
	}
}

func (c *Core) handleHeartbeat(m Message) {
	if c.role == Candidate || c.role == PreCandidate {
		c.becomeFollower(c.term, m.From)
	}
	c.leader = m.From
	c.leaderSeenElapsed = 0
	c.electionElapsed = 0
	c.leaderCommit = m.Commit
	c.maybeCommit()
	c.out.msg(Message{
		Type: MsgHeartbeatResp, From: c.id, To: m.From, Term: c.term, ReadCtx: m.ReadCtx,
		// Report the local commit so the leader can probe a follower whose
		// commit watermark lagged a restart (MatchIndex is informational;
		// commitment still advances only through a verified AppendEntries).
		MatchIndex: c.commitIndex,
	}, 0)
}

func (c *Core) handleHeartbeatResp(m Message) {
	if c.role != Leader {
		return
	}
	p := c.progress[m.From]
	if p != nil {
		p.RecentActive = true
		// Probe/replicate to a follower that may be behind (e.g. after restart),
		// or whose commit watermark lags the leader's commit (a restart can
		// lose the last commit-watermark batch while the log survived). An
		// in-flight snapshot transfer is NOT cleared here: heartbeat acks
		// during an active stream would otherwise restart it forever; stalled
		// transfers time out via StreamElapsed (§6.4).
		if p.Match < c.log.lastIndex() {
			c.sendAppend(m.From, p)
		} else if m.MatchIndex < c.commitIndex && p.ProbeElapsed >= c.cfg.ElectionTick {
			// Fully replicated but commit watermark lags (e.g. after a restart
			// that lost the last commit batch): probe to re-verify the prefix.
			// Cooldown-gated; the previous in-flight append may have been lost.
			p.ProbeElapsed = 0
			p.AppendInflight = false
			c.sendAppend(m.From, p)
		}
	}
	c.quorumElapsed = 0
	if len(m.ReadCtx) > 0 {
		c.onReadAck(m.From, m.ReadCtx)
	}
}

func (c *Core) handleSnapshot(m Message) {
	c.leader = m.From
	c.leaderSeenElapsed = 0
	c.electionElapsed = 0
	snap := m.Snapshot
	// Stale-snapshot guard (§6.4): never regress applied/committed state.
	if snap.Index <= c.appliedIndex || snap.Index <= c.commitIndex {
		if c.leader != 0 {
			c.out.msg(Message{
				Type: MsgSnapshotResp, From: c.id, To: c.leader, Term: c.term, MatchIndex: c.appliedIndex,
			}, 0)
		}
		return
	}
	c.installingSnapshot = &snap
	id := c.newWork(WorkSnapshotInstall, pendingWork{snapshot: snap})
	c.out.work(Work{ID: id, Epoch: c.epoch, Kind: WorkSnapshotInstall, Snapshot: snap})
}

func (c *Core) handleSnapshotResp(m Message) {
	if c.role != Leader {
		return
	}
	p := c.progress[m.From]
	if p == nil {
		return
	}
	p.RecentActive = true
	c.quorumElapsed = 0
	if m.MatchIndex > p.Match {
		p.Match = m.MatchIndex
	}
	p.Next = p.Match + 1
	p.PendingSnapshot = 0
	p.StreamOffset = 0
	c.maybeCommit()
	if p.Next <= c.log.lastIndex() {
		c.sendAppend(m.From, p)
	}
}
