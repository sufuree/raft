package raft

import "hash/crc32"

// snapshotStaging tracks one inbound streamed snapshot (§6.4). Chunks arrive
// in order, one in flight, acknowledged by staged byte offset; the complete
// payload installs through the ordinary snapshot accounting (restore is an
// apply). Interrupted transfers never become snapshots.
type snapshotStaging struct {
	meta  SnapshotMeta
	next  uint64 // next expected byte offset
	total uint64
	crc   uint32
}

// snapshotCRC caches the end-to-end checksum of the leader's current snapshot.
func (c *Core) snapshotCRC() uint32 {
	if c.snapCRCIndex != c.log.snapshot.Index {
		c.snapCRCIndex = c.log.snapshot.Index
		c.snapCRC = crc32.ChecksumIEEE(c.log.snapshot.Data)
	}
	return c.snapCRC
}

// sendSnapshotChunk sends the next bounded piece of the current snapshot.
func (c *Core) sendSnapshotChunk(to NodeID, p *Progress) {
	snap := c.log.snapshot
	total := len(snap.Data)
	end := p.StreamOffset + c.cfg.SnapshotChunkBytes
	if end > total {
		end = total
	}
	meta := snap
	meta.Data = nil
	c.out.msg(Message{
		Type:        MsgSnapshotChunk,
		From:        c.id,
		To:          to,
		Term:        c.term,
		Snapshot:    meta,
		ChunkOffset: uint64(p.StreamOffset),
		ChunkTotal:  uint64(total),
		ChunkCRC:    c.snapshotCRC(),
		ChunkData:   snap.Data[p.StreamOffset:end],
	}, 0)
}

// handleSnapshotChunk stages one inbound chunk under the stale-snapshot and
// size guards (§6.4, §8).
func (c *Core) handleSnapshotChunk(m Message) {
	c.leader = m.From
	c.leaderSeenElapsed = 0
	c.electionElapsed = 0
	snap := m.Snapshot
	// Stale-snapshot guard: never regress committed/applied state.
	if snap.Index <= c.appliedIndex || snap.Index <= c.commitIndex {
		c.out.msg(Message{
			Type: MsgSnapshotChunkAck, From: c.id, To: m.From, Term: c.term,
			MatchIndex: c.appliedIndex, Reject: true,
		}, 0)
		return
	}
	if m.ChunkTotal > uint64(c.cfg.MaxSnapshotBytes) {
		// Typed catch-up failure, never an unbounded allocation (§8).
		c.out.msg(Message{
			Type: MsgSnapshotChunkAck, From: c.id, To: m.From, Term: c.term, Reject: true,
		}, 0)
		c.emitStatus("snapshot catch-up rejected: exceeds local snapshot limit")
		return
	}
	if c.staging == nil || c.staging.meta.Index != snap.Index || c.staging.meta.Term != snap.Term {
		meta := snap
		meta.Config = snap.Config.Clone()
		meta.Data = nil
		c.staging = &snapshotStaging{meta: meta, total: m.ChunkTotal, crc: m.ChunkCRC}
	}
	st := c.staging
	if m.ChunkOffset < st.next {
		// The leader restarted the transfer (e.g. after a stall timeout) while
		// staging survived: acknowledge the staged prefix so it fast-forwards
		// instead of deadlocking on ignored duplicates (§6.4).
		c.out.msg(Message{
			Type: MsgSnapshotChunkAck, From: c.id, To: c.leader, Term: c.term,
			MatchIndex: Index(st.next),
		}, 0)
		return
	}
	if m.ChunkOffset != st.next || m.ChunkOffset+uint64(len(m.ChunkData)) > st.total {
		return // reordered or malformed: ignore
	}
	id := c.newWork(WorkSnapshotStage, pendingWork{stagedOffset: m.ChunkOffset + uint64(len(m.ChunkData))})
	c.out.work(Work{
		ID: id, Epoch: c.epoch, Kind: WorkSnapshotStage, Snapshot: snap,
		ChunkOffset: m.ChunkOffset, ChunkData: m.ChunkData, ChunkTotal: st.total, ChunkCRC: st.crc,
	})
}

// onSnapshotStaged advances staging; the final chunk releases the install.
func (c *Core) onSnapshotStaged(ev Event) {
	pw, ok := c.pending[ev.WorkID]
	if !ok || ev.Epoch != c.epoch || pw.kind != WorkSnapshotStage {
		return
	}
	delete(c.pending, ev.WorkID)
	st := c.staging
	if st == nil || pw.stagedOffset <= st.next || pw.stagedOffset > st.total {
		return // stale or invalid completion cannot advance state (§5)
	}
	st.next = pw.stagedOffset
	if st.next < st.total {
		c.out.msg(Message{
			Type: MsgSnapshotChunkAck, From: c.id, To: c.leader, Term: c.term,
			MatchIndex: Index(st.next),
		}, 0)
		return
	}
	// Complete: install through the ordinary accounting. The payload resolves
	// from staging and is verified end-to-end before publication (§6.4).
	meta := st.meta
	c.staging = nil
	c.installingSnapshot = &meta
	id := c.newWork(WorkSnapshotInstall, pendingWork{snapshot: meta})
	w := Work{
		ID: id, Epoch: c.epoch, Kind: WorkSnapshotInstall, Snapshot: meta,
		ChunkTotal: st.total, ChunkCRC: st.crc,
	}
	// Defer until any in-flight FSM work completes so Restore never races an
	// Apply (§6.4).
	c.pendingInstall = &w
	c.maybeFSM()
}

// handleSnapshotChunkAck drives the leader side of the stream: one chunk in
// flight per peer, advanced by acknowledged staged bytes.
func (c *Core) handleSnapshotChunkAck(m Message) {
	if c.role != Leader {
		return
	}
	p := c.progress[m.From]
	if p == nil {
		return
	}
	p.RecentActive = true
	c.quorumElapsed = 0
	p.StreamElapsed = 0
	if m.Reject {
		p.PendingSnapshot = 0
		p.StreamOffset = 0
		c.emitStatus("snapshot catch-up rejected by peer")
		return
	}
	if uint64(m.MatchIndex) > uint64(p.StreamOffset) {
		p.StreamOffset = int(m.MatchIndex)
	}
	if p.PendingSnapshot == 0 || p.PendingSnapshot != c.log.snapshot.Index {
		return // transfer superseded; a heartbeat restarts catch-up
	}
	if p.StreamOffset >= len(c.log.snapshot.Data) {
		return // fully staged; awaiting the install acknowledgement
	}
	c.sendSnapshotChunk(m.From, p)
}
