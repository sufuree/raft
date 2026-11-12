package raft

import "encoding/binary"

// pendingRead is a captured read barrier awaiting a confirmation round.
type pendingRead struct {
	ctx   []byte
	index Index
}

// readRound is a sent leadership-confirmation round (§4.3). Only acks for this
// round count toward its reads; earlier rounds cannot establish the barrier.
type readRound struct {
	ctx       []byte
	reads     []pendingRead
	acks      map[NodeID]bool
	confirmed bool
}

// roundCtx builds a unique, deterministic confirmation context.
func (c *Core) roundCtx() []byte {
	c.readRoundSeq++
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[0:8], c.id.uint())
	binary.BigEndian.PutUint64(b[8:16], c.readRoundSeq)
	return b
}

func (id NodeID) uint() uint64 { return uint64(id) }

// flushReads sends one confirmation round for all reads captured since the last
// round was sent. Reads may join a batch only before that round is sent.
func (c *Core) flushReads() {
	if len(c.pendingReads) == 0 || c.role != Leader || !c.currentTermCommitted {
		return
	}
	// Reads that arrived before the current-term entry committed capture the
	// commit index now — never index 0 (§4.3).
	for i := range c.pendingReads {
		if c.pendingReads[i].index == 0 {
			c.pendingReads[i].index = c.commitIndex
		}
	}
	r := &readRound{
		ctx:   c.roundCtx(),
		reads: c.pendingReads,
		acks:  map[NodeID]bool{c.id: true},
	}
	c.pendingReads = nil
	c.readRounds = append(c.readRounds, r)
	// Self may already form a quorum (single voter): no acks will arrive, so
	// confirm immediately (§4.3).
	if c.config.QuorumActive(r.acks) {
		r.confirmed = true
	}
	for _, set := range c.config.VoterSets() {
		for _, id := range set {
			if id == c.id {
				continue
			}
			c.out.msg(Message{
				Type:    MsgHeartbeat,
				From:    c.id,
				To:      id,
				Term:    c.term,
				Commit:  c.commitIndex,
				ReadCtx: r.ctx,
			}, 0)
		}
	}
	c.maybeCompleteReads()
}

// onReadAck records a confirmation-round acknowledgement.
func (c *Core) onReadAck(from NodeID, ctx []byte) {
	for _, r := range c.readRounds {
		if !bytesEqual(r.ctx, ctx) {
			continue
		}
		r.acks[from] = true
		if c.config.QuorumActive(r.acks) {
			r.confirmed = true
		}
	}
	c.maybeCompleteReads()
}

// maybeCompleteReads emits results for confirmed reads whose index is applied.
func (c *Core) maybeCompleteReads() {
	var keep []*readRound
	for _, r := range c.readRounds {
		var remaining []pendingRead
		for _, pr := range r.reads {
			if r.confirmed && c.appliedIndex >= pr.index {
				c.out.Reads = append(c.out.Reads, ReadResult{Ctx: pr.ctx, Index: pr.index})
			} else {
				remaining = append(remaining, pr)
			}
		}
		r.reads = remaining
		if len(r.reads) > 0 {
			keep = append(keep, r)
		}
	}
	c.readRounds = keep
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
