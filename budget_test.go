package raft

import (
	"bytes"
	"testing"
)

// drive completes all work in outputs until none remains, modelling an
// always-successful storage/FSM (deterministic, single-owner).
func drive(c *Core, out Outputs) Outputs {
	for len(out.Work) > 0 {
		w := out.Work[0]
		out.Work = out.Work[1:]
		var ev Event
		switch w.Kind {
		case WorkPersist:
			ev = Event{Kind: EvPersisted, Epoch: w.Epoch, WorkID: w.ID}
		case WorkApply:
			ev = Event{Kind: EvApplied, Epoch: w.Epoch, WorkID: w.ID}
		case WorkSnapshotInstall:
			ev = Event{Kind: EvSnapshotInstalled, Epoch: w.Epoch, WorkID: w.ID}
		case WorkSnapshotCreate:
			ev = Event{Kind: EvSnapshotCreated, Epoch: w.Epoch, WorkID: w.ID, Snapshot: w.Snapshot}
		}
		more := c.Step(ev)
		out.Work = append(out.Work, more.Work...)
		out.Messages = append(out.Messages, more.Messages...)
		out.Proposals = append(out.Proposals, more.Proposals...)
	}
	return out
}

func singleLeader(t *testing.T, cfg Config) *Core {
	t.Helper()
	if cfg.ID == 0 {
		cfg.ID = 1
	}
	c := NewCore(cfg, InitState{Config: Configuration{Voters: NodeSet{cfg.ID}}})
	for i := 0; i < 3*cfg.ElectionTick+30 && c.Role() != Leader; i++ {
		drive(c, c.Step(Event{Kind: EvTick}))
	}
	if c.Role() != Leader {
		t.Fatalf("did not become leader, role %v", c.Role())
	}
	drive(c, c.Step(Event{Kind: EvTick}))
	return c
}

// TestCommittedPrefixNeverTruncated: an AppendEntries whose conflicting
// suffix would overwrite committed entries is refused without mutating the
// in-memory log (§4). Regression: the conflict check previously ran after
// log.append had already truncated.
func TestCommittedPrefixNeverTruncated(t *testing.T) {
	c := singleLeader(t, Config{ElectionTick: 5, HeartbeatTick: 1})
	for i := 0; i < 3; i++ {
		out := c.Step(Event{Kind: EvPropose, WorkID: uint64(i + 1), Data: []byte("cmd")})
		drive(c, out)
	}
	_, commit, _, _, _ := c.Watermarks()
	if commit < 4 { // noop + 3 commands
		t.Fatalf("commit %d too low", commit)
	}
	before := c.DebugLog()
	// Conflicting entry at index 2 (committed) with a different term.
	out := c.Step(Event{Kind: EvReceive, Message: Message{
		Type: MsgAppend, From: 2, To: 1, Term: c.Term(),
		LogIndex: 1, LogTerm: before[0].Term,
		Entries: []Entry{{Term: c.Term() + 5, Index: 2, Data: []byte("evil")}},
		Commit:  1,
	}})
	var resp *OutMessage
	for i := range out.Messages {
		if out.Messages[i].Message.Type == MsgAppendResp {
			resp = &out.Messages[i]
		}
	}
	if resp == nil || !resp.Message.Reject {
		t.Fatal("expected rejected AppendResp for committed-prefix conflict")
	}
	after := c.DebugLog()
	if len(after) != len(before) {
		t.Fatalf("log mutated: %d -> %d entries", len(before), len(after))
	}
	for i := range before {
		if after[i].Index != before[i].Index || after[i].Term != before[i].Term || !bytes.Equal(after[i].Data, before[i].Data) {
			t.Fatalf("entry %d mutated", i)
		}
	}
	_, commit2, _, _, _ := c.Watermarks()
	if commit2 != commit {
		t.Fatalf("commit regressed: %d -> %d", commit, commit2)
	}
}

// TestUncommittedBudget: admission beyond the uncommitted-log cap returns
// typed overload, and commitment releases the budget (§8).
func TestUncommittedBudget(t *testing.T) {
	c := singleLeader(t, Config{ElectionTick: 5, HeartbeatTick: 1, MaxUncommittedBytes: 1024})
	out := c.Step(Event{Kind: EvPropose, WorkID: 1, Data: bytes.Repeat([]byte("x"), 600)})
	drive(c, out)
	// A fresh core where persistence never completes: entries stay
	// uncommitted, so the budget must reject the second proposal.
	blocked := NewCore(Config{ID: 1, ElectionTick: 5, HeartbeatTick: 1, MaxUncommittedBytes: 1024},
		InitState{Config: Configuration{Voters: NodeSet{1}}})
	for i := 0; i < 40 && blocked.Role() != Leader; i++ {
		blocked.Step(Event{Kind: EvTick}) // do not complete work
	}
	if blocked.Role() != Leader {
		t.Fatal("no leader")
	}
	blocked.Step(Event{Kind: EvPropose, WorkID: 1, Data: bytes.Repeat([]byte("x"), 600)})
	out = blocked.Step(Event{Kind: EvPropose, WorkID: 2, Data: bytes.Repeat([]byte("y"), 600)})
	if len(out.Proposals) != 1 || out.Proposals[0].Outcome != OutcomeOverload {
		t.Fatalf("expected OutcomeOverload, got %+v", out.Proposals)
	}
	// Completing persistence+application commits the first entry and frees
	// the budget; a new proposal is then admitted.
	drive(c, c.Step(Event{Kind: EvTick}))
	out = c.Step(Event{Kind: EvPropose, WorkID: 2, Data: bytes.Repeat([]byte("y"), 600)})
	if len(out.Proposals) != 0 {
		t.Fatalf("proposal rejected after budget release: %+v", out.Proposals)
	}
	drive(c, out)
}

// TestIdempotentAppendNoPersist: an AppendEntries that adds nothing new and
// carries no new commit hint is acknowledged without a persistence batch.
// Regression: re-persisting idempotent appends amplified probe traffic into
// storage load and stalled replication under burst.
func TestIdempotentAppendNoPersist(t *testing.T) {
	c := singleLeader(t, Config{ElectionTick: 5, HeartbeatTick: 1})
	out := drive(c, c.Step(Event{Kind: EvPropose, WorkID: 1, Data: []byte("cmd")}))
	_ = out
	_, commit, _, _, _ := c.Watermarks()
	last := c.log.lastIndex()
	term := c.log.lastTerm()
	// Re-deliver the same suffix with the same commit hint.
	out = c.Step(Event{Kind: EvReceive, Message: Message{
		Type: MsgAppend, From: 2, To: 1, Term: c.Term(),
		LogIndex: last - 1, LogTerm: c.log.term(last - 1),
		Entries: []Entry{{Term: term, Index: last, Data: []byte("cmd")}},
		Commit:  commit,
	}})
	for _, w := range out.Work {
		if w.Kind == WorkPersist {
			t.Fatal("idempotent append generated a persistence batch")
		}
	}
	var ack *OutMessage
	for i := range out.Messages {
		if out.Messages[i].Message.Type == MsgAppendResp {
			ack = &out.Messages[i]
		}
	}
	if ack == nil || ack.Message.Reject || ack.Message.MatchIndex != last || ack.After != 0 {
		t.Fatalf("expected immediate ack at %d, got %+v", last, ack)
	}
}
