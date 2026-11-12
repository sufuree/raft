package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestWireRoundTrip(t *testing.T) {
	msgs := []Message{
		{Type: MsgVote, From: 1, To: 2, Term: 7, LastLogIndex: 9, LastLogTerm: 6},
		{Type: MsgAppend, From: 2, To: 1, Term: 7, LogIndex: 9, LogTerm: 6, Commit: 8,
			Entries: []Entry{
				{Term: 7, Index: 10, Type: EntryNormal, Data: []byte("cmd")},
				{Term: 7, Index: 11, Type: EntryConfChange, Data: EncodeConfig(Configuration{Voters: NodeSet{1, 2}})},
			}},
		{Type: MsgAppendResp, From: 1, To: 2, Term: 7, Reject: true, RejectHint: 4},
		{Type: MsgHeartbeat, From: 2, To: 1, Term: 7, Commit: 8, ReadCtx: []byte("ctx-123")},
		{Type: MsgSnapshot, From: 2, To: 1, Term: 7, Snapshot: SnapshotMeta{
			Index: 20, Term: 7, Config: Configuration{Voters: NodeSet{1, 2, 3}, Learners: NodeSet{4}},
			Data: []byte("snapshot-bytes"),
		}},
	}
	for i, m := range msgs {
		b := EncodeMessage(m)
		got, err := DecodeMessage(b, 1<<20)
		if err != nil {
			t.Fatalf("msg %d: %v", i, err)
		}
		if got.Type != m.Type || got.From != m.From || got.To != m.To || got.Term != m.Term ||
			got.LogIndex != m.LogIndex || got.LogTerm != m.LogTerm || got.Commit != m.Commit ||
			got.Reject != m.Reject || got.RejectHint != m.RejectHint || got.MatchIndex != m.MatchIndex ||
			got.LastLogIndex != m.LastLogIndex || got.LastLogTerm != m.LastLogTerm ||
			!bytes.Equal(got.ReadCtx, m.ReadCtx) ||
			got.Snapshot.Index != m.Snapshot.Index || got.Snapshot.Term != m.Snapshot.Term ||
			!bytes.Equal(got.Snapshot.Data, m.Snapshot.Data) ||
			len(got.Entries) != len(m.Entries) {
			t.Fatalf("msg %d round trip mismatch:\n in %+v\nout %+v", i, m, got)
		}
		for j := range m.Entries {
			if got.Entries[j].Term != m.Entries[j].Term || got.Entries[j].Index != m.Entries[j].Index ||
				got.Entries[j].Type != m.Entries[j].Type || !bytes.Equal(got.Entries[j].Data, m.Entries[j].Data) {
				t.Fatalf("msg %d entry %d mismatch", i, j)
			}
		}
	}
}

func TestWireDecodeBounds(t *testing.T) {
	m := Message{Type: MsgAppend, From: 1, To: 2, Term: 3,
		Entries: []Entry{{Term: 3, Index: 1, Data: bytes.Repeat([]byte("x"), 4096)}}}
	b := EncodeMessage(m)

	// Explicit decode limit rejects before allocation.
	if _, err := DecodeMessage(b, 100); !errors.Is(err, ErrMsgTooLarge) {
		t.Fatalf("want ErrMsgTooLarge, got %v", err)
	}
	// Unsupported version rejected.
	bad := append([]byte(nil), b...)
	bad[0] = WireVersion + 1
	if _, err := DecodeMessage(bad, 1<<20); !errors.Is(err, ErrWireVersion) {
		t.Fatalf("want ErrWireVersion, got %v", err)
	}
	// Every truncation of a valid encoding must fail cleanly, never panic.
	for i := 0; i < len(b); i++ {
		if _, err := DecodeMessage(b[:i], 1<<20); err == nil {
			t.Fatalf("truncation at %d decoded without error", i)
		}
	}
	// Trailing garbage rejected.
	if _, err := DecodeMessage(append(append([]byte(nil), b...), 0), 1<<20); err == nil {
		t.Fatal("trailing garbage accepted")
	}
	// Impossible entry count rejected before allocation.
	hdr := append([]byte(nil), b[:87]...) // fixed fields (83) + entry count (4)
	binary.BigEndian.PutUint32(hdr[83:87], 0xFFFFFFFF)
	if _, err := DecodeMessage(hdr, 1<<20); err == nil {
		t.Fatal("impossible entry count accepted")
	}
}
