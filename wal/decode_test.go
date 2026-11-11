package wal

import (
	"encoding/binary"
	"testing"

	raft "github.com/sufuree/raft"
)

// TestDecodeBatchAllocationCap verifies a malformed entry count cannot drive
// unbounded allocation (§8): the count must be plausible against the payload,
// and decoding stops at the first short field.
func TestDecodeBatchAllocationCap(t *testing.T) {
	// Header: no hard state, truncate 0, then entry count 0xFFFFFFFF.
	b := []byte{0}
	b = binary.BigEndian.AppendUint64(b, 0)
	b = binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
	if _, err := decodeBatch(b); err == nil {
		t.Fatal("expected error for impossible entry count")
	}
	// A plausible count with truncated entry bytes must also fail promptly.
	b = []byte{0}
	b = binary.BigEndian.AppendUint64(b, 0)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = append(b, 1, 2, 3) // far short of one entry
	if _, err := decodeBatch(b); err == nil {
		t.Fatal("expected error for short entry payload")
	}
}

// TestBatchRoundTrip guards the bounded decoder against regressions.
func TestBatchRoundTrip(t *testing.T) {
	commit := raft.Index(7)
	hs := raft.HardState{Term: 3, Vote: 2, Commit: 7}
	batch := raft.PersistBatch{
		HardState: &hs,
		Entries:   []raft.Entry{{Term: 3, Index: 8, Type: raft.EntryNormal, Data: []byte("x")}},
		Commit:    &commit,
	}
	got, err := decodeBatch(encodeBatch(batch))
	if err != nil {
		t.Fatal(err)
	}
	if got.HardState == nil || got.HardState.Term != 3 || len(got.Entries) != 1 || got.Commit == nil || *got.Commit != 7 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}
