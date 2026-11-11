package raft

import (
	"encoding/binary"
	"testing"
)

// TestConfDecodeAllocationCap verifies a malformed configuration cannot drive
// an attacker-controlled allocation (§8): the set count must be plausible
// against the remaining payload before any slice is allocated.
func TestConfDecodeAllocationCap(t *testing.T) {
	// Voters count = 2^60 with no IDs following.
	b := binary.AppendUvarint(nil, 1<<60)
	if _, err := DecodeConfig(b); err == nil {
		t.Fatal("expected error for impossible voter count")
	}
	// Valid voters/learners, then an impossible joint old-voter count.
	b = binary.AppendUvarint(nil, 1)
	b = binary.AppendUvarint(b, 1)
	b = binary.AppendUvarint(b, 0)
	b = append(b, 1)
	b = binary.AppendUvarint(b, 1<<60)
	if _, err := DecodeConfig(b); err == nil {
		t.Fatal("expected error for impossible joint count")
	}
	// A well-formed configuration still round-trips.
	c := Configuration{Voters: NodeSet{1, 2, 3}, Learners: NodeSet{4},
		Joint: &JointConfig{OldVoters: NodeSet{1, 2}}}
	got, err := DecodeConfig(EncodeConfig(c))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Voters) != 3 || len(got.Learners) != 1 || !got.InJoint() {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}
