package raft

import (
	"bytes"
	"testing"
)

// FuzzDecodeMessage verifies arbitrary bytes never panic the bounded message
// decoder (§8, §11), and valid decodings round-trip deterministically.
func FuzzDecodeMessage(f *testing.F) {
	f.Add(EncodeMessage(Message{Type: MsgAppend, From: 1, To: 2, Term: 3,
		Entries: []Entry{{Term: 3, Index: 1, Data: []byte("x")}}}))
	f.Add(EncodeMessage(Message{Type: MsgSnapshot, From: 1, To: 2, Term: 3,
		Snapshot: SnapshotMeta{Index: 5, Term: 3, Config: Configuration{Voters: NodeSet{1, 2}}, Data: []byte("s")}}))
	f.Add([]byte{1, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeMessage(data, 1<<20)
		if err != nil {
			return
		}
		again := EncodeMessage(m)
		m2, err := DecodeMessage(again, 1<<20)
		if err != nil {
			t.Fatalf("re-encode failed to decode: %v", err)
		}
		if m2.Type != m.Type || m2.From != m.From || m2.To != m.To || m2.Term != m.Term ||
			len(m2.Entries) != len(m.Entries) || !bytes.Equal(m2.ChunkData, m.ChunkData) {
			t.Fatal("round trip mismatch")
		}
	})
}

// FuzzDecodeConfig verifies configuration decoding never panics or allocates
// beyond the payload (§8).
func FuzzDecodeConfig(f *testing.F) {
	f.Add(EncodeConfig(Configuration{Voters: NodeSet{1, 2, 3}, Learners: NodeSet{4},
		Joint: &JointConfig{OldVoters: NodeSet{1, 2}}}))
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0x7f})
	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := DecodeConfig(data)
		if err != nil {
			return
		}
		c2, err := DecodeConfig(EncodeConfig(c))
		if err != nil {
			t.Fatalf("re-encode failed: %v", err)
		}
		if len(c2.Voters) != len(c.Voters) || len(c2.Learners) != len(c.Learners) || c2.InJoint() != c.InJoint() {
			t.Fatal("round trip mismatch")
		}
	})
}
