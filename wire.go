package raft

import (
	"encoding/binary"
	"errors"
)

// WireVersion is the explicit protocol frame version (§8). Incompatible
// versions are rejected before protocol participation.
const WireVersion uint8 = 1

// FramingReserve is the conservative byte reserve for message framing on top
// of a command payload: fixed message fields (83), entry header (21), empty
// snapshot metadata (24), length prefixes (8), frame header (5), and slack for
// a bounded membership configuration. Limit validation requires
// MaxCommandBytes+FramingReserve <= MaxBatchBytes <= peer decode limit (§8).
const FramingReserve = 1024

// Wire decode errors (§8). All are typed so transports can reject peers and
// surface precise failures.
var (
	ErrWireVersion  = errors.New("raft: unsupported wire version")
	ErrMsgMalformed = errors.New("raft: malformed message")
	ErrMsgTooLarge  = errors.New("raft: message exceeds decode limit")
)

// EncodeMessage serializes a message deterministically (big-endian,
// fixed-width fields). The result is owned by the caller.
func EncodeMessage(m Message) []byte {
	b := []byte{WireVersion, byte(m.Type)}
	b = putWireU64(b, uint64(m.From))
	b = putWireU64(b, uint64(m.To))
	b = putWireU64(b, uint64(m.Term))
	b = putWireU64(b, uint64(m.LastLogIndex))
	b = putWireU64(b, uint64(m.LastLogTerm))
	if m.Reject {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	b = putWireU64(b, uint64(m.LogIndex))
	b = putWireU64(b, uint64(m.LogTerm))
	b = putWireU64(b, uint64(m.Commit))
	b = putWireU64(b, uint64(m.RejectHint))
	b = putWireU64(b, uint64(m.MatchIndex))
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Entries)))
	for _, e := range m.Entries {
		b = putWireU64(b, uint64(e.Term))
		b = putWireU64(b, uint64(e.Index))
		b = append(b, byte(e.Type))
		b = binary.BigEndian.AppendUint32(b, uint32(len(e.Data)))
		b = append(b, e.Data...)
	}
	b = putWireU64(b, uint64(m.Snapshot.Index))
	b = putWireU64(b, uint64(m.Snapshot.Term))
	cfg := EncodeConfig(m.Snapshot.Config)
	b = binary.BigEndian.AppendUint32(b, uint32(len(cfg)))
	b = append(b, cfg...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Snapshot.Data)))
	b = append(b, m.Snapshot.Data...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.ReadCtx)))
	b = append(b, m.ReadCtx...)
	return b
}

func putWireU64(b []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(b, v) }

// DecodeMessage parses a message under an explicit allocation cap (§8). limit
// bounds the encoded size; every length claim is validated against the
// remaining payload before allocation. Returned bytes are freshly allocated:
// the caller owns them and the input buffer may be reused.
func DecodeMessage(b []byte, limit int) (Message, error) {
	if limit > 0 && len(b) > limit {
		return Message{}, ErrMsgTooLarge
	}
	r := &wireReader{b: b}
	if r.byte() != WireVersion {
		return Message{}, ErrWireVersion
	}
	var m Message
	m.Type = MessageType(r.byte())
	if m.Type > MsgSnapshotResp {
		return Message{}, ErrMsgMalformed
	}
	m.From = NodeID(r.u64())
	m.To = NodeID(r.u64())
	m.Term = Term(r.u64())
	m.LastLogIndex = Index(r.u64())
	m.LastLogTerm = Term(r.u64())
	m.Reject = r.byte() == 1
	m.LogIndex = Index(r.u64())
	m.LogTerm = Term(r.u64())
	m.Commit = Index(r.u64())
	m.RejectHint = Index(r.u64())
	m.MatchIndex = Index(r.u64())
	n := r.u32()
	// Each entry costs at least 21 bytes; reject impossible counts before
	// allocating (§8 decode allocation cap).
	if r.err == nil && uint64(n)*21 > uint64(len(r.b)) {
		r.err = ErrMsgMalformed
	}
	if n > 0 && r.err == nil {
		m.Entries = make([]Entry, 0, n)
	}
	for i := uint32(0); i < n && r.err == nil; i++ {
		e := Entry{Term: Term(r.u64()), Index: Index(r.u64()), Type: EntryType(r.byte())}
		if e.Type > EntryConfChange {
			r.err = ErrMsgMalformed
			break
		}
		dl := r.u32()
		e.Data = append([]byte(nil), r.bytes(dl)...)
		m.Entries = append(m.Entries, e)
	}
	m.Snapshot.Index = Index(r.u64())
	m.Snapshot.Term = Term(r.u64())
	cl := r.u32()
	cfg, err := DecodeConfig(r.bytes(cl))
	if r.err != nil {
		return Message{}, r.err
	}
	if err != nil {
		return Message{}, ErrMsgMalformed
	}
	m.Snapshot.Config = cfg
	dl := r.u32()
	m.Snapshot.Data = append([]byte(nil), r.bytes(dl)...)
	rl := r.u32()
	m.ReadCtx = append([]byte(nil), r.bytes(rl)...)
	if r.err != nil {
		return Message{}, r.err
	}
	if len(r.b) != 0 {
		return Message{}, ErrMsgMalformed // trailing garbage
	}
	return m, nil
}

type wireReader struct {
	b   []byte
	err error
}

func (r *wireReader) u64() uint64 {
	if r.err != nil || len(r.b) < 8 {
		r.err = ErrMsgMalformed
		return 0
	}
	v := binary.BigEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}

func (r *wireReader) u32() uint32 {
	if r.err != nil || len(r.b) < 4 {
		r.err = ErrMsgMalformed
		return 0
	}
	v := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v
}

func (r *wireReader) byte() byte {
	if r.err != nil || len(r.b) < 1 {
		r.err = ErrMsgMalformed
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *wireReader) bytes(n uint32) []byte {
	if r.err != nil || uint32(len(r.b)) < n {
		r.err = ErrMsgMalformed
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}
