package wal

import (
	"encoding/binary"
	"errors"

	raft "github.com/sufuree/raft"
)

var errShort = errors.New("wal: short batch payload")

func putU64(b []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(b, v) }
func putU32(b []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(b, v) }

func encodeBatch(batch raft.PersistBatch) []byte {
	var b []byte
	if batch.HardState != nil {
		b = append(b, 1)
		b = putU64(b, uint64(batch.HardState.Term))
		b = putU64(b, uint64(batch.HardState.Vote))
		b = putU64(b, uint64(batch.HardState.Commit))
	} else {
		b = append(b, 0)
	}
	b = putU64(b, uint64(batch.TruncateAt))
	b = putU32(b, uint32(len(batch.Entries)))
	for _, e := range batch.Entries {
		b = putU64(b, uint64(e.Term))
		b = putU64(b, uint64(e.Index))
		b = append(b, byte(e.Type))
		b = putU32(b, uint32(len(e.Data)))
		b = append(b, e.Data...)
	}
	if batch.Commit != nil {
		b = append(b, 1)
		b = putU64(b, uint64(*batch.Commit))
	} else {
		b = append(b, 0)
	}
	if batch.Snapshot != nil {
		b = append(b, 1)
		b = putU64(b, uint64(batch.Snapshot.Index))
		b = putU64(b, uint64(batch.Snapshot.Term))
		cfg := raft.EncodeConfig(batch.Snapshot.Config)
		b = putU32(b, uint32(len(cfg)))
		b = append(b, cfg...)
		b = putU32(b, uint32(len(batch.Snapshot.Data)))
		b = append(b, batch.Snapshot.Data...)
	} else {
		b = append(b, 0)
	}
	return b
}

type reader struct {
	b   []byte
	err error
}

func (r *reader) u64() uint64 {
	if r.err != nil || len(r.b) < 8 {
		r.err = errShort
		return 0
	}
	v := binary.BigEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}
func (r *reader) u32() uint32 {
	if r.err != nil || len(r.b) < 4 {
		r.err = errShort
		return 0
	}
	v := binary.BigEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v
}
func (r *reader) byte() byte {
	if r.err != nil || len(r.b) < 1 {
		r.err = errShort
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}
func (r *reader) bytes(n uint32) []byte {
	if r.err != nil || uint32(len(r.b)) < n {
		r.err = errShort
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

func decodeBatch(payload []byte) (raft.PersistBatch, error) {
	r := &reader{b: payload}
	var batch raft.PersistBatch
	if r.byte() == 1 {
		hs := raft.HardState{
			Term:   raft.Term(r.u64()),
			Vote:   raft.NodeID(r.u64()),
			Commit: raft.Index(r.u64()),
		}
		batch.HardState = &hs
	}
	batch.TruncateAt = raft.Index(r.u64())
	n := r.u32()
	for i := uint32(0); i < n; i++ {
		e := raft.Entry{
			Term:  raft.Term(r.u64()),
			Index: raft.Index(r.u64()),
			Type:  raft.EntryType(r.byte()),
		}
		dl := r.u32()
		e.Data = append([]byte(nil), r.bytes(dl)...)
		batch.Entries = append(batch.Entries, e)
	}
	if r.byte() == 1 {
		c := raft.Index(r.u64())
		batch.Commit = &c
	}
	if r.byte() == 1 {
		snap := raft.SnapshotMeta{
			Index: raft.Index(r.u64()),
			Term:  raft.Term(r.u64()),
		}
		cl := r.u32()
		cfg, err := raft.DecodeConfig(r.bytes(cl))
		if err != nil {
			return batch, err
		}
		snap.Config = cfg
		dl := r.u32()
		snap.Data = append([]byte(nil), r.bytes(dl)...)
		batch.Snapshot = &snap
	}
	if r.err != nil {
		return batch, r.err
	}
	return batch, nil
}
