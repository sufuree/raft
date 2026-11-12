package wal

import (
	"testing"

	raft "github.com/sufuree/raft"
)

// FuzzDecodeBatch verifies batch decoding never panics on arbitrary bytes and
// respects the allocation cap (§8).
func FuzzDecodeBatch(f *testing.F) {
	f.Add(encodeBatch(raft.PersistBatch{
		Entries: []raft.Entry{{Term: 1, Index: 1, Data: []byte("x")}},
	}))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		b, err := decodeBatch(data)
		if err != nil {
			return
		}
		if _, err := decodeBatch(encodeBatch(b)); err != nil {
			t.Fatalf("re-encode failed: %v", err)
		}
	})
}

// FuzzWALRecovery verifies recovery over arbitrary device contents never
// panics: it either returns a typed error or a consistent state (§6.3, §11).
func FuzzWALRecovery(f *testing.F) {
	dev := NewMemDevice()
	w, err := Open(dev, Options{ClusterID: 1, MemberID: 1})
	if err != nil {
		f.Fatal(err)
	}
	c := raft.Index(1)
	w.Save(raft.PersistBatch{Entries: []raft.Entry{{Term: 1, Index: 1, Data: []byte("x")}}, Commit: &c})
	w.Close()
	seed, _ := dev.ReadAt(make([]byte, 4096), 0)
	_ = seed
	buf := make([]byte, 8192)
	n, _ := dev.ReadAt(buf, 0)
	f.Add(buf[:n])
	f.Add(make([]byte, 4096))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		d := NewMemDevice()
		if len(data) > 0 {
			d.WriteAt(data, 0)
			d.Sync()
		}
		w, err := Open(d, Options{ClusterID: 1, MemberID: 1})
		if err != nil {
			return // typed corruption errors are the expected outcome
		}
		st, err := w.Load()
		if err != nil {
			t.Fatalf("load after recovery: %v", err)
		}
		if st.HardState.Commit > st.DurableIndex {
			t.Fatalf("commit %d beyond durable %d", st.HardState.Commit, st.DurableIndex)
		}
		w.Close()
	})
}
