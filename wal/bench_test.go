package wal

import (
	"testing"

	raft "github.com/sufuree/raft"
)

func BenchmarkSave(b *testing.B) {
	dev := NewMemDevice()
	w, err := Open(dev, Options{ClusterID: 1, MemberID: 1})
	if err != nil {
		b.Fatal(err)
	}
	e := entry(1, 1, "payload")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Index = raft.Index(i + 1)
		c := e.Index
		if err := w.Save(raft.PersistBatch{Entries: []raft.Entry{e}, Commit: &c}); err != nil {
			b.Fatal(err)
		}
	}
}
