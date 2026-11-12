package main

import (
	"io"
	"os"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/wal"
)

// reconstructFSM rebuilds an in-memory state machine from the durable snapshot
// and the committed suffix through the persisted applied watermark (RFC §7).
// Used by the WAL-backed compositions on (re)start; HashiCorp restores its FSM
// inside NewRaft and does not need this.
func reconstructFSM(w *wal.WAL, f *stateFSM) error {
	st, err := w.Load()
	if err != nil {
		return err
	}
	if st.Snapshot.Index > 0 && len(st.Snapshot.Data) > 0 {
		if err := f.restore(st.Snapshot.Data, uint64(st.Snapshot.Index)); err != nil {
			return err
		}
	}
	if st.AppliedIndex > st.Snapshot.Index {
		ents, err := w.ReadEntries(st.Snapshot.Index+1, st.AppliedIndex+1)
		if err != nil {
			return err
		}
		for _, e := range ents {
			if e.Type == raft.EntryNormal && len(e.Data) > 0 {
				f.applyEntry(uint64(e.Index))
			} else {
				f.advance(uint64(e.Index))
			}
		}
	}
	return nil
}

func mkdir(path string) error { return os.MkdirAll(path, 0o755) }

func readAll(r io.Reader) ([]byte, error) { return io.ReadAll(r) }
