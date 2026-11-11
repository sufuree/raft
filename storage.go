package raft

import (
	"errors"
	"fmt"
	"sync"
)

// ErrCompacted is returned when requested history has been reclaimed.
var ErrCompacted = errors.New("raft: requested history compacted")

// ErrUnavailable is returned when requested entries are not yet durable.
var ErrUnavailable = errors.New("raft: entries not available")

// Storage is the durability contract (§6). Implementations apply each batch
// atomically: recovery exposes either the previous complete batch or the next
// complete batch, never a partially applied suffix replacement. Backend choice
// must not change protocol semantics.
type Storage interface {
	// Load returns the recovered durable state for core initialization.
	Load() (InitState, error)
	// Save atomically applies one persist batch. A successful return is the
	// durable receipt; a failed write/sync is terminal for the node (§6.2).
	Save(PersistBatch) error
	// ReadEntries returns durable entries in [lo, hi).
	ReadEntries(lo, hi Index) ([]Entry, error)
	// ReadSnapshot returns the authoritative published snapshot.
	ReadSnapshot() (SnapshotMeta, error)
	// MarkApplied records the FSM's durable applied watermark (§7).
	MarkApplied(Index) error
	// Close releases resources.
	Close() error
}

// MemStorage is an in-memory Storage for simulation and tests. Each Save is
// atomic. It is not crash-safe by itself; the simulator models crash/recovery
// by reverting to the last completed Save.
type MemStorage struct {
	mu            sync.Mutex
	hs            HardState
	snap          SnapshotMeta
	ents          []Entry // contiguous entries after snap
	applied       Index
	initialConfig Configuration
	closed        bool
	failNext      error // injected failure for tests
}

// NewMemStorage creates empty storage with an optional initial configuration.
func NewMemStorage() *MemStorage { return &MemStorage{} }

// SetInitialConfig records the bootstrap configuration durably so it survives
// restart before any configuration entry exists.
func (m *MemStorage) SetInitialConfig(c Configuration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initialConfig = c.Clone()
}

// FailNext injects a failure into the next Save (for failure-path tests).
func (m *MemStorage) FailNext(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNext = err
}

func (m *MemStorage) lastIndex() Index {
	if len(m.ents) > 0 {
		return m.ents[len(m.ents)-1].Index
	}
	return m.snap.Index
}

// Load implements Storage.
func (m *MemStorage) Load() (InitState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg := m.snap.Config.Clone()
	if len(cfg.Voters) == 0 {
		cfg = m.initialConfig.Clone()
	}
	return InitState{
		HardState:    m.hs,
		Snapshot:     m.snap,
		Entries:      append([]Entry(nil), m.ents...),
		Config:       cfg,
		AppliedIndex: m.applied,
		DurableIndex: m.lastIndex(),
	}, nil
}

// Save implements Storage atomically.
func (m *MemStorage) Save(b PersistBatch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("raft: storage closed")
	}
	if m.failNext != nil {
		err := m.failNext
		m.failNext = nil
		return err
	}
	if b.Snapshot != nil {
		m.snap = *b.Snapshot
		m.snap.Config = b.Snapshot.Config.Clone()
		m.ents = nil
	}
	if b.TruncateAt > 0 {
		var keep []Entry
		for _, e := range m.ents {
			if e.Index < b.TruncateAt {
				keep = append(keep, e)
			}
		}
		m.ents = keep
	}
	for _, e := range b.Entries {
		m.appendEntry(e)
	}
	if b.HardState != nil {
		// Term/vote are atomic; commit watermark only advances.
		if b.HardState.Term != 0 || b.HardState.Vote != 0 {
			m.hs.Term = b.HardState.Term
			m.hs.Vote = b.HardState.Vote
		}
		if b.HardState.Commit > m.hs.Commit {
			m.hs.Commit = b.HardState.Commit
		}
	}
	if b.Commit != nil && *b.Commit > m.hs.Commit {
		m.hs.Commit = *b.Commit
	}
	return nil
}

func (m *MemStorage) appendEntry(e Entry) {
	if e.Index <= m.snap.Index {
		return
	}
	if len(m.ents) > 0 {
		last := m.ents[len(m.ents)-1].Index
		if e.Index <= last {
			// Idempotent replace by index.
			off := e.Index - m.snap.Index - 1
			if off < Index(len(m.ents)) {
				m.ents[off] = e
			}
			return
		}
		if e.Index != last+1 {
			panic(fmt.Sprintf("memstorage: gap appending %d after %d", e.Index, last))
		}
	} else if e.Index != m.snap.Index+1 {
		panic(fmt.Sprintf("memstorage: gap appending %d after snapshot %d", e.Index, m.snap.Index))
	}
	m.ents = append(m.ents, e)
}

// ReadEntries implements Storage.
func (m *MemStorage) ReadEntries(lo, hi Index) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lo <= m.snap.Index {
		return nil, ErrCompacted
	}
	if hi > m.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	if lo >= hi {
		return nil, nil
	}
	out := make([]Entry, 0, hi-lo)
	for _, e := range m.ents {
		if e.Index >= lo && e.Index < hi {
			out = append(out, e)
		}
	}
	return out, nil
}

// ReadSnapshot implements Storage.
func (m *MemStorage) ReadSnapshot() (SnapshotMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap, nil
}

// MarkApplied implements Storage.
func (m *MemStorage) MarkApplied(idx Index) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx > m.applied {
		m.applied = idx
	}
	return nil
}

// Close implements Storage.
func (m *MemStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// DurableState captures the durable state for crash simulation.
type DurableState struct {
	hs            HardState
	snap          SnapshotMeta
	ents          []Entry
	applied       Index
	initialConfig Configuration
}

// Capture returns a copy of the current durable state.
func (m *MemStorage) Capture() DurableState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return DurableState{
		hs:            m.hs,
		snap:          m.snap,
		ents:          append([]Entry(nil), m.ents...),
		applied:       m.applied,
		initialConfig: m.initialConfig.Clone(),
	}
}

// Restore reverts storage to a captured durable state (crash model).
func (m *MemStorage) Restore(s DurableState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = s.hs
	m.snap = s.snap
	m.ents = append([]Entry(nil), s.ents...)
	m.applied = s.applied
	m.initialConfig = s.initialConfig.Clone()
	m.failNext = nil
}
