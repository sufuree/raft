package raft

import "fmt"

// raftLog is the core's in-memory view of the accepted log. It holds a dummy
// entry at the snapshot position so term lookups at the boundary work. The
// durable prefix is tracked separately by the core (durableIndex); this log may
// contain accepted-but-not-yet-durable entries.
type raftLog struct {
	snapshot SnapshotMeta
	// ents[0] is the dummy {Index: snapshot.Index, Term: snapshot.Term}.
	ents []Entry
}

func newLog(snap SnapshotMeta) *raftLog {
	l := &raftLog{snapshot: snap}
	l.ents = []Entry{{Index: snap.Index, Term: snap.Term}}
	return l
}

func (l *raftLog) lastIndex() Index  { return l.ents[len(l.ents)-1].Index }
func (l *raftLog) lastTerm() Term    { return l.ents[len(l.ents)-1].Term }
func (l *raftLog) firstIndex() Index { return l.snapshot.Index + 1 }

// term returns the term at index i, or 0 if unavailable.
func (l *raftLog) term(i Index) Term {
	if i < l.snapshot.Index || i > l.lastIndex() {
		return 0
	}
	return l.ents[i-l.snapshot.Index].Term
}

// matchTerm reports whether index i holds term t.
func (l *raftLog) matchTerm(i Index, t Term) bool { return l.term(i) == t }

// upToDate implements the §4 voting rule: a candidate log is at least as
// up-to-date as this log if its last term is greater, or equal with a longer log.
func (l *raftLog) upToDate(lastIndex Index, lastTerm Term) bool {
	if lastTerm != l.lastTerm() {
		return lastTerm > l.lastTerm()
	}
	return lastIndex >= l.lastIndex()
}

// append adds entries, resolving conflicts. It returns the truncation point
// (0 if none) and the entries actually appended, describing the persist delta.
func (l *raftLog) append(es []Entry) (Index, []Entry) {
	if len(es) == 0 {
		return 0, nil
	}
	off := 0
	for off < len(es) && es[off].Index <= l.lastIndex() && l.term(es[off].Index) == es[off].Term {
		off++
	}
	if off == len(es) {
		return 0, nil
	}
	rest := es[off:]
	var trunc Index
	if rest[0].Index <= l.lastIndex() {
		trunc = rest[0].Index
		l.truncateFrom(trunc)
	}
	if rest[0].Index != l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: log gap: have last %d, appending %d", l.lastIndex(), rest[0].Index))
	}
	l.ents = append(l.ents, rest...)
	return trunc, rest
}

// truncateFrom discards entries with index >= i.
func (l *raftLog) truncateFrom(i Index) {
	if i <= l.snapshot.Index {
		panic("raft: cannot truncate into snapshot")
	}
	if i > l.lastIndex() {
		return
	}
	l.ents = l.ents[:i-l.snapshot.Index]
}

// entries returns entries in [lo, hi), bounded by available range.
func (l *raftLog) entries(lo, hi Index) []Entry {
	if lo < l.firstIndex() {
		lo = l.firstIndex()
	}
	if hi > l.lastIndex()+1 {
		hi = l.lastIndex() + 1
	}
	if lo >= hi {
		return nil
	}
	return l.ents[lo-l.snapshot.Index : hi-l.snapshot.Index]
}

// entry returns the entry at i.
func (l *raftLog) entry(i Index) (Entry, bool) {
	if i < l.firstIndex() || i > l.lastIndex() {
		return Entry{}, false
	}
	return l.ents[i-l.snapshot.Index], true
}

// applySnapshot resets the log to a snapshot position.
func (l *raftLog) applySnapshot(snap SnapshotMeta) {
	l.snapshot = snap
	l.ents = []Entry{{Index: snap.Index, Term: snap.Term}}
}

// compact drops entries at or below snap.Index, retaining the suffix above it.
func (l *raftLog) compact(snap SnapshotMeta) {
	var keep []Entry
	for _, e := range l.ents {
		if e.Index > snap.Index {
			keep = append(keep, e)
		}
	}
	l.snapshot = snap
	l.ents = append([]Entry{{Index: snap.Index, Term: snap.Term}}, keep...)
}

// scanConf returns the latest configuration entry at or below hi, if any.
func (l *raftLog) scanConf(hi Index) (Entry, bool) {
	for i := l.lastIndex(); i >= l.firstIndex(); i-- {
		if i > hi {
			continue
		}
		e := l.ents[i-l.snapshot.Index]
		if e.Type == EntryConfChange {
			return e, true
		}
	}
	return Entry{}, false
}
