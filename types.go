// Package raft implements a deterministic Raft consensus core per RFC-0001.
//
// The core is a single-owner state machine: it performs no filesystem access,
// networking, wall-clock reads, or goroutine creation. Time arrives as logical
// ticks, randomness from a seeded stream, and I/O as completion events.
// Identical initial state and ordered inputs yield identical outputs.
package raft

import "fmt"

// Index is a log position. Index 0 is the empty prefix.
type Index uint64

// Term is a Raft term.
type Term uint64

// NodeID identifies a member. Zero means "no node".
type NodeID uint64

// Role is the protocol role of a node.
type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
	// Failed is terminal for this runtime instance (RFC-0001 §6.2).
	Failed
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Failed:
		return "failed"
	}
	return "unknown"
}

// EntryType distinguishes normal commands from configuration entries.
type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfChange
)

// Entry is one log record. Data is opaque to the library.
type Entry struct {
	Term  Term
	Index Index
	Type  EntryType
	Data  []byte
}

// Empty reports whether e is the zero sentinel.
func (e Entry) Empty() bool { return e.Index == 0 && e.Term == 0 }

// HardState is the atomic persistent term/vote/commit state (RFC-0001 §6.1).
type HardState struct {
	Term   Term
	Vote   NodeID
	Commit Index
}

// Empty reports whether hs carries no state.
func (hs HardState) Empty() bool { return hs.Term == 0 && hs.Vote == 0 && hs.Commit == 0 }

// NodeSet is an ordered, deduplicated set of member IDs.
type NodeSet []NodeID

// Contains reports whether id is in the set.
func (s NodeSet) Contains(id NodeID) bool {
	for _, n := range s {
		if n == id {
			return true
		}
	}
	return false
}

// Majority is the number of votes needed for a quorum of the set.
func (s NodeSet) Majority() int { return len(s)/2 + 1 }

// Configuration is the effective membership. When Joint is non-nil the cluster
// is in joint consensus and both voter sets must separately agree (§4, §6.1).
type Configuration struct {
	Voters   NodeSet
	Learners NodeSet
	Joint    *JointConfig
}

// JointConfig holds the outgoing voter set during a transition.
type JointConfig struct {
	OldVoters NodeSet
}

// InJoint reports whether the configuration is transitional.
func (c Configuration) InJoint() bool { return c.Joint != nil }

// VoterSets returns the voter sets that must each reach a majority.
func (c Configuration) VoterSets() []NodeSet {
	if c.InJoint() {
		return []NodeSet{c.Voters, c.Joint.OldVoters}
	}
	return []NodeSet{c.Voters}
}

// HasVoter reports whether id is a voter in any active voter set.
func (c Configuration) HasVoter(id NodeID) bool {
	for _, set := range c.VoterSets() {
		if set.Contains(id) {
			return true
		}
	}
	return false
}

// IsLearner reports whether id is a learner.
func (c Configuration) IsLearner(id NodeID) bool { return c.Learners.Contains(id) }

// QuorumActive reports whether every voter set has a majority of active nodes.
func (c Configuration) QuorumActive(active map[NodeID]bool) bool {
	for _, set := range c.VoterSets() {
		n := 0
		for _, id := range set {
			if active[id] {
				n++
			}
		}
		if n < set.Majority() {
			return false
		}
	}
	return true
}

// Clone deep-copies the configuration.
func (c Configuration) Clone() Configuration {
	out := Configuration{
		Voters:   append(NodeSet(nil), c.Voters...),
		Learners: append(NodeSet(nil), c.Learners...),
	}
	if c.Joint != nil {
		out.Joint = &JointConfig{OldVoters: append(NodeSet(nil), c.Joint.OldVoters...)}
	}
	return out
}

// SnapshotMeta describes a snapshot at an exact applied index and term.
type SnapshotMeta struct {
	Index  Index
	Term   Term
	Config Configuration
	// Data is an opaque reference/handle resolved by the storage adapter.
	Data []byte
}

// Empty reports whether the snapshot is the zero sentinel.
func (s SnapshotMeta) Empty() bool { return s.Index == 0 }

func (m Message) String() string {
	return fmt.Sprintf("%v %d->%d t%d", m.Type, m.From, m.To, m.Term)
}
