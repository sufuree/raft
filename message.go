package raft

// MessageType identifies a protocol message.
type MessageType uint8

const (
	MsgPreVote MessageType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgAppend
	MsgAppendResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnapshot
	MsgSnapshotResp
)

func (t MessageType) String() string {
	switch t {
	case MsgPreVote:
		return "PreVote"
	case MsgPreVoteResp:
		return "PreVoteResp"
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgAppend:
		return "Append"
	case MsgAppendResp:
		return "AppendResp"
	case MsgHeartbeat:
		return "Heartbeat"
	case MsgHeartbeatResp:
		return "HeartbeatResp"
	case MsgSnapshot:
		return "Snapshot"
	case MsgSnapshotResp:
		return "SnapshotResp"
	}
	return "Unknown"
}

// Message is a protocol exchange between members. Fields are used per type.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID
	Term Term

	// Vote / PreVote: candidate's last log position for the up-to-date rule.
	LastLogIndex Index
	LastLogTerm  Term
	// Reject is set on negative vote/append responses.
	Reject bool

	// Append: prev log position and entries.
	LogIndex Index
	LogTerm  Term
	Entries  []Entry
	// Commit is the leader's commit index (Append/Heartbeat).
	Commit Index

	// AppendResp: follower's matching hint on rejection, or matched index.
	RejectHint Index
	MatchIndex Index

	// Snapshot metadata (MsgSnapshot).
	Snapshot SnapshotMeta

	// ReadCtx carries a unique read-barrier confirmation context (§4.3).
	ReadCtx []byte
}

// Clone deep-copies a message so payload ownership is explicit (§5).
func (m Message) Clone() Message {
	out := m
	if m.Entries != nil {
		out.Entries = make([]Entry, len(m.Entries))
		for i, e := range m.Entries {
			out.Entries[i] = e
			out.Entries[i].Data = append([]byte(nil), e.Data...)
		}
	}
	out.ReadCtx = append([]byte(nil), m.ReadCtx...)
	out.Snapshot.Config = m.Snapshot.Config.Clone()
	out.Snapshot.Data = append([]byte(nil), m.Snapshot.Data...)
	return out
}
