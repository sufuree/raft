package raft

import "encoding/binary"

// confEncode serializes a configuration into a conf-change entry payload.
func confEncode(c Configuration) []byte {
	var b []byte
	putSet := func(s NodeSet) {
		b = binary.AppendUvarint(b, uint64(len(s)))
		for _, id := range s {
			b = binary.AppendUvarint(b, uint64(id))
		}
	}
	putSet(c.Voters)
	putSet(c.Learners)
	if c.InJoint() {
		b = append(b, 1)
		putSet(c.Joint.OldVoters)
	} else {
		b = append(b, 0)
	}
	return b
}

// confDecode parses a conf-change entry payload.
func confDecode(data []byte) (Configuration, error) {
	var c Configuration
	getSet := func() (NodeSet, error) {
		n, r := binary.Uvarint(data)
		if r <= 0 {
			return nil, errConf
		}
		data = data[r:]
		// Decode allocation cap (§8): every ID costs at least one byte, so a
		// count larger than the remaining payload is malformed. Validate before
		// allocating; never trust an attacker-controlled count.
		if n > uint64(len(data)) {
			return nil, errConf
		}
		s := make(NodeSet, 0, n)
		for i := uint64(0); i < n; i++ {
			id, r := binary.Uvarint(data)
			if r <= 0 {
				return nil, errConf
			}
			data = data[r:]
			s = append(s, NodeID(id))
		}
		return s, nil
	}
	var err error
	if c.Voters, err = getSet(); err != nil {
		return c, err
	}
	if c.Learners, err = getSet(); err != nil {
		return c, err
	}
	if len(data) < 1 {
		return c, errConf
	}
	joint := data[0]
	data = data[1:]
	if joint == 1 {
		old, err := getSet()
		if err != nil {
			return c, err
		}
		c.Joint = &JointConfig{OldVoters: old}
	}
	return c, nil
}

type confError struct{}

func (confError) Error() string { return "raft: malformed configuration entry" }

var errConf = confError{}

// confRecord is a configuration entry currently present in the log.
type confRecord struct {
	index Index
	cfg   Configuration
}

// effectiveConfig returns the append-time effective configuration (§6.1): the
// latest configuration entry in the log, committed or not, else the snapshot's.
func (c *Core) effectiveConfig() Configuration {
	if n := len(c.confStack); n > 0 {
		return c.confStack[n-1].cfg
	}
	return c.confBase
}

// applyConfigDelta updates configuration tracking after a log append/truncate.
func (c *Core) applyConfigDelta(trunc Index, appended []Entry) {
	if trunc > 0 {
		var keep []confRecord
		for _, r := range c.confStack {
			if r.index < trunc {
				keep = append(keep, r)
			}
		}
		c.confStack = keep
	}
	for _, e := range appended {
		if e.Type != EntryConfChange {
			continue
		}
		cfg, err := confDecode(e.Data)
		if err != nil {
			// A malformed conf entry is a deterministic application error; the
			// core treats it as a failed entry rather than panicking.
			continue
		}
		c.confStack = append(c.confStack, confRecord{index: e.Index, cfg: cfg})
	}
	c.config = c.effectiveConfig()
	c.reconcileProgress()
}

// reconcileProgress adds/removes leader replication targets to match the
// effective configuration after a membership change (§4.4).
func (c *Core) reconcileProgress() {
	if c.role != Leader {
		return
	}
	next := c.log.lastIndex() + 1
	wanted := make(map[NodeID]bool)
	for _, set := range c.config.VoterSets() {
		for _, id := range set {
			if id != c.id {
				wanted[id] = true
			}
		}
	}
	for _, id := range c.config.Learners {
		wanted[id] = true
	}
	// Retain removed peers until they have matched the latest configuration
	// entry; otherwise they are stranded in a joint view that still lists
	// them as voters (§4.4). Peers named by uncommitted transitions are
	// seeded at election (becomeLeader).
	if e, ok := c.log.scanConf(c.log.lastIndex()); ok {
		for id, p := range c.progress {
			if !wanted[id] && p.Match < e.Index {
				wanted[id] = true
			}
		}
	}
	for id := range wanted {
		if _, ok := c.progress[id]; !ok {
			c.progress[id] = &Progress{Next: next, Match: 0, RecentActive: true}
		}
	}
	for id := range c.progress {
		if !wanted[id] {
			delete(c.progress, id)
		}
	}
	// A leader removed from the configuration steps down once the final
	// configuration activates; otherwise its heartbeats would suppress
	// elections among the remaining voters indefinitely (§4.4).
	if c.role == Leader && !c.config.HasVoter(c.id) {
		c.becomeFollower(c.term, 0)
		c.emitStatus("removed from configuration")
	}
}

// resetConfigToSnapshot resets configuration tracking on snapshot install.
func (c *Core) resetConfigToSnapshot(snap SnapshotMeta) {
	c.confBase = snap.Config.Clone()
	c.confStack = nil
	c.config = c.effectiveConfig()
}

// EncodeConfig serializes a configuration (exported for storage adapters).
func EncodeConfig(c Configuration) []byte { return confEncode(c) }

// DecodeConfig parses a configuration payload (exported for storage adapters).
func DecodeConfig(b []byte) (Configuration, error) { return confDecode(b) }
