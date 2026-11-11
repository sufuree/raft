package raft

// Progress tracks a follower's replication state from the leader's view.
type Progress struct {
	Match Index
	Next  Index
	// RecentActive is set on any successful contact; used by quorum checks.
	RecentActive bool
	// PendingSnapshot is the index of an in-flight snapshot, if any.
	PendingSnapshot Index
	// StreamOffset is the next byte offset to send for a streamed snapshot
	// (§6.4); reset when the transfer restarts.
	StreamOffset int
	// StreamElapsed ticks since the last stream progress; a stalled transfer
	// restarts after the election timeout (§6.4).
	StreamElapsed int
	// AppendInflight marks one unacknowledged AppendEntries per peer (§8
	// per-peer in-flight budget): bounded pipelining, no redundant probes.
	AppendInflight bool
	// InflightElapsed ticks since the append was sent; a lost append is
	// retransmitted after the election timeout.
	InflightElapsed int
	// ProbeElapsed ticks since the last commit probe; probes are cooldown-
	// gated so a lagging follower is not flooded (§8).
	ProbeElapsed int
	// Inflight bytes/messages for resource budgets (§8), tracked by the driver.
	InflightBytes int
	InflightMsgs  int
}

// quorumIndex computes the highest index replicated by a majority of every
// voter set. Self is counted only when selfDurable >= candidate (§6.2).
func quorumIndex(sets []NodeSet, match map[NodeID]Index, self NodeID, selfMatch Index) Index {
	// Collect candidate indexes from all voters.
	var all []Index
	for _, set := range sets {
		for _, id := range set {
			if id == self {
				all = append(all, selfMatch)
			} else {
				all = append(all, match[id])
			}
		}
	}
	// The quorum index is the largest N such that every voter set has a
	// majority with match >= N. Compute per-set and take the min.
	var result Index
	first := true
	for _, set := range sets {
		var vals []Index
		for _, id := range set {
			if id == self {
				vals = append(vals, selfMatch)
			} else {
				vals = append(vals, match[id])
			}
		}
		// Sort descending; the majority-th value is the set's quorum index.
		sortDesc(vals)
		q := vals[set.Majority()-1]
		if first || q < result {
			result = q
			first = false
		}
	}
	_ = all
	return result
}

func sortDesc(v []Index) {
	// insertion sort; voter sets are small
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] > v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}
