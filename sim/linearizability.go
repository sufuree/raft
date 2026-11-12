package sim

import "sort"

// LinOp is one operation in a client history checked against the sequential
// register specification (§11): writes set the register; reads must observe
// the value at their linearization point. Start/End are logical ticks;
// End < 0 marks a pending operation (invoked, no return observed). Optional
// writes model unknown outcomes: admission occurred but application is not
// known, so the operation may or may not have taken effect.
type LinOp struct {
	Write    bool
	Value    string
	Start    int
	End      int
	Optional bool
}

// CheckLinearizable runs the Wing-Gong search: an operation may be
// linearized next only if no remaining operation finished before it started
// (real-time order), and its effect must match the sequential specification.
// Pending operations may be linearized any time after their invoke. Unknown
// (optional) writes branch: included with effect, or discarded without it.
func CheckLinearizable(ops []LinOp) bool {
	sorted := append([]LinOp(nil), ops...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Start < sorted[j].Start })
	if len(sorted) > 63 {
		// The bitmask memo requires <= 63 operations; callers keep histories
		// bounded (the checker is exponential in the worst case).
		return false
	}
	memo := make(map[linKey]bool)
	var rec func(mask uint64, state string) bool
	rec = func(mask uint64, state string) bool {
		if mask == 0 {
			return true
		}
		k := linKey{mask, state}
		if v, ok := memo[k]; ok {
			return v
		}
		res := false
		// Determine the earliest finish among remaining operations: any
		// operation starting after it cannot go next.
		minEnd := int(^uint(0) >> 1)
		for i, o := range sorted {
			if mask&(1<<uint(i)) != 0 && o.End >= 0 && o.End < minEnd {
				minEnd = o.End
			}
		}
		for i, o := range sorted {
			bit := uint64(1) << uint(i)
			if mask&bit == 0 {
				continue
			}
			if o.Start > minEnd {
				continue // a remaining operation finished before this began
			}
			if o.Write {
				if rec(mask&^bit, o.Value) {
					res = true
					break
				}
				if o.Optional && rec(mask&^bit, state) {
					// Unknown outcome: model "never took effect".
					res = true
					break
				}
			} else if o.Value == state {
				if rec(mask&^bit, state) {
					res = true
					break
				}
			}
		}
		memo[k] = res
		return res
	}
	return rec((uint64(1)<<uint(len(sorted)))-1, "")
}

type linKey struct {
	mask  uint64
	state string
}

// HistoryOps converts a recorded simulator history into checker operations.
// Rejected proposals never admitted are excluded; failed reads impose no
// constraint; unknown-outcome writes become optional operations.
func HistoryOps(history []HistoryOp) []LinOp {
	type invoke struct {
		start int
		data  string
	}
	writes := make(map[uint64]invoke)
	var ops []LinOp
	for _, h := range history {
		switch h.Kind {
		case "propose":
			writes[h.ID] = invoke{start: h.Time, data: string(h.Data)}
		case "result":
			inv, ok := writes[h.ID]
			if !ok {
				continue
			}
			if h.Ok {
				ops = append(ops, LinOp{Write: true, Value: inv.data, Start: inv.start, End: h.Time})
			} else if h.Unknown {
				ops = append(ops, LinOp{Write: true, Value: inv.data, Start: inv.start, End: -1, Optional: true})
			}
			// Rejected/overload: not admitted; no operation occurred.
			delete(writes, h.ID)
		case "read":
			if h.Ok {
				ops = append(ops, LinOp{Write: false, Value: h.Value, Start: h.Time, End: h.End})
			}
		}
	}
	// Invoked writes with no recorded result stay pending (deterministic
	// order for reproducibility).
	ids := make([]uint64, 0, len(writes))
	for id := range writes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		inv := writes[id]
		ops = append(ops, LinOp{Write: true, Value: inv.data, Start: inv.start, End: -1})
	}
	return ops
}
