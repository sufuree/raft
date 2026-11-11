// Package sim is a deterministic Raft simulator (RFC-0001 §11): virtual time,
// seeded elections, scheduled network delivery, and a crashable storage model.
// It doubles as a deterministic reference driver for the core.
package sim

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sort"

	raft "github.com/sufuree/raft"
)

// FSM is a simple deterministic state machine recording commands by index.
type FSM struct {
	Applied  raft.Index
	Commands map[raft.Index][]byte
	snap     []byte
}

func newFSM() *FSM { return &FSM{Commands: make(map[raft.Index][]byte)} }

func (f *FSM) apply(entries []raft.Entry) error {
	for _, e := range entries {
		if e.Type == raft.EntryNormal {
			f.Commands[e.Index] = append([]byte(nil), e.Data...)
		}
		if e.Index > f.Applied {
			f.Applied = e.Index
		}
	}
	return nil
}

func (f *FSM) snapshot() []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(f.Applied)
	enc.Encode(f.Commands)
	return buf.Bytes()
}

func (f *FSM) restore(snap raft.SnapshotMeta, data []byte) {
	f.Commands = make(map[raft.Index][]byte)
	f.Applied = snap.Index
	f.snap = append([]byte(nil), data...)
	if len(data) > 0 {
		dec := gob.NewDecoder(bytes.NewReader(data))
		dec.Decode(&f.Applied)
		dec.Decode(&f.Commands)
	}
}

// Node is one simulated member: core + storage + FSM.
type Node struct {
	ID    raft.NodeID
	Core  *raft.Core
	Store *raft.MemStorage
	FSM   *FSM
	Alive bool
	Epoch uint64

	held    map[uint64][]raft.Message
	durable raft.DurableState
}

// Network models delay, drop, duplication, reordering, and partitions.
type Network struct {
	rng       *rand.Rand
	queue     []delivery
	MinDelay  int
	MaxDelay  int
	DropRate  float64
	DupRate   float64
	partition map[raft.NodeID]int // node -> partition group
}

type delivery struct {
	at  int
	msg raft.Message
}

func newNetwork(seed int64) *Network {
	return &Network{
		rng:       rand.New(rand.NewSource(seed)),
		MinDelay:  1,
		MaxDelay:  3,
		partition: make(map[raft.NodeID]int),
	}
}

func (n *Network) send(now int, m raft.Message) {
	if n.DropRate > 0 && n.rng.Float64() < n.DropRate {
		return
	}
	delay := n.MinDelay
	if n.MaxDelay > n.MinDelay {
		delay += n.rng.Intn(n.MaxDelay - n.MinDelay + 1)
	}
	n.queue = append(n.queue, delivery{at: now + delay, msg: m})
	if n.DupRate > 0 && n.rng.Float64() < n.DupRate {
		n.queue = append(n.queue, delivery{at: now + delay + 1, msg: m.Clone()})
	}
}

func (n *Network) reachable(from, to raft.NodeID) bool {
	gf, okf := n.partition[from]
	gt, okt := n.partition[to]
	if okf && okt && gf != gt {
		return false
	}
	return true
}

// Simulator drives a cluster deterministically.
type Simulator struct {
	Nodes   map[raft.NodeID]*Node
	Net     *Network
	Time    int
	rng     *rand.Rand
	seed    int64
	nextID  uint64
	leaders map[raft.Term]raft.NodeID // election-safety witness
	history []HistoryOp
}

// HistoryOp records a client-visible operation for linearizability checking.
type HistoryOp struct {
	Kind  string // "propose", "read", "result"
	Node  raft.NodeID
	ID    uint64
	Index raft.Index
	Data  []byte
	Ok    bool
}

// Config configures a simulator run.
type Config struct {
	Seed          int64
	Nodes         int
	ElectionTick  int
	HeartbeatTick int
	PreVote       bool
	CheckQuorum   bool
	Voters        []raft.NodeID // initial voter set; defaults to all nodes
}

// New creates a simulator with an initial configuration.
func New(cfg Config) *Simulator {
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = 1
	}
	s := &Simulator{
		Nodes:   make(map[raft.NodeID]*Node),
		Net:     newNetwork(cfg.Seed + 1),
		rng:     rand.New(rand.NewSource(cfg.Seed + 2)),
		seed:    cfg.Seed,
		leaders: make(map[raft.Term]raft.NodeID),
	}
	voters := cfg.Voters
	if voters == nil {
		for i := 1; i <= cfg.Nodes; i++ {
			voters = append(voters, raft.NodeID(i))
		}
	}
	initCfg := raft.Configuration{Voters: voters}
	for i := 1; i <= cfg.Nodes; i++ {
		id := raft.NodeID(i)
		store := raft.NewMemStorage()
		store.SetInitialConfig(initCfg)
		n := &Node{
			ID:    id,
			Store: store,
			FSM:   newFSM(),
			Alive: true,
			Epoch: 1,
			held:  make(map[uint64][]raft.Message),
		}
		init, _ := store.Load()
		init.Epoch = n.Epoch
		n.Core = raft.NewCore(raft.Config{
			ID:            id,
			ElectionTick:  cfg.ElectionTick,
			HeartbeatTick: cfg.HeartbeatTick,
			PreVote:       cfg.PreVote,
			CheckQuorum:   cfg.CheckQuorum,
			Seed:          cfg.Seed + int64(i)*7919,
		}, init)
		n.durable = store.Capture()
		s.Nodes[id] = n
	}
	return s
}

// Propose submits a command to a node, returning a proposal ID.
func (s *Simulator) Propose(id raft.NodeID, data []byte) uint64 {
	s.nextID++
	pid := s.nextID
	n := s.Nodes[id]
	if n == nil || !n.Alive {
		return pid
	}
	s.history = append(s.history, HistoryOp{Kind: "propose", Node: id, ID: pid, Data: data})
	out := n.Core.Step(raft.Event{Kind: raft.EvPropose, WorkID: pid, Data: data})
	s.drain(n, out)
	return pid
}

// Read issues a linearizable read barrier on a node.
func (s *Simulator) Read(id raft.NodeID, ctx []byte) {
	n := s.Nodes[id]
	if n == nil || !n.Alive {
		return
	}
	out := n.Core.Step(raft.Event{Kind: raft.EvReadBarrier, ReadCtx: ctx})
	s.drain(n, out)
}

// ConfChange proposes a membership transition on the leader.
func (s *Simulator) ConfChange(id raft.NodeID, target raft.Configuration) uint64 {
	s.nextID++
	pid := s.nextID
	n := s.Nodes[id]
	if n == nil || !n.Alive {
		return pid
	}
	out := n.Core.Step(raft.Event{Kind: raft.EvProposeConfChange, WorkID: pid, Config: target})
	s.drain(n, out)
	return pid
}

// Partition assigns nodes to groups; cross-group traffic is blocked.
func (s *Simulator) Partition(groups map[raft.NodeID]int) {
	s.Net.partition = groups
}

// Heal removes all partitions.
func (s *Simulator) Heal() { s.Net.partition = make(map[raft.NodeID]int) }

// Crash stops a node and reverts storage to its last durable state.
func (s *Simulator) Crash(id raft.NodeID) {
	n := s.Nodes[id]
	if n == nil {
		return
	}
	n.Alive = false
	n.Store.Restore(n.durable)
	n.held = make(map[uint64][]raft.Message)
}

// Restart brings a crashed node back from durable state with a fresh epoch.
func (s *Simulator) Restart(id raft.NodeID) {
	n := s.Nodes[id]
	if n == nil || n.Alive {
		return
	}
	n.Epoch++
	init, _ := n.Store.Load()
	init.Epoch = n.Epoch
	cfg := n.Core.Config()
	_ = cfg
	n.Core = raft.NewCore(raft.Config{
		ID:            id,
		ElectionTick:  10,
		HeartbeatTick: 1,
		PreVote:       true,
		CheckQuorum:   true,
		Seed:          s.seed + int64(id)*7919 + int64(n.Epoch)*104729,
	}, init)
	n.FSM = newFSM()
	// Reconstruct FSM from durable snapshot + committed replay.
	s.rebuildFSM(n)
	n.Alive = true
	n.held = make(map[uint64][]raft.Message)
}

func (s *Simulator) rebuildFSM(n *Node) {
	snap, _ := n.Store.ReadSnapshot()
	if !snap.Empty() {
		n.FSM.restore(snap, snap.Data)
	}
	init, _ := n.Store.Load()
	// Replay committed entries above the snapshot.
	lo := snap.Index + 1
	hi := init.HardState.Commit + 1
	if hi > lo {
		entries, err := n.Store.ReadEntries(lo, hi)
		if err == nil {
			n.FSM.apply(entries)
		}
	}
}

// Run advances virtual time by ticks, delivering messages and checking safety.
func (s *Simulator) Run(ticks int) error {
	for t := 0; t < ticks; t++ {
		s.Time++
		s.deliver()
		for _, id := range s.sortedIDs() {
			n := s.Nodes[id]
			if !n.Alive {
				continue
			}
			out := n.Core.Step(raft.Event{Kind: raft.EvTick})
			s.drain(n, out)
		}
		if err := s.checkSafety(); err != nil {
			return fmt.Errorf("tick %d: %w", s.Time, err)
		}
	}
	return nil
}

func (s *Simulator) sortedIDs() []raft.NodeID {
	ids := make([]raft.NodeID, 0, len(s.Nodes))
	for id := range s.Nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (s *Simulator) deliver() {
	due := s.Net.queue
	s.Net.queue = nil
	var remaining []delivery
	for _, d := range due {
		if d.at > s.Time {
			remaining = append(remaining, d)
			continue
		}
		n := s.Nodes[d.msg.To]
		if n == nil || !n.Alive {
			continue
		}
		if !s.Net.reachable(d.msg.From, d.msg.To) {
			continue
		}
		out := n.Core.Step(raft.Event{Kind: raft.EvReceive, Message: d.msg})
		s.drain(n, out)
	}
	// Messages sent while delivering land in the fresh queue; keep future
	// deliveries scheduled after them.
	s.Net.queue = append(s.Net.queue, remaining...)
}

// drain applies a node's outputs: work to storage/FSM, messages to the network.
func (s *Simulator) drain(n *Node, out raft.Outputs) {
	// Route messages first so durability prerequisites are registered before
	// the work they depend on is applied (and releases them).
	for _, om := range out.Messages {
		if om.After == 0 {
			s.Net.send(s.Time, om.Message)
		} else {
			n.held[om.After] = append(n.held[om.After], om.Message)
		}
	}
	for _, w := range out.Work {
		s.applyWork(n, w)
	}
	for _, r := range out.Reads {
		s.history = append(s.history, HistoryOp{Kind: "read", Node: n.ID, Index: r.Index, Ok: r.Err == nil})
	}
	for _, p := range out.Proposals {
		s.history = append(s.history, HistoryOp{Kind: "result", Node: n.ID, ID: p.ID, Index: p.Index, Ok: p.Outcome == raft.OutcomeApplied})
	}
	if out.Status != nil && out.Status.Role == raft.Leader {
		s.leaders[out.Status.Term] = n.ID
	}
}

func (s *Simulator) applyWork(n *Node, w raft.Work) {
	switch w.Kind {
	case raft.WorkPersist:
		err := n.Store.Save(w.Persist)
		if err != nil {
			s.drain(n, n.Core.Step(raft.Event{Kind: raft.EvStorageFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err}))
			return
		}
		n.durable = n.Store.Capture()
		s.releaseHeld(n, w.ID)
		s.drain(n, n.Core.Step(raft.Event{Kind: raft.EvPersisted, Epoch: w.Epoch, WorkID: w.ID}))
	case raft.WorkApply:
		err := n.FSM.apply(w.Apply)
		if err != nil {
			s.drain(n, n.Core.Step(raft.Event{Kind: raft.EvApplyFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err}))
			return
		}
		var last raft.Index
		if len(w.Apply) > 0 {
			last = w.Apply[len(w.Apply)-1].Index
		}
		n.Store.MarkApplied(last)
		n.durable = n.Store.Capture()
		s.drain(n, n.Core.Step(raft.Event{Kind: raft.EvApplied, Epoch: w.Epoch, WorkID: w.ID}))
	case raft.WorkSnapshotInstall:
		n.FSM.restore(w.Snapshot, w.Snapshot.Data)
		n.Store.Save(raft.PersistBatch{Snapshot: &w.Snapshot})
		n.Store.MarkApplied(w.Snapshot.Index)
		n.durable = n.Store.Capture()
		s.drain(n, n.Core.Step(raft.Event{Kind: raft.EvSnapshotInstalled, Epoch: w.Epoch, WorkID: w.ID}))
	case raft.WorkSnapshotCreate:
		meta := w.Snapshot
		meta.Data = n.FSM.snapshot()
		s.drain(n, n.Core.Step(raft.Event{Kind: raft.EvSnapshotCreated, Epoch: w.Epoch, WorkID: w.ID, Snapshot: meta}))
	}
}

// Snapshot requests a snapshot capture on a node.
func (s *Simulator) Snapshot(id raft.NodeID) {
	n := s.Nodes[id]
	if n == nil || !n.Alive {
		return
	}
	s.drain(n, n.Core.RequestSnapshot())
}

func (s *Simulator) releaseHeld(n *Node, workID uint64) {
	for _, m := range n.held[workID] {
		s.Net.send(s.Time, m)
	}
	delete(n.held, workID)
}

func (s *Simulator) dump() string {
	out := ""
	for _, id := range s.sortedIDs() {
		n := s.Nodes[id]
		role, term, commit, durable, persisted, applied := n.Core.DebugState()
		out += fmt.Sprintf("node %d alive=%v role=%s term=%d commit=%d durable=%d persisted=%d applied=%d\n",
			id, n.Alive, role, term, commit, durable, persisted, applied)
		for _, e := range n.Core.DebugLog() {
			out += fmt.Sprintf("   [%d] term=%d type=%d data=%q\n", e.Index, e.Term, e.Type, e.Data)
		}
		for _, tr := range n.Core.Trace() {
			if tr.Kind == "commit" || tr.Kind == "match" || tr.Kind == "adopt" || tr.Kind == "leader" {
				out += fmt.Sprintf("   trace %s t%d idx%d %s\n", tr.Kind, tr.Term, tr.Index, tr.Detail)
			}
		}
	}
	return out
}

// Leader returns the current known leader, if any alive node claims leadership.
func (s *Simulator) Leader() (raft.NodeID, bool) {
	for _, id := range s.sortedIDs() {
		n := s.Nodes[id]
		if n.Alive && n.Core.Role() == raft.Leader {
			return id, true
		}
	}
	return 0, false
}

// checkSafety asserts the core Raft safety invariants (§11).
func (s *Simulator) checkSafety() error {
	// Election safety: at most one leader per term.
	seen := make(map[raft.Term]raft.NodeID)
	for _, id := range s.sortedIDs() {
		n := s.Nodes[id]
		if n.Alive && n.Core.Role() == raft.Leader {
			t := n.Core.Term()
			if other, ok := seen[t]; ok && other != id {
				return fmt.Errorf("election safety: two leaders in term %d: %d and %d", t, other, id)
			}
			seen[t] = id
		}
	}
	// No divergent committed commands: applied prefixes must agree across nodes.
	for _, a := range s.sortedIDs() {
		na := s.Nodes[a]
		if !na.Alive {
			continue
		}
		for _, b := range s.sortedIDs() {
			if b <= a {
				continue
			}
			nb := s.Nodes[b]
			if !nb.Alive {
				continue
			}
			min := na.FSM.Applied
			if nb.FSM.Applied < min {
				min = nb.FSM.Applied
			}
			for i := raft.Index(1); i <= min; i++ {
				da, oka := na.FSM.Commands[i]
				db, okb := nb.FSM.Commands[i]
				if oka != okb {
					return fmt.Errorf("log matching: index %d present on %d but not %d", i, a, b)
				}
				if oka && string(da) != string(db) {
					return fmt.Errorf("divergent committed command at index %d: %d=%q %d=%q\n%s", i, a, da, b, db, s.dump())
				}
			}
		}
	}
	return nil
}
