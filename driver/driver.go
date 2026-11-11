// Package driver is the reference runtime driver (RFC-0001 §5). It owns the
// single-owner core event loop, bounded I/O workers, prerequisite-gated
// messaging, resource budgets, and proposal/read waiters. The core never does
// I/O; the driver never mutates core state off the event loop.
package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	raft "github.com/sufuree/raft"
)

// FSM is the consumer's deterministic state machine (§7, §10).
type FSM interface {
	Apply(entries []raft.Entry) error
	Snapshot() ([]byte, error)
	Restore(snap raft.SnapshotMeta, data []byte) error
}

// Transport carries authenticated, framed protocol messages (§8).
type Transport interface {
	Send(m raft.Message) error
	Messages() <-chan raft.Message
	Close() error
}

// Errors returned by the driver.
var (
	ErrNotLeader      = errors.New("driver: not leader")
	ErrOverload       = errors.New("driver: admission overload")
	ErrFailed         = errors.New("driver: node failed")
	ErrStopped        = errors.New("driver: stopped")
	ErrTooLarge       = errors.New("driver: command exceeds byte limit")
	ErrOutcomeUnknown = errors.New("driver: outcome unknown")
)

// Config configures the driver. All limits are finite (§8).
type Config struct {
	ID            raft.NodeID
	ElectionTick  int
	HeartbeatTick int
	PreVote       bool
	CheckQuorum   bool
	Seed          int64
	TickInterval  time.Duration

	MaxCommandBytes     int
	MaxPendingProposals int
	MaxPendingBytes     int
	MaxInflightWork     int
}

func (c *Config) setDefaults() {
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.TickInterval == 0 {
		c.TickInterval = 10 * time.Millisecond
	}
	if c.MaxCommandBytes == 0 {
		c.MaxCommandBytes = 1 << 20
	}
	if c.MaxPendingProposals == 0 {
		c.MaxPendingProposals = 1024
	}
	if c.MaxPendingBytes == 0 {
		c.MaxPendingBytes = 32 << 20
	}
	if c.MaxInflightWork == 0 {
		c.MaxInflightWork = 256
	}
}

// Result is a structured proposal outcome (§7).
type Result struct {
	Outcome raft.Outcome
	Index   raft.Index
	Term    raft.Term
}

// Driver runs one Raft node.
type Driver struct {
	cfg   Config
	core  *raft.Core
	store raft.Storage
	fsm   FSM
	tr    Transport
	epoch uint64

	events chan raft.Event
	stop   chan struct{}
	done   chan struct{}

	mu           sync.Mutex
	started      bool
	propWait     map[uint64]chan Result
	readWait     map[string]chan raft.ReadResult
	pendingCount int
	pendingBytes int
	nextID       uint64
	failed       bool
	stopped      bool

	held  map[uint64][]raft.Message
	sem   chan struct{}
	state atomic.Pointer[stateSnapshot]
}

// stateSnapshot is the observable state published atomically by the event loop
// so accessors never read core state off-loop (single-owner, §5).
type stateSnapshot struct {
	role       raft.Role
	term       raft.Term
	leader     raft.NodeID
	durable    raft.Index
	commit     raft.Index
	persisted  raft.Index
	dispatched raft.Index
	applied    raft.Index
}

// New constructs a driver from recovered storage state.
func New(cfg Config, store raft.Storage, fsm FSM, tr Transport) (*Driver, error) {
	cfg.setDefaults()
	init, err := store.Load()
	if err != nil {
		return nil, err
	}
	epoch := init.Epoch + 1
	init.Epoch = epoch
	core := raft.NewCore(raft.Config{
		ID: cfg.ID, ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		PreVote: cfg.PreVote, CheckQuorum: cfg.CheckQuorum, Seed: cfg.Seed,
	}, init)
	d := &Driver{
		cfg: cfg, core: core, store: store, fsm: fsm, tr: tr, epoch: epoch,
		events: make(chan raft.Event, 1024),
		stop:   make(chan struct{}), done: make(chan struct{}),
		propWait: make(map[uint64]chan Result),
		readWait: make(map[string]chan raft.ReadResult),
		held:     make(map[uint64][]raft.Message),
		sem:      make(chan struct{}, cfg.MaxInflightWork),
	}
	d.publish()
	return d, nil
}

// Start launches the event loop and workers.
func (d *Driver) Start() {
	d.mu.Lock()
	if d.started || d.stopped {
		d.mu.Unlock()
		return
	}
	d.started = true
	d.mu.Unlock()
	go d.loop()
}

// Stop shuts down the driver. It is safe to call on a driver that was never
// started.
func (d *Driver) Stop() error {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return nil
	}
	d.stopped = true
	started := d.started
	close(d.stop)
	d.mu.Unlock()
	if started {
		<-d.done
	}
	return d.tr.Close()
}

func (d *Driver) loop() {
	defer close(d.done)
	ticker := time.NewTicker(d.cfg.TickInterval)
	defer ticker.Stop()
	msgs := d.tr.Messages()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			d.step(raft.Event{Kind: raft.EvTick})
		case m := <-msgs:
			d.step(raft.Event{Kind: raft.EvReceive, Message: m})
		case ev := <-d.events:
			d.step(ev)
			if ev.Kind == raft.EvPersisted {
				d.releaseHeld(ev.WorkID)
			}
		}
	}
}

func (d *Driver) step(ev raft.Event) {
	out := d.core.Step(ev)
	d.handle(out)
	d.publish()
}

func (d *Driver) publish() {
	du, co, pe, di, ap := d.core.Watermarks()
	d.state.Store(&stateSnapshot{
		role: d.core.Role(), term: d.core.Term(), leader: d.core.Leader(),
		durable: du, commit: co, persisted: pe, dispatched: di, applied: ap,
	})
}

func (d *Driver) handle(out raft.Outputs) {
	for _, om := range out.Messages {
		if om.After == 0 {
			_ = d.tr.Send(om.Message)
		} else {
			d.held[om.After] = append(d.held[om.After], om.Message)
		}
	}
	for _, w := range out.Work {
		d.dispatch(w)
	}
	for _, r := range out.Reads {
		d.mu.Lock()
		ch := d.readWait[string(r.Ctx)]
		delete(d.readWait, string(r.Ctx))
		d.mu.Unlock()
		if ch != nil {
			ch <- r
		}
	}
	for _, p := range out.Proposals {
		d.mu.Lock()
		ch := d.propWait[p.ID]
		delete(d.propWait, p.ID)
		d.pendingCount--
		d.mu.Unlock()
		if ch != nil {
			ch <- Result{Outcome: p.Outcome, Index: p.Index, Term: p.Term}
		}
	}
	if out.Status != nil && out.Status.Role == raft.Failed {
		d.mu.Lock()
		d.failed = true
		d.mu.Unlock()
	}
}

func (d *Driver) releaseHeld(workID uint64) {
	for _, m := range d.held[workID] {
		_ = d.tr.Send(m)
	}
	delete(d.held, workID)
}

func (d *Driver) dispatch(w raft.Work) {
	// Bounded workers: acquiring blocks the event loop when saturated, providing
	// backpressure rather than unbounded goroutine/memory growth (§5, §8).
	d.sem <- struct{}{}
	go func() {
		defer func() { <-d.sem }()
		switch w.Kind {
		case raft.WorkPersist:
			err := d.store.Save(w.Persist)
			if err != nil {
				d.post(raft.Event{Kind: raft.EvStorageFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			d.post(raft.Event{Kind: raft.EvPersisted, Epoch: w.Epoch, WorkID: w.ID})
		case raft.WorkApply:
			err := d.fsm.Apply(w.Apply)
			if err != nil {
				d.post(raft.Event{Kind: raft.EvApplyFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			var last raft.Index
			if len(w.Apply) > 0 {
				last = w.Apply[len(w.Apply)-1].Index
			}
			_ = d.store.MarkApplied(last)
			d.post(raft.Event{Kind: raft.EvApplied, Epoch: w.Epoch, WorkID: w.ID})
		case raft.WorkSnapshotCreate:
			data, err := d.fsm.Snapshot()
			if err != nil {
				d.post(raft.Event{Kind: raft.EvApplyFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			meta := w.Snapshot
			meta.Data = data
			d.post(raft.Event{Kind: raft.EvSnapshotCreated, Epoch: w.Epoch, WorkID: w.ID, Snapshot: meta})
		case raft.WorkSnapshotInstall:
			if err := d.fsm.Restore(w.Snapshot, w.Snapshot.Data); err != nil {
				d.post(raft.Event{Kind: raft.EvApplyFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			if err := d.store.Save(raft.PersistBatch{Snapshot: &w.Snapshot}); err != nil {
				d.post(raft.Event{Kind: raft.EvStorageFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			_ = d.store.MarkApplied(w.Snapshot.Index)
			d.post(raft.Event{Kind: raft.EvSnapshotInstalled, Epoch: w.Epoch, WorkID: w.ID})
		}
	}()
}

func (d *Driver) post(ev raft.Event) {
	select {
	case d.events <- ev:
	case <-d.stop:
	}
}

// Propose admits a command and waits for its structured outcome (§7).
func (d *Driver) Propose(ctx context.Context, data []byte) (Result, error) {
	if len(data) > d.cfg.MaxCommandBytes {
		return Result{}, ErrTooLarge
	}
	d.mu.Lock()
	if d.failed {
		d.mu.Unlock()
		return Result{}, ErrFailed
	}
	if d.stopped {
		d.mu.Unlock()
		return Result{}, ErrStopped
	}
	if d.pendingCount >= d.cfg.MaxPendingProposals || d.pendingBytes+len(data) > d.cfg.MaxPendingBytes {
		d.mu.Unlock()
		return Result{}, ErrOverload
	}
	d.nextID++
	id := d.nextID
	ch := make(chan Result, 1)
	d.propWait[id] = ch
	d.pendingCount++
	d.pendingBytes += len(data)
	d.mu.Unlock()

	d.post(raft.Event{Kind: raft.EvPropose, WorkID: id, Data: data})
	select {
	case <-ctx.Done():
		d.mu.Lock()
		delete(d.propWait, id)
		d.mu.Unlock()
		// Cancellation stops waiting; the command may still have been admitted.
		return Result{Outcome: raft.OutcomeUnknown}, ctx.Err()
	case r := <-ch:
		d.mu.Lock()
		d.pendingBytes -= len(data)
		d.mu.Unlock()
		if r.Outcome == raft.OutcomeRejected {
			return r, ErrNotLeader
		}
		if r.Outcome == raft.OutcomeUnknown {
			return r, ErrOutcomeUnknown
		}
		return r, nil
	case <-d.stop:
		return Result{Outcome: raft.OutcomeUnknown}, ErrStopped
	}
}

// Read performs a linearizable read barrier and returns the safe applied index.
func (d *Driver) Read(ctx context.Context) (raft.Index, error) {
	d.mu.Lock()
	if d.failed {
		d.mu.Unlock()
		return 0, ErrFailed
	}
	id := fmt.Sprintf("r-%d-%d", d.cfg.ID, atomic.AddUint64(&d.nextID, 1))
	ch := make(chan raft.ReadResult, 1)
	d.readWait[id] = ch
	d.mu.Unlock()
	d.post(raft.Event{Kind: raft.EvReadBarrier, ReadCtx: []byte(id)})
	select {
	case <-ctx.Done():
		d.mu.Lock()
		delete(d.readWait, id)
		d.mu.Unlock()
		return 0, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return 0, r.Err
		}
		return r.Index, nil
	case <-d.stop:
		return 0, ErrStopped
	}
}

// ConfChange proposes a membership transition.
func (d *Driver) ConfChange(ctx context.Context, target raft.Configuration) (Result, error) {
	d.mu.Lock()
	if d.failed || d.stopped {
		d.mu.Unlock()
		return Result{}, ErrFailed
	}
	d.nextID++
	id := d.nextID
	ch := make(chan Result, 1)
	d.propWait[id] = ch
	d.mu.Unlock()
	d.post(raft.Event{Kind: raft.EvProposeConfChange, WorkID: id, Config: target})
	select {
	case <-ctx.Done():
		return Result{Outcome: raft.OutcomeUnknown}, ctx.Err()
	case r := <-ch:
		if r.Outcome == raft.OutcomeRejected {
			return r, ErrNotLeader
		}
		return r, nil
	case <-d.stop:
		return Result{Outcome: raft.OutcomeUnknown}, ErrStopped
	}
}

// Snapshot requests a snapshot capture through the event loop.
func (d *Driver) Snapshot() {
	d.post(raft.Event{Kind: raft.EvSnapshotRequest})
}

// Leader reports the known leader.
func (d *Driver) Leader() raft.NodeID { return d.state.Load().leader }

// IsLeader reports whether this node is the leader.
func (d *Driver) IsLeader() bool { return d.state.Load().role == raft.Leader }

// Role returns the current role.
func (d *Driver) Role() raft.Role { return d.state.Load().role }

// Term returns the current term.
func (d *Driver) Term() raft.Term { return d.state.Load().term }

// Watermarks returns the five progress watermarks.
func (d *Driver) Watermarks() (durable, commit, persisted, dispatched, applied raft.Index) {
	s := d.state.Load()
	return s.durable, s.commit, s.persisted, s.dispatched, s.applied
}
