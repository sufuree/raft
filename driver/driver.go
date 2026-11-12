// Package driver is the reference runtime driver (RFC-0001 §5). It owns the
// single-owner core event loop, bounded I/O workers, prerequisite-gated
// messaging, resource budgets, and proposal/read waiters. The core never does
// I/O; the driver never mutates core state off the event loop.
package driver

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
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
	ErrLimits         = errors.New("driver: incompatible resource limits")
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
	// MaxReadWaiters caps concurrent read barriers (§8).
	MaxReadWaiters int
	// MaxBatchBytes is the storage append-batch limit; the command plus
	// framing must fit it (§8). It must match the storage adapter's limit.
	MaxBatchBytes int
	// StagingDir holds streamed snapshot staging files (§6.4). Empty selects
	// a directory under os.TempDir. Interrupted staging never becomes a
	// snapshot; orphans are removed at construction.
	StagingDir string
	// MaxSnapshotBytes caps an inbound streamed snapshot (§8).
	MaxSnapshotBytes int
	// SnapshotChunkBytes bounds one streamed snapshot chunk (§6.4).
	SnapshotChunkBytes int

	// OnStore, when set, observes each ordered storage operation's wall
	// duration (§8 metrics): op "save" covers write+sync of one persistence
	// batch; op "applied" covers the applied-watermark update. It runs
	// synchronously on the storage goroutine, never on the event loop, so a slow
	// observer delays the durability receipt of subsequent batches — keep it
	// non-blocking. It is observability only: protocol decisions never depend on
	// it (§5).
	OnStore func(op string, d time.Duration)
	// OnApply, when set, observes each FSM apply batch with the dispatch →
	// FSM-completion duration (§8, §11 apply-latency reporting). It runs on an
	// apply worker, never on the event loop, and must not block. It is
	// observability only: protocol decisions never depend on it (§5).
	OnApply func(entries int, d time.Duration)
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
	if c.MaxReadWaiters == 0 {
		c.MaxReadWaiters = 1024
	}
	if c.MaxBatchBytes == 0 {
		c.MaxBatchBytes = 8 << 20
	}
	if c.MaxSnapshotBytes == 0 {
		c.MaxSnapshotBytes = 64 << 20
	}
	if c.StagingDir == "" {
		c.StagingDir = filepath.Join(os.TempDir(), "raft-staging")
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

	events     chan raft.Event
	storeCh    chan storeJob
	compMu     sync.Mutex
	comps      []raft.Event
	compNotify chan struct{}
	stop       chan struct{}
	done       chan struct{}
	storeDone  chan struct{}

	mu           sync.Mutex
	started      bool
	propWait     map[uint64]chan Result
	readWait     map[string]chan raft.ReadResult
	pendingCount int
	pendingBytes int
	// pendingSize records each admitted proposal's charged bytes so the budget
	// is refunded when the core produces its result (§8), independent of whether
	// the caller is still waiting (cancellation must not leak the charge).
	pendingSize map[uint64]int
	nextID      uint64
	failed      bool
	stopped     bool

	held    map[uint64][]raft.Message
	sem     chan struct{}
	state   atomic.Pointer[stateSnapshot]
	failMsg atomic.Pointer[string]
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
	// Limit chain (§8): the maximum command plus framing must fit an append
	// batch; the transport handshake enforces batch <= every peer's decode
	// limit at every connection.
	if cfg.MaxCommandBytes+raft.FramingReserve > cfg.MaxBatchBytes {
		return nil, fmt.Errorf("%w: command %d + framing %d exceeds batch %d",
			ErrLimits, cfg.MaxCommandBytes, raft.FramingReserve, cfg.MaxBatchBytes)
	}
	init, err := store.Load()
	if err != nil {
		return nil, err
	}
	epoch := init.Epoch + 1
	init.Epoch = epoch
	core := raft.NewCore(raft.Config{
		ID: cfg.ID, ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		PreVote: cfg.PreVote, CheckQuorum: cfg.CheckQuorum, Seed: cfg.Seed,
		SnapshotChunkBytes: cfg.SnapshotChunkBytes, MaxSnapshotBytes: cfg.MaxSnapshotBytes,
		MaxAppendBatchBytes: cfg.MaxBatchBytes - raft.FramingReserve,
	}, init)
	d := &Driver{
		cfg: cfg, core: core, store: store, fsm: fsm, tr: tr, epoch: epoch,
		events:     make(chan raft.Event, 1024),
		storeCh:    make(chan storeJob, cfg.MaxInflightWork),
		compNotify: make(chan struct{}, 1),
		stop:       make(chan struct{}), done: make(chan struct{}),
		storeDone:   make(chan struct{}),
		propWait:    make(map[uint64]chan Result),
		readWait:    make(map[string]chan raft.ReadResult),
		pendingSize: make(map[uint64]int),
		held:        make(map[uint64][]raft.Message),
		sem:         make(chan struct{}, cfg.MaxInflightWork),
	}
	// Remove orphaned staging files from prior instances (§6.4): interrupted
	// transfers never become snapshots.
	if matches, _ := filepath.Glob(d.stagingPattern()); matches != nil {
		for _, m := range matches {
			_ = os.Remove(m)
		}
	}
	d.publish()
	return d, nil
}

func (d *Driver) stagingPattern() string {
	return filepath.Join(d.cfg.StagingDir, fmt.Sprintf("raft-snap-%d-*.staging", d.cfg.ID))
}

func (d *Driver) stagingPath(index raft.Index) string {
	return filepath.Join(d.cfg.StagingDir, fmt.Sprintf("raft-snap-%d-%d.staging", d.cfg.ID, index))
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
	go d.storeLoop()
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
		<-d.storeDone
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
		case <-d.compNotify:
			// One completion per loop iteration: completions can generate new
			// work, and draining a whole batch here would starve network and
			// tick processing under load (§5 control-traffic scheduling).
			d.compMu.Lock()
			var ev raft.Event
			ok := false
			if len(d.comps) > 0 {
				ev = d.comps[0]
				d.comps = d.comps[1:]
				ok = true
			}
			more := len(d.comps) > 0
			d.compMu.Unlock()
			if ok {
				d.step(ev)
				if ev.Kind == raft.EvPersisted {
					d.releaseHeld(ev.WorkID)
				}
			}
			if more {
				select {
				case d.compNotify <- struct{}{}:
				default:
				}
			}
		}
	}
}

// stageChunk writes one streamed snapshot chunk to its staging file (§6.4).
// Staging is transient; end-to-end integrity is verified at install.
func (d *Driver) stageChunk(w raft.Work) error {
	if w.ChunkTotal > uint64(d.cfg.MaxSnapshotBytes) {
		return fmt.Errorf("driver: snapshot %d exceeds cap %d", w.ChunkTotal, d.cfg.MaxSnapshotBytes)
	}
	if err := os.MkdirAll(d.cfg.StagingDir, 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(d.stagingPath(w.Snapshot.Index), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteAt(w.ChunkData, int64(w.ChunkOffset)); err != nil {
		return err
	}
	return nil
}

// readStaged resolves a complete staged snapshot, bounded and verified.
func (d *Driver) readStaged(w raft.Work) ([]byte, error) {
	if w.ChunkTotal > uint64(d.cfg.MaxSnapshotBytes) {
		return nil, fmt.Errorf("driver: snapshot %d exceeds cap %d", w.ChunkTotal, d.cfg.MaxSnapshotBytes)
	}
	f, err := os.Open(d.stagingPath(w.Snapshot.Index))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data := make([]byte, w.ChunkTotal)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, fmt.Errorf("driver: incomplete staging: %w", err)
	}
	if crc32.ChecksumIEEE(data) != w.ChunkCRC {
		return nil, errors.New("driver: staged snapshot checksum mismatch")
	}
	return data, nil
}

// postCompletion queues a worker completion for the event loop. It never
// blocks the caller.
func (d *Driver) postCompletion(ev raft.Event) {
	d.compMu.Lock()
	d.comps = append(d.comps, ev)
	d.compMu.Unlock()
	select {
	case d.compNotify <- struct{}{}:
	default:
	}
}

// storeJob is one ordered storage operation. All Save and MarkApplied calls
// go through storeLoop: one ordered persistence batch in flight per node
// (§5), so batches apply in work order and storage has a single accessor.
type storeJob struct {
	save    *raft.PersistBatch
	applied raft.Index
	reply   chan error // synchronous caller (apply/install workers)
	work    raft.Work  // asynchronous persist completion
}

func (d *Driver) storeLoop() {
	defer close(d.storeDone)
	for {
		select {
		case <-d.stop:
			return
		case j := <-d.storeCh:
			var err error
			start := time.Now()
			op := "save"
			if j.save != nil {
				err = d.store.Save(*j.save)
			} else {
				op = "applied"
				err = d.store.MarkApplied(j.applied)
			}
			if d.cfg.OnStore != nil {
				d.cfg.OnStore(op, time.Since(start))
			}
			if j.reply != nil {
				j.reply <- err
				continue
			}
			if err != nil {
				d.postCompletion(raft.Event{Kind: raft.EvStorageFailed, Epoch: j.work.Epoch, WorkID: j.work.ID, Err: err})
			} else {
				d.postCompletion(raft.Event{Kind: raft.EvPersisted, Epoch: j.work.Epoch, WorkID: j.work.ID})
			}
		}
	}
}

// storeSync runs one storage operation in order and waits for its receipt.
func (d *Driver) storeSync(j storeJob) error {
	j.reply = make(chan error, 1)
	select {
	case d.storeCh <- j:
	case <-d.stop:
		return ErrStopped
	}
	select {
	case err := <-j.reply:
		return err
	case <-d.stop:
		return ErrStopped
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
		// Refund the admission charge when the result is produced (§8), whether
		// or not the caller is still waiting; cancellation only removes the
		// waiter and must not leak the budget.
		d.pendingCount--
		d.pendingBytes -= d.pendingSize[p.ID]
		delete(d.pendingSize, p.ID)
		d.mu.Unlock()
		if ch != nil {
			ch <- Result{Outcome: p.Outcome, Index: p.Index, Term: p.Term}
		}
	}
	if out.Status != nil && out.Status.Role == raft.Failed {
		d.mu.Lock()
		d.failed = true
		d.mu.Unlock()
		reason := out.Status.Reason
		d.failMsg.Store(&reason)
	}
}

func (d *Driver) releaseHeld(workID uint64) {
	for _, m := range d.held[workID] {
		_ = d.tr.Send(m)
	}
	delete(d.held, workID)
}

func (d *Driver) dispatch(w raft.Work) {
	// Persistence is serialized in work order through storeLoop (§5): one
	// ordered batch in flight per node. A full queue applies backpressure to
	// the event loop rather than unbounded growth (§8).
	if w.Kind == raft.WorkPersist {
		select {
		case d.storeCh <- storeJob{save: &w.Persist, work: w}:
		case <-d.stop:
		}
		return
	}
	// Bounded apply/snapshot workers: acquiring blocks the event loop when
	// saturated, providing backpressure rather than unbounded goroutines.
	select {
	case d.sem <- struct{}{}:
	case <-d.stop:
		return
	}
	dispatched := time.Now()
	go func() {
		defer func() { <-d.sem }()
		switch w.Kind {
		case raft.WorkApply:
			err := d.fsm.Apply(w.Apply)
			if d.cfg.OnApply != nil {
				d.cfg.OnApply(len(w.Apply), time.Since(dispatched))
			}
			if err != nil {
				d.postCompletion(raft.Event{Kind: raft.EvApplyFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			var last raft.Index
			if len(w.Apply) > 0 {
				last = w.Apply[len(w.Apply)-1].Index
			}
			// A failed applied-watermark write is a failed durable write (§6.2).
			if err := d.storeSync(storeJob{applied: last}); err != nil {
				d.postCompletion(raft.Event{Kind: raft.EvStorageFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			d.postCompletion(raft.Event{Kind: raft.EvApplied, Epoch: w.Epoch, WorkID: w.ID})
		case raft.WorkSnapshotCreate:
			data, err := d.fsm.Snapshot()
			if err != nil {
				d.postCompletion(raft.Event{Kind: raft.EvApplyFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			meta := w.Snapshot
			meta.Data = data
			d.postCompletion(raft.Event{Kind: raft.EvSnapshotCreated, Epoch: w.Epoch, WorkID: w.ID, Snapshot: meta})
		case raft.WorkSnapshotStage:
			if err := d.stageChunk(w); err != nil {
				d.postCompletion(raft.Event{Kind: raft.EvStorageFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			d.postCompletion(raft.Event{Kind: raft.EvSnapshotStaged, Epoch: w.Epoch, WorkID: w.ID})
		case raft.WorkSnapshotInstall:
			data := w.Snapshot.Data
			if data == nil && w.ChunkTotal > 0 {
				// Streamed install: resolve staging with end-to-end integrity
				// before restore (§6.4).
				var err error
				data, err = d.readStaged(w)
				if err != nil {
					d.postCompletion(raft.Event{Kind: raft.EvApplyFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
					return
				}
			}
			meta := w.Snapshot
			meta.Data = data
			if err := d.fsm.Restore(meta, data); err != nil {
				d.postCompletion(raft.Event{Kind: raft.EvApplyFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			if err := d.storeSync(storeJob{save: &raft.PersistBatch{Snapshot: &meta}}); err != nil {
				d.postCompletion(raft.Event{Kind: raft.EvStorageFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			if err := d.storeSync(storeJob{applied: w.Snapshot.Index}); err != nil {
				d.postCompletion(raft.Event{Kind: raft.EvStorageFailed, Epoch: w.Epoch, WorkID: w.ID, Err: err})
				return
			}
			if w.ChunkTotal > 0 {
				_ = os.Remove(d.stagingPath(w.Snapshot.Index))
			}
			d.postCompletion(raft.Event{Kind: raft.EvSnapshotInstalled, Epoch: w.Epoch, WorkID: w.ID})
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
	// Storage preflight (§8): reject admission before exhaustion while the
	// recovery reserve is preserved. Actual write failure is terminal (§6.2).
	if pf, ok := d.store.(interface{ Preflight(int) error }); ok {
		if err := pf.Preflight(len(data)); err != nil {
			d.mu.Unlock()
			return Result{}, ErrOverload
		}
	}
	d.nextID++
	id := d.nextID
	ch := make(chan Result, 1)
	d.propWait[id] = ch
	d.pendingCount++
	d.pendingBytes += len(data)
	d.pendingSize[id] = len(data)
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
		// The admission charge was refunded when the core produced this result
		// (handle); nothing to release here.
		if r.Outcome == raft.OutcomeRejected {
			return r, ErrNotLeader
		}
		if r.Outcome == raft.OutcomeOverload {
			return r, ErrOverload
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
	if d.stopped {
		d.mu.Unlock()
		return 0, ErrStopped
	}
	if len(d.readWait) >= d.cfg.MaxReadWaiters {
		d.mu.Unlock()
		return 0, ErrOverload
	}
	d.nextID++
	id := fmt.Sprintf("r-%d-%d", d.cfg.ID, d.nextID)
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
	sz := len(raft.EncodeConfig(target))
	d.mu.Lock()
	if d.failed || d.stopped {
		d.mu.Unlock()
		return Result{}, ErrFailed
	}
	// A membership transition is a pending proposal too: charge the same
	// admission budget so handle's per-result refund stays balanced (§8).
	if d.pendingCount >= d.cfg.MaxPendingProposals || d.pendingBytes+sz > d.cfg.MaxPendingBytes {
		d.mu.Unlock()
		return Result{}, ErrOverload
	}
	d.nextID++
	id := d.nextID
	ch := make(chan Result, 1)
	d.propWait[id] = ch
	d.pendingCount++
	d.pendingBytes += sz
	d.pendingSize[id] = sz
	d.mu.Unlock()
	d.post(raft.Event{Kind: raft.EvProposeConfChange, WorkID: id, Config: target})
	select {
	case <-ctx.Done():
		d.mu.Lock()
		delete(d.propWait, id)
		d.mu.Unlock()
		return Result{Outcome: raft.OutcomeUnknown}, ctx.Err()
	case r := <-ch:
		if r.Outcome == raft.OutcomeRejected {
			return r, ErrNotLeader
		}
		if r.Outcome == raft.OutcomeOverload {
			return r, ErrOverload
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

// DebugProgress reports leader replication state for diagnostics. It reads
// core state directly: call only after Stop (single-owner rule, §5).
func (d *Driver) DebugProgress() map[raft.NodeID]raft.Progress { return d.core.DebugProgress() }

// FailReason reports why the node entered terminal Failed, or "" (§8).
func (d *Driver) FailReason() string {
	if p := d.failMsg.Load(); p != nil {
		return *p
	}
	return ""
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
