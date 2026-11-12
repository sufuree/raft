package driver

import (
	"context"
	"testing"
	"time"

	raft "github.com/sufuree/raft"
)

// TestAdmissionBudgetReleasedOnCancel: a cancelled proposal must not leak its
// admission charge (§8). The budget is refunded when the core produces the
// result, not when the caller stops waiting; after cancellations drain, new
// proposals must still be admitted (no spurious overload).
func TestAdmissionBudgetReleasedOnCancel(t *testing.T) {
	store := raft.NewMemStorage()
	store.SetInitialConfig(raft.Configuration{Voters: raft.NodeSet{1}})
	d, err := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, TickInterval: time.Millisecond,
		MaxPendingProposals: 8, MaxPendingBytes: 4096},
		store, newTestFSM(), NewLoopNetwork().NewTransport(1))
	if err != nil {
		t.Fatal(err)
	}
	d.Start()
	defer d.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for !d.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Cancel many proposals (each charges then is abandoned by the caller).
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _ = d.Propose(ctx, make([]byte, 256))
	}
	// The core still resolves them; the budget must drain to zero.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		pc, pb := d.pendingCount, d.pendingBytes
		d.mu.Unlock()
		if pc == 0 && pb == 0 {
			break
		}
		if pc < 0 || pb < 0 {
			t.Fatalf("admission budget went negative: count=%d bytes=%d", pc, pb)
		}
		time.Sleep(5 * time.Millisecond)
	}
	d.mu.Lock()
	pc, pb := d.pendingCount, d.pendingBytes
	d.mu.Unlock()
	if pc != 0 || pb != 0 {
		t.Fatalf("admission budget leaked after cancellations: count=%d bytes=%d", pc, pb)
	}
	// A fresh proposal must still be admitted.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := d.Propose(ctx, []byte("ok")); err != nil {
		t.Fatalf("proposal after cancellations rejected (leaked budget?): %v", err)
	}
}

// TestConfChangeAdmissionBalanced: a membership transition charges the same
// admission budget it refunds; pendingCount must never go negative.
func TestConfChangeAdmissionBalanced(t *testing.T) {
	store := raft.NewMemStorage()
	store.SetInitialConfig(raft.Configuration{Voters: raft.NodeSet{1}})
	d, err := New(Config{ID: 1, ElectionTick: 10, HeartbeatTick: 1, TickInterval: time.Millisecond},
		store, newTestFSM(), NewLoopNetwork().NewTransport(1))
	if err != nil {
		t.Fatal(err)
	}
	d.Start()
	defer d.Stop()
	deadline := time.Now().Add(5 * time.Second)
	for !d.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// 1 -> 1 transition is a no-op membership entry; it still exercises the
	// charge/refund path. Repeat to catch a per-result decrement imbalance.
	for i := 0; i < 5; i++ {
		_, _ = d.ConfChange(ctx, raft.Configuration{Voters: raft.NodeSet{1}})
		d.mu.Lock()
		pc := d.pendingCount
		d.mu.Unlock()
		if pc < 0 {
			t.Fatalf("pendingCount negative after conf change %d: %d", i, pc)
		}
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		pc := d.pendingCount
		d.mu.Unlock()
		if pc == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("pendingCount did not drain to 0 after conf changes")
}
