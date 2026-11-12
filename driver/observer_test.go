package driver

import (
	"context"
	"sync"
	"testing"
	"time"

	raft "github.com/sufuree/raft"
)

// TestLatencyObservers: the §8 storage/apply observers fire off the event loop
// with non-negative durations and correct entry counts, and do not alter
// protocol behavior (the same proposals still commit and apply in order).
// Durations may legitimately be zero: in-memory storage can complete an
// operation below the monotonic clock's resolution.
func TestLatencyObservers(t *testing.T) {
	var mu sync.Mutex
	var saves, appliedMarks, applyBatches int
	var appliedEntries int
	var minStore, minApply time.Duration
	store := raft.NewMemStorage()
	store.SetInitialConfig(raft.Configuration{Voters: raft.NodeSet{1}})
	d, err := New(Config{
		ID: 1, ElectionTick: 5, HeartbeatTick: 1, TickInterval: time.Millisecond,
		OnStore: func(op string, dur time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			switch op {
			case "save":
				saves++
			case "applied":
				appliedMarks++
			default:
				t.Errorf("unknown store op %q", op)
			}
			if dur < 0 {
				t.Errorf("store op %q duration %v is negative", op, dur)
			}
			if minStore == 0 || dur < minStore {
				minStore = dur
			}
		},
		OnApply: func(n int, dur time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			applyBatches++
			appliedEntries += n
			if dur < 0 {
				t.Errorf("apply duration %v is negative", dur)
			}
			if minApply == 0 || dur < minApply {
				minApply = dur
			}
		},
	}, store, newTestFSM(), NewLoopNetwork().NewTransport(1))
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
	const n = 20
	for i := 0; i < n; i++ {
		res, err := d.Propose(ctx, []byte("observed"))
		if err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		if res.Outcome != raft.OutcomeApplied {
			t.Fatalf("propose %d outcome %v", i, res.Outcome)
		}
	}
	// Stop drains workers so all observer calls have happened before reading.
	d.Stop()
	mu.Lock()
	defer mu.Unlock()
	if saves == 0 || appliedMarks == 0 {
		t.Fatalf("store observer: saves=%d appliedMarks=%d, want both > 0", saves, appliedMarks)
	}
	if applyBatches == 0 || appliedEntries < n {
		t.Fatalf("apply observer: batches=%d entries=%d, want batches > 0 and entries >= %d", applyBatches, appliedEntries, n)
	}
}

// TestNilObserversUnchanged: with no observers configured the driver behaves
// identically (observers are pure observability, §5 determinism).
func TestNilObserversUnchanged(t *testing.T) {
	store := raft.NewMemStorage()
	store.SetInitialConfig(raft.Configuration{Voters: raft.NodeSet{1}})
	d, err := New(Config{ID: 1, ElectionTick: 5, HeartbeatTick: 1, TickInterval: time.Millisecond},
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
	if _, err := d.Propose(ctx, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, ap := d.Watermarks(); ap < 2 {
		t.Fatalf("applied watermark %d < 2", ap)
	}
}

// TestLoopNetworkSetDrop: the partition predicate can be installed and healed
// while transports are sending (SetDrop and Send share the network lock).
func TestLoopNetworkSetDrop(t *testing.T) {
	net := NewLoopNetwork()
	a := net.NewTransport(1)
	b := net.NewTransport(2)
	defer a.Close()
	defer b.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			net.SetDrop(func(from, to raft.NodeID) bool { return i%2 == 0 })
			net.SetDrop(nil)
		}
	}()
	for i := 0; i < 200; i++ {
		if err := a.Send(raft.Message{From: 1, To: 2}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-b.Messages():
		default:
		}
	}
	<-done
	net.SetDrop(func(from, to raft.NodeID) bool { return true })
	if err := a.Send(raft.Message{From: 1, To: 2}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-b.Messages():
		t.Fatalf("partitioned message delivered: %v", m)
	default:
	}
	net.SetDrop(nil)
}
