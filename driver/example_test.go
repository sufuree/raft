package driver_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/driver"
	"github.com/sufuree/raft/wal"
)

// kvFSM is a minimal deterministic state machine. Applications own their
// command schema, deduplication, and any external effects; the library only
// orders committed commands (RFC §10).
// kvFSM is synchronized: Apply runs on the driver's worker, and readers must
// hold the same lock. The read barrier guarantees the applied index, not
// memory visibility of the FSM's fields (RFC §7 synchronized view).
type kvFSM struct {
	mu      sync.Mutex
	applied raft.Index
	values  map[string]string
}

func (f *kvFSM) Apply(entries []raft.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range entries {
		if e.Type == raft.EntryNormal && len(e.Data) > 0 {
			f.values[string(e.Data)] = "set"
		}
		if e.Index > f.applied {
			f.applied = e.Index
		}
	}
	return nil
}

func (f *kvFSM) get(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.values[key]
}

func (f *kvFSM) Snapshot() ([]byte, error) { return nil, nil }

func (f *kvFSM) Restore(s raft.SnapshotMeta, data []byte) error {
	f.applied = s.Index
	return nil
}

// Example shows a single-voter embedded node: propose a command, wait for its
// structured outcome, then perform a linearizable read. A single voter gives
// ordering and durability for one failure domain, not failover.
func Example() {
	dev, err := wal.OpenFile(tempWALPath())
	if err != nil {
		panic(err)
	}
	defer dev.Close()
	store, err := wal.Open(dev, wal.Options{
		ClusterID: 1, MemberID: 1, Generation: 1,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{1}},
	})
	if err != nil {
		panic(err)
	}
	fsm := &kvFSM{values: make(map[string]string)}
	d, err := driver.New(driver.Config{
		ID: 1, TickInterval: 5 * time.Millisecond,
	}, store, fsm, driver.NewLoopNetwork().NewTransport(1))
	if err != nil {
		panic(err)
	}
	d.Start()
	defer d.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Wait for leadership (single voter elects itself).
	for !d.IsLeader() {
		time.Sleep(time.Millisecond)
	}

	res, err := d.Propose(ctx, []byte("greeting"))
	if err != nil {
		// ErrOutcomeUnknown means the command may still commit: reconcile by
		// application-owned operation identity. A timeout is never proof of
		// rollback. Leadership does not fence external resources (RFC §10).
		panic(err)
	}
	fmt.Println("outcome:", res.Outcome == raft.OutcomeApplied)

	// Linearizable read: the barrier waits for completed application; read
	// the synchronized FSM view at or beyond the returned index.
	if _, err := d.Read(ctx); err != nil {
		panic(err)
	}
	fmt.Println("applied:", fsm.get("greeting"))
	// Output:
	// outcome: true
	// applied: set
}
