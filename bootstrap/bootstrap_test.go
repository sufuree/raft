package bootstrap

import (
	"errors"
	"testing"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/driver"
	"github.com/sufuree/raft/wal"
)

type nopFSM struct{}

func (nopFSM) Apply([]raft.Entry) error                { return nil }
func (nopFSM) Snapshot() ([]byte, error)               { return nil, nil }
func (nopFSM) Restore(raft.SnapshotMeta, []byte) error { return nil }

func opts(dev wal.Device) Options {
	return Options{
		Device: dev, ClusterID: 1, MemberID: 1, Generation: 1,
		InitialConfig: raft.Configuration{Voters: raft.NodeSet{1}},
		Driver:        driver.Config{ID: 1},
		FSM:           nopFSM{},
		Transport:     driver.NewLoopNetwork().NewTransport(1),
	}
}

func TestCreateThenRefuseRecreate(t *testing.T) {
	dev := wal.NewMemDevice()
	d, err := Create(opts(dev))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	d.Stop()
	if _, err := Create(opts(dev)); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestOpenEmptyRefused(t *testing.T) {
	dev := wal.NewMemDevice()
	if _, err := Open(opts(dev)); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("expected ErrRecoveryRequired on empty, got %v", err)
	}
}

func TestOpenExisting(t *testing.T) {
	dev := wal.NewMemDevice()
	d, err := Create(opts(dev))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	d.Stop()
	d2, err := Open(opts(dev))
	if err != nil {
		t.Fatalf("open existing: %v", err)
	}
	d2.Stop()
}

func TestOpenWrongIdentity(t *testing.T) {
	dev := wal.NewMemDevice()
	d, _ := Create(opts(dev))
	d.Stop()
	o := opts(dev)
	o.ClusterID = 999
	if _, err := Open(o); err == nil {
		t.Fatalf("expected identity mismatch on open")
	}
}

func TestDisasterRecoverRequiresFencing(t *testing.T) {
	dev := wal.NewMemDevice()
	if _, err := DisasterRecover(opts(dev), FencingAck{}); !errors.Is(err, ErrFencingRequired) {
		t.Fatalf("expected ErrFencingRequired, got %v", err)
	}
	d, err := DisasterRecover(opts(dev), FencingAck{Acknowledged: true, Operator: "op"})
	if err != nil {
		t.Fatalf("fenced DR: %v", err)
	}
	d.Stop()
}

func TestDisasterRecoverGenerationRegression(t *testing.T) {
	dev := wal.NewMemDevice()
	o := opts(dev)
	o.Generation = 5
	d, _ := Create(o)
	d.Stop()
	// Attempt DR with an older generation must be rejected.
	o2 := opts(dev)
	o2.Generation = 3
	if _, err := DisasterRecover(o2, FencingAck{Acknowledged: true}); err == nil {
		t.Fatalf("expected generation regression rejection")
	}
}

func TestDisasterRecoverAdvances(t *testing.T) {
	dev := wal.NewMemDevice()
	o := opts(dev)
	o.Generation = 5
	d, err := Create(o)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	d.Stop()
	o2 := opts(dev)
	o2.Generation = 9
	d2, err := DisasterRecover(o2, FencingAck{Acknowledged: true, Operator: "op"})
	if err != nil {
		t.Fatalf("DR advance: %v", err)
	}
	d2.Stop()
	_, _, gen, _ := wal.ReadIdentity(dev)
	if gen != 9 {
		t.Fatalf("generation not advanced: %d", gen)
	}
}
