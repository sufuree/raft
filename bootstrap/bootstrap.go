// Package bootstrap enforces the creation/restart/disaster-recovery discipline
// of RFC-0001 §9: explicit creation, ordinary restart that never auto-creates,
// and disaster recovery gated on an operator fencing acknowledgement.
package bootstrap

import (
	"errors"
	"fmt"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/driver"
	"github.com/sufuree/raft/wal"
)

// Errors returned by bootstrap.
var (
	ErrAlreadyExists     = errors.New("bootstrap: state already exists; refusing to re-create")
	ErrRecoveryRequired  = errors.New("bootstrap: no state; explicit creation or recovery required")
	ErrFencingRequired   = errors.New("bootstrap: disaster recovery requires an operator fencing acknowledgement")
	ErrGenerationAdvance = errors.New("bootstrap: disaster recovery requires a newer storage generation")
)

// Options describes a node's identity and runtime.
type Options struct {
	Device        wal.Device
	ClusterID     uint64
	MemberID      uint64
	Generation    uint64
	InitialConfig raft.Configuration
	Driver        driver.Config
	FSM           driver.FSM
	Transport     driver.Transport
}

func (o Options) walOpts() wal.Options {
	return wal.Options{
		ClusterID: o.ClusterID, MemberID: o.MemberID, Generation: o.Generation,
		InitialConfig: o.InitialConfig,
	}
}

func build(o Options, store *wal.WAL) (*driver.Driver, error) {
	cfg := o.Driver
	cfg.ID = raft.NodeID(o.MemberID)
	return driver.New(cfg, store, o.FSM, o.Transport)
}

// Create explicitly bootstraps a new node. It refuses if any state exists, so a
// missing mount or empty directory can never silently create a fresh cluster.
func Create(o Options) (*driver.Driver, error) {
	has, err := wal.HasState(o.Device)
	if err != nil {
		return nil, err
	}
	if has {
		return nil, ErrAlreadyExists
	}
	store, err := wal.Open(o.Device, o.walOpts())
	if err != nil {
		return nil, err
	}
	return build(o, store)
}

// Open restarts a node from existing durable state. It never creates state.
func Open(o Options) (*driver.Driver, error) {
	has, err := wal.HasState(o.Device)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, ErrRecoveryRequired
	}
	store, err := wal.Open(o.Device, o.walOpts())
	if err != nil {
		return nil, err
	}
	return build(o, store)
}

// FencingAck is the operator's explicit acknowledgement that the old cluster
// authority and any external writers have been stopped or isolated (§9).
type FencingAck struct {
	Acknowledged   bool
	Operator       string
	OldIncarnation uint64
}

// DisasterRecover activates a restored backup as a new recovery incarnation. It
// requires an explicit fencing acknowledgement and a newer storage generation;
// a new incarnation alone does not stop the old cluster (§9).
func DisasterRecover(o Options, ack FencingAck) (*driver.Driver, error) {
	if !ack.Acknowledged {
		return nil, ErrFencingRequired
	}
	has, err := wal.HasState(o.Device)
	if err != nil {
		return nil, err
	}
	if has {
		_, _, storedGen, err := wal.ReadIdentity(o.Device)
		if err != nil {
			return nil, err
		}
		// A recovery incarnation must advance the storage generation beyond any
		// surviving authority; a new incarnation alone is not a fence (§9).
		if o.Generation <= storedGen {
			return nil, fmt.Errorf("%w: have %d, want > %d", ErrGenerationAdvance, o.Generation, storedGen)
		}
		if err := wal.AdvanceGeneration(o.Device, o.Generation); err != nil {
			return nil, err
		}
	}
	store, err := wal.Open(o.Device, o.walOpts())
	if err != nil {
		return nil, err
	}
	return build(o, store)
}
