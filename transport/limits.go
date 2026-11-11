// Package transport is the production peer transport (RFC-0001 §8): TCP with
// mutual TLS, an authenticated handshake binding member identity to the
// cluster incarnation, explicit wire versioning, bounded frame decode, and
// per-peer byte/message budgets. Admission authorization is distinct from log
// membership: explicitly admitted learners and configuration-transition peers
// are accepted even when the receiver's membership view lags.
package transport

import (
	"errors"
	"fmt"

	raft "github.com/sufuree/raft"
)

// Typed transport errors (§8). All rejections are observable by the caller.
var (
	ErrClosed             = errors.New("transport: closed")
	ErrPeerUnknown        = errors.New("transport: peer not configured or admitted")
	ErrNotAdmitted        = errors.New("transport: peer not admitted")
	ErrClusterMismatch    = errors.New("transport: cluster incarnation mismatch")
	ErrIncompatibleLimits = errors.New("transport: incompatible resource limits")
	ErrIdentityMismatch   = errors.New("transport: claimed identity not bound to certificate")
	ErrFrameTooLarge      = errors.New("transport: frame exceeds decode limit")
	ErrSnapshotTooLarge   = errors.New("transport: snapshot exceeds peer limit")
	ErrOverload           = errors.New("transport: peer budget exhausted")
	ErrHandshake          = errors.New("transport: malformed handshake")
)

// Limits are the finite resource bounds advertised inside the authenticated
// handshake (§8). They are negotiated on every connection: join, configuration
// change, and reconnect after restart. An unauthenticated peer cannot
// downgrade them because the handshake only runs inside the mTLS channel.
//
// The compatibility chain is: MaxCommandBytes + framing <= MaxBatchBytes <=
// every participating peer's MaxFrameBytes. Inline snapshots are additionally
// bounded by the receiver's MaxSnapshotBytes; a larger snapshot produces a
// typed catch-up failure, never an unbounded allocation.
type Limits struct {
	MaxCommandBytes      int // largest admitted command payload
	MaxBatchBytes        int // largest encoded append batch produced
	MaxFrameBytes        int // decode allocation cap per inbound frame
	MaxSnapshotBytes     int // largest inline snapshot accepted
	MaxPeerInflightBytes int // per-peer outbound byte budget
	MaxPeerInflightMsgs  int // per-peer outbound message budget
}

// setDefaults chooses finite provisional bounds (§8: unspecified limits never
// mean unlimited). The recorded unikernel profile will refine these.
func (l *Limits) setDefaults() {
	if l.MaxCommandBytes == 0 {
		l.MaxCommandBytes = 1 << 20
	}
	if l.MaxBatchBytes == 0 {
		l.MaxBatchBytes = 8 << 20
	}
	if l.MaxFrameBytes == 0 {
		l.MaxFrameBytes = 16 << 20
	}
	if l.MaxSnapshotBytes == 0 {
		l.MaxSnapshotBytes = 8 << 20
	}
	if l.MaxPeerInflightBytes == 0 {
		l.MaxPeerInflightBytes = 32 << 20
	}
	if l.MaxPeerInflightMsgs == 0 {
		l.MaxPeerInflightMsgs = 1024
	}
}

// Validate checks the local limit chain: command plus framing fits a batch,
// a batch fits this node's own decode cap, and an inline snapshot fits a frame.
func (l Limits) Validate() error {
	if l.MaxCommandBytes <= 0 || l.MaxBatchBytes <= 0 || l.MaxFrameBytes <= 0 ||
		l.MaxSnapshotBytes <= 0 || l.MaxPeerInflightBytes <= 0 || l.MaxPeerInflightMsgs <= 0 {
		return fmt.Errorf("%w: all limits must be positive", ErrIncompatibleLimits)
	}
	if l.MaxCommandBytes+raft.FramingReserve > l.MaxBatchBytes {
		return fmt.Errorf("%w: command %d + framing %d exceeds batch %d",
			ErrIncompatibleLimits, l.MaxCommandBytes, raft.FramingReserve, l.MaxBatchBytes)
	}
	if l.MaxBatchBytes > l.MaxFrameBytes {
		return fmt.Errorf("%w: batch %d exceeds own decode limit %d",
			ErrIncompatibleLimits, l.MaxBatchBytes, l.MaxFrameBytes)
	}
	if l.MaxSnapshotBytes+raft.FramingReserve > l.MaxFrameBytes {
		return fmt.Errorf("%w: snapshot %d + framing exceeds decode limit %d",
			ErrIncompatibleLimits, l.MaxSnapshotBytes, l.MaxFrameBytes)
	}
	return nil
}

// Compatible checks the cluster-wide chain against a peer's advertised limits
// in both directions (§8): each side's maximum batch must fit the other's
// decode cap. Rejection is typed and happens at every handshake.
func (l Limits) Compatible(peer Limits) error {
	if l.MaxBatchBytes > peer.MaxFrameBytes {
		return fmt.Errorf("%w: local batch %d exceeds peer decode limit %d",
			ErrIncompatibleLimits, l.MaxBatchBytes, peer.MaxFrameBytes)
	}
	if peer.MaxBatchBytes > l.MaxFrameBytes {
		return fmt.Errorf("%w: peer batch %d exceeds local decode limit %d",
			ErrIncompatibleLimits, peer.MaxBatchBytes, l.MaxFrameBytes)
	}
	return nil
}
