package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	raft "github.com/sufuree/raft"
)

// Frame layout: [4-byte big-endian length][1-byte kind][payload]. The length
// covers kind+payload and is validated against the decode cap before any
// allocation (§8).
const (
	frameHandshake byte = 1
	frameMessage   byte = 2
	frameReject    byte = 3

	frameHeaderSize = 5
	// handshakeCap bounds handshake allocation independent of the negotiated
	// frame limit: the handshake is small and fixed-size.
	handshakeCap = 1024
	// rejectReasonCap bounds a rejection reason string.
	rejectReasonCap = 512
)

// handshakeMagic identifies the protocol inside the authenticated channel.
var handshakeMagic = [4]byte{'S', 'F', 'R', '1'}

// Rejection codes travel in a reject frame so the dialer observes a typed
// error rather than an opaque EOF.
const (
	rejectVersion   byte = 1
	rejectCluster   byte = 2
	rejectAdmission byte = 3
	rejectLimits    byte = 4
	rejectIdentity  byte = 5
	rejectMalformed byte = 6
)

func rejectError(code byte, reason string) error {
	var base error
	switch code {
	case rejectVersion:
		base = raft.ErrWireVersion
	case rejectCluster:
		base = ErrClusterMismatch
	case rejectAdmission:
		base = ErrNotAdmitted
	case rejectLimits:
		base = ErrIncompatibleLimits
	case rejectIdentity:
		base = ErrIdentityMismatch
	default:
		base = ErrHandshake
	}
	if reason == "" {
		return base
	}
	return fmt.Errorf("%w: %s", base, reason)
}

func rejectCode(err error) byte {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, raft.ErrWireVersion):
		return rejectVersion
	case errors.Is(err, ErrClusterMismatch):
		return rejectCluster
	case errors.Is(err, ErrNotAdmitted):
		return rejectAdmission
	case errors.Is(err, ErrIncompatibleLimits):
		return rejectLimits
	case errors.Is(err, ErrIdentityMismatch):
		return rejectIdentity
	default:
		return rejectMalformed
	}
}

// readFrame reads one bounded frame. The length claim is checked against
// limit before allocating the payload buffer (§8).
func readFrame(r io.Reader, limit int) (byte, []byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n < 1 {
		return 0, nil, ErrHandshake
	}
	if limit > 0 && int64(n) > int64(limit) {
		return 0, nil, fmt.Errorf("%w: claim %d > cap %d", ErrFrameTooLarge, n, limit)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, nil, err
	}
	return buf[0], buf[1:], nil
}

// writeFrame writes one frame with an explicit deadline.
func writeFrame(w net.Conn, kind byte, payload []byte, deadline time.Time) error {
	buf := make([]byte, frameHeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(1+len(payload)))
	buf[4] = kind
	copy(buf[5:], payload)
	if err := w.SetWriteDeadline(deadline); err != nil {
		return err
	}
	_, err := w.Write(buf)
	return err
}

// handshake is the authenticated identity-and-limits exchange (§8, §9).
type handshake struct {
	cluster uint64
	member  raft.NodeID
	limits  Limits
}

func encodeHandshake(h handshake) []byte {
	b := append([]byte(nil), handshakeMagic[:]...)
	b = append(b, raft.WireVersion)
	b = binary.BigEndian.AppendUint64(b, h.cluster)
	b = binary.BigEndian.AppendUint64(b, uint64(h.member))
	b = binary.BigEndian.AppendUint64(b, uint64(h.limits.MaxCommandBytes))
	b = binary.BigEndian.AppendUint64(b, uint64(h.limits.MaxBatchBytes))
	b = binary.BigEndian.AppendUint64(b, uint64(h.limits.MaxFrameBytes))
	b = binary.BigEndian.AppendUint64(b, uint64(h.limits.MaxSnapshotBytes))
	b = binary.BigEndian.AppendUint64(b, uint64(h.limits.MaxPeerInflightBytes))
	b = binary.BigEndian.AppendUint64(b, uint64(h.limits.MaxPeerInflightMsgs))
	return b
}

func decodeHandshake(b []byte) (handshake, error) {
	var h handshake
	if len(b) != 4+1+8+8+6*8 {
		return h, ErrHandshake
	}
	if string(b[0:4]) != string(handshakeMagic[:]) {
		return h, ErrHandshake
	}
	if b[4] != raft.WireVersion {
		return h, fmt.Errorf("%w: peer version %d, local %d", raft.ErrWireVersion, b[4], raft.WireVersion)
	}
	h.cluster = binary.BigEndian.Uint64(b[5:13])
	h.member = raft.NodeID(binary.BigEndian.Uint64(b[13:21]))
	u := func(off int) int { return int(binary.BigEndian.Uint64(b[off : off+8])) }
	h.limits = Limits{
		MaxCommandBytes:      u(21),
		MaxBatchBytes:        u(29),
		MaxFrameBytes:        u(37),
		MaxSnapshotBytes:     u(45),
		MaxPeerInflightBytes: u(53),
		MaxPeerInflightMsgs:  u(61),
	}
	return h, nil
}

func encodeReject(code byte, reason string) []byte {
	if len(reason) > rejectReasonCap {
		reason = reason[:rejectReasonCap]
	}
	b := []byte{code}
	b = binary.BigEndian.AppendUint16(b, uint16(len(reason)))
	return append(b, reason...)
}

func decodeReject(b []byte) error {
	if len(b) < 3 {
		return ErrHandshake
	}
	n := int(binary.BigEndian.Uint16(b[1:3]))
	if n > len(b)-3 || n > rejectReasonCap {
		return ErrHandshake
	}
	return rejectError(b[0], string(b[3:3+n]))
}
