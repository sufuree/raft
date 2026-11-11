package driver

import (
	"sync"

	raft "github.com/sufuree/raft"
)

// LoopNetwork is an in-process authenticated-transport stand-in for tests. It
// routes framed messages between drivers with optional partitioning.
type LoopNetwork struct {
	mu    sync.Mutex
	nodes map[raft.NodeID]*LoopTransport
	// Drop, when set, drops messages for which it returns true (partitions).
	Drop func(from, to raft.NodeID) bool
}

// NewLoopNetwork creates an empty loopback network.
func NewLoopNetwork() *LoopNetwork {
	return &LoopNetwork{nodes: make(map[raft.NodeID]*LoopTransport)}
}

// NewTransport registers a transport for a node.
func (n *LoopNetwork) NewTransport(id raft.NodeID) *LoopTransport {
	n.mu.Lock()
	defer n.mu.Unlock()
	t := &LoopTransport{id: id, net: n, ch: make(chan raft.Message, 8192)}
	n.nodes[id] = t
	return t
}

// LoopTransport is one endpoint.
type LoopTransport struct {
	id     raft.NodeID
	net    *LoopNetwork
	ch     chan raft.Message
	mu     sync.Mutex
	closed bool
}

// Send implements Transport.
func (t *LoopTransport) Send(m raft.Message) error {
	t.net.mu.Lock()
	dst := t.net.nodes[m.To]
	drop := t.net.Drop
	t.net.mu.Unlock()
	if dst == nil {
		return nil
	}
	if drop != nil && drop(m.From, m.To) {
		return nil
	}
	dst.mu.Lock()
	defer dst.mu.Unlock()
	if dst.closed {
		return nil
	}
	select {
	case dst.ch <- m.Clone():
	default: // bounded queue: drop rather than block the sender
	}
	return nil
}

// Messages implements Transport.
func (t *LoopTransport) Messages() <-chan raft.Message { return t.ch }

// Close implements Transport.
func (t *LoopTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.closed = true
		close(t.ch)
	}
	return nil
}
