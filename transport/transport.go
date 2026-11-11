package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	raft "github.com/sufuree/raft"
)

// alpn pins the application protocol inside TLS.
const alpn = "sufuree-raft/1"

// Config configures a Transport. TLS must provide mutual authentication: a
// certificate, ClientCAs (server verifies clients), and RootCAs (client
// verifies servers). Trust/key configuration stays outside the core (§8).
type Config struct {
	ID        raft.NodeID
	ClusterID uint64 // cluster incarnation; validated on every handshake (§9)
	Listen    string
	Peers     map[raft.NodeID]string // dial addresses

	TLS    *tls.Config
	Limits Limits

	// Admit authorizes peers distinct from log membership (§8): explicitly
	// admitted learners and transition peers are accepted even when the local
	// membership view lags. nil admits only configured Peers. Authentication
	// does not itself grant voting membership.
	Admit func(raft.NodeID) bool

	// BindIdentity binds the claimed member ID to the authenticated leaf
	// certificate. nil accepts any ID whose certificate chain verified; set
	// this to enforce a certificate-to-identity mapping in production.
	BindIdentity func(raft.NodeID, *x509.Certificate) bool

	HandshakeTimeout  time.Duration // default 5s
	DialTimeout       time.Duration // default 5s
	WriteTimeout      time.Duration // default 5s
	MinRedialInterval time.Duration // default 200ms
	MessageQueue      int           // inbound delivery queue depth, default 4096
}

func (c *Config) setDefaults() {
	if c.HandshakeTimeout == 0 {
		c.HandshakeTimeout = 5 * time.Second
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.MinRedialInterval == 0 {
		c.MinRedialInterval = 200 * time.Millisecond
	}
	if c.MessageQueue == 0 {
		c.MessageQueue = 4096
	}
	c.Limits.setDefaults()
}

// Stats are bounded-cardinality transport counters (§8).
type Stats struct {
	HandshakesOK      uint64
	HandshakeRejected uint64
	FrameDecodeErrors uint64
	MessagesDelivered uint64
	MessagesDropped   uint64
	SendOverload      uint64
	SendDropped       uint64
	DialErrors        uint64
}

// PeerStats reports one peer's budget usage and health.
type PeerStats struct {
	Connected     bool
	InflightBytes int
	InflightMsgs  int
	Sent          uint64
	Dropped       uint64
	LastError     string
}

// Transport implements the driver Transport contract over TCP+mTLS. Send is
// non-blocking: a saturated peer budget returns typed ErrOverload instead of
// stalling the protocol event loop; Raft retransmission recovers drops.
type Transport struct {
	cfg       Config
	ln        net.Listener
	clientTLS *tls.Config
	messages  chan raft.Message
	stop      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once

	mu      sync.Mutex
	stopped bool
	peers   map[raft.NodeID]*peer
	conns   map[*connState]struct{}

	stats struct {
		handshakesOK      atomic.Uint64
		handshakeRejected atomic.Uint64
		frameDecodeErrors atomic.Uint64
		messagesDelivered atomic.Uint64
		messagesDropped   atomic.Uint64
		sendOverload      atomic.Uint64
		sendDropped       atomic.Uint64
		dialErrors        atomic.Uint64
	}
}

// connState is one authenticated connection. initiator is the dialing side's
// ID; simultaneous dials resolve deterministically toward the smaller ID.
type connState struct {
	conn      net.Conn
	initiator raft.NodeID
	remote    Limits
}

type sendItem struct {
	buf  []byte // encoded message payload
	size int    // framed size charged to the budget
	snap int    // snapshot data size (0 for non-snapshot messages)
}

// peer owns one remote member's connection lifecycle. A single supervisor
// goroutine serializes dialing, adoption, and writes; a read loop per adopted
// connection delivers inbound messages.
type peer struct {
	t      *Transport
	id     raft.NodeID
	addr   string // empty for inbound-only (admitted, not configured) peers
	sendCh chan sendItem

	inbound chan *connState
	broken  chan *connState

	mu         sync.Mutex
	inflightB  int
	inflightM  int
	remote     Limits
	haveRemote bool
	connected  bool
	sent       uint64
	dropped    uint64
	lastErr    string
}

// New validates configuration, starts the listener, and prepares peer state.
func New(cfg Config) (*Transport, error) {
	if cfg.ID == 0 {
		return nil, fmt.Errorf("transport: nonzero member ID required")
	}
	if cfg.ClusterID == 0 {
		return nil, fmt.Errorf("transport: nonzero cluster incarnation required")
	}
	if cfg.TLS == nil || (len(cfg.TLS.Certificates) == 0 && cfg.TLS.GetCertificate == nil) {
		return nil, fmt.Errorf("transport: mTLS certificate required")
	}
	if cfg.TLS.ClientCAs == nil || cfg.TLS.RootCAs == nil {
		return nil, fmt.Errorf("transport: mTLS requires ClientCAs and RootCAs")
	}
	cfg.setDefaults()
	if err := cfg.Limits.Validate(); err != nil {
		return nil, err
	}
	serverTLS := cfg.TLS.Clone()
	serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	serverTLS.MinVersion = tls.VersionTLS13
	serverTLS.NextProtos = []string{alpn}
	clientTLS := cfg.TLS.Clone()
	clientTLS.MinVersion = tls.VersionTLS13
	clientTLS.NextProtos = []string{alpn}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{
		cfg:       cfg,
		ln:        tls.NewListener(ln, serverTLS),
		clientTLS: clientTLS,
		messages:  make(chan raft.Message, cfg.MessageQueue),
		stop:      make(chan struct{}),
		ctx:       ctx, cancel: cancel,
		peers: make(map[raft.NodeID]*peer),
		conns: make(map[*connState]struct{}),
	}
	for id, addr := range cfg.Peers {
		if id == cfg.ID {
			continue
		}
		t.newPeerLocked(id, addr)
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

// Addr returns the bound listen address.
func (t *Transport) Addr() net.Addr { return t.ln.Addr() }

// AddPeer configures a dial address for a peer (join or reconfiguration).
// The peer is admitted by configuration; the authenticated handshake still
// validates incarnation, identity, and limits on every connection.
func (t *Transport) AddPeer(id raft.NodeID, addr string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return ErrClosed
	}
	if id == t.cfg.ID {
		return nil
	}
	if p := t.peers[id]; p != nil {
		p.mu.Lock()
		p.addr = addr
		p.mu.Unlock()
		return nil
	}
	t.newPeerLocked(id, addr)
	return nil
}

// Messages implements the driver Transport contract.
func (t *Transport) Messages() <-chan raft.Message { return t.messages }

// Stats returns a snapshot of transport counters.
func (t *Transport) Stats() Stats {
	return Stats{
		HandshakesOK:      t.stats.handshakesOK.Load(),
		HandshakeRejected: t.stats.handshakeRejected.Load(),
		FrameDecodeErrors: t.stats.frameDecodeErrors.Load(),
		MessagesDelivered: t.stats.messagesDelivered.Load(),
		MessagesDropped:   t.stats.messagesDropped.Load(),
		SendOverload:      t.stats.sendOverload.Load(),
		SendDropped:       t.stats.sendDropped.Load(),
		DialErrors:        t.stats.dialErrors.Load(),
	}
}

// PeerStats reports one peer's state; ok is false for unknown peers.
func (t *Transport) PeerStats(id raft.NodeID) (PeerStats, bool) {
	t.mu.Lock()
	p := t.peers[id]
	t.mu.Unlock()
	if p == nil {
		return PeerStats{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return PeerStats{
		Connected:     p.connected,
		InflightBytes: p.inflightB,
		InflightMsgs:  p.inflightM,
		Sent:          p.sent,
		Dropped:       p.dropped,
		LastError:     p.lastErr,
	}, true
}

// Send implements the driver Transport contract. It encodes once, enforces
// local and negotiated limits before queueing, charges the per-peer budget,
// and never blocks the caller (§8).
func (t *Transport) Send(m raft.Message) error {
	select {
	case <-t.stop:
		return ErrClosed
	default:
	}
	if m.To == t.cfg.ID {
		return nil
	}
	t.mu.Lock()
	p := t.peers[m.To]
	t.mu.Unlock()
	if p == nil {
		return ErrPeerUnknown
	}
	buf := raft.EncodeMessage(m)
	size := frameHeaderSize + len(buf)
	snap := 0
	if m.Type == raft.MsgSnapshot {
		snap = len(m.Snapshot.Data)
	}
	if size > t.cfg.Limits.MaxFrameBytes {
		return fmt.Errorf("%w: %d > local cap %d", ErrFrameTooLarge, size, t.cfg.Limits.MaxFrameBytes)
	}
	p.mu.Lock()
	if p.haveRemote {
		if size > p.remote.MaxFrameBytes {
			p.mu.Unlock()
			return fmt.Errorf("%w: %d > peer cap %d", ErrFrameTooLarge, size, p.remote.MaxFrameBytes)
		}
		if snap > p.remote.MaxSnapshotBytes {
			p.mu.Unlock()
			return fmt.Errorf("%w: %d > peer snapshot cap %d", ErrSnapshotTooLarge, snap, p.remote.MaxSnapshotBytes)
		}
	}
	if p.inflightM >= t.cfg.Limits.MaxPeerInflightMsgs || p.inflightB+size > t.cfg.Limits.MaxPeerInflightBytes {
		p.mu.Unlock()
		t.stats.sendOverload.Add(1)
		return fmt.Errorf("%w: peer %d (%d bytes, %d msgs in flight)", ErrOverload, m.To, p.inflightB, p.inflightM)
	}
	p.inflightB += size
	p.inflightM++
	p.mu.Unlock()
	select {
	case p.sendCh <- sendItem{buf: buf, size: size, snap: snap}:
		return nil
	default:
		p.release(size)
		t.stats.sendOverload.Add(1)
		return ErrOverload
	}
}

func (p *peer) release(size int) {
	p.mu.Lock()
	p.inflightB -= size
	p.inflightM--
	p.mu.Unlock()
}

func (p *peer) recordErr(err error) {
	p.mu.Lock()
	p.lastErr = err.Error()
	p.mu.Unlock()
}

// Close shuts down the listener, all connections, and all goroutines.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.stopped = true
		for c := range t.conns {
			c.conn.Close()
		}
		t.mu.Unlock()
		close(t.stop)
		t.cancel()
		t.ln.Close()
		t.wg.Wait()
		close(t.messages)
	})
	return nil
}

// track registers a live connection so Close can reach it; it fails after
// shutdown so no connection can outlive Close.
func (t *Transport) track(c *connState) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	t.conns[c] = struct{}{}
	return true
}

func (t *Transport) closeConn(c *connState) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
	c.conn.Close()
}

// newPeerLocked creates peer state and starts its supervisor. Caller holds
// t.mu; creation is refused after shutdown.
func (t *Transport) newPeerLocked(id raft.NodeID, addr string) *peer {
	if t.stopped {
		return nil
	}
	p := &peer{
		t: t, id: id, addr: addr,
		sendCh:  make(chan sendItem, t.cfg.Limits.MaxPeerInflightMsgs),
		inbound: make(chan *connState, 8),
		broken:  make(chan *connState, 64),
	}
	t.peers[id] = p
	t.wg.Add(1)
	go p.run()
	return p
}

// peerFor returns existing peer state or creates inbound-only state for an
// admitted peer (§8: admission is distinct from configured membership).
func (t *Transport) peerFor(id raft.NodeID) *peer {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p := t.peers[id]; p != nil {
		return p
	}
	return t.newPeerLocked(id, "")
}

func (t *Transport) admitted(id raft.NodeID) bool {
	t.mu.Lock()
	_, configured := t.peers[id]
	t.mu.Unlock()
	if configured {
		return true
	}
	return t.cfg.Admit != nil && t.cfg.Admit(id)
}

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			select {
			case <-t.stop:
				return
			default:
				continue
			}
		}
		t.wg.Add(1)
		go t.handleInbound(conn)
	}
}

// handleInbound authenticates and authorizes an inbound connection before any
// protocol message is processed (§8).
func (t *Transport) handleInbound(conn net.Conn) {
	defer t.wg.Done()
	fail := func(err error) {
		t.stats.handshakeRejected.Add(1)
		reason := err.Error()
		if u := errors.Unwrap(err); u != nil {
			reason = strings.TrimPrefix(reason, u.Error()+": ")
		}
		_ = writeFrame(conn, frameReject, encodeReject(rejectCode(err), reason), time.Now().Add(t.cfg.WriteTimeout))
		conn.Close()
	}
	conn.SetDeadline(time.Now().Add(t.cfg.HandshakeTimeout))
	kind, payload, err := readFrame(conn, handshakeCap)
	if err != nil {
		fail(ErrHandshake)
		return
	}
	if kind != frameHandshake {
		fail(ErrHandshake)
		return
	}
	h, err := decodeHandshake(payload)
	if err != nil {
		fail(err)
		return
	}
	if h.cluster != t.cfg.ClusterID {
		fail(fmt.Errorf("%w: peer cluster %d, local %d", ErrClusterMismatch, h.cluster, t.cfg.ClusterID))
		return
	}
	if h.member == t.cfg.ID || !t.admitted(h.member) {
		fail(fmt.Errorf("%w: %d", ErrNotAdmitted, h.member))
		return
	}
	if err := t.bindIdentity(h.member, conn); err != nil {
		fail(err)
		return
	}
	if err := t.cfg.Limits.Compatible(h.limits); err != nil {
		fail(err)
		return
	}
	if err := writeFrame(conn, frameHandshake, t.handshakePayload(), time.Now().Add(t.cfg.WriteTimeout)); err != nil {
		fail(ErrHandshake)
		return
	}
	conn.SetDeadline(time.Time{})
	c := &connState{conn: conn, initiator: h.member, remote: h.limits}
	if !t.track(c) {
		conn.Close()
		return
	}
	p := t.peerFor(h.member)
	if p == nil {
		t.closeConn(c)
		return
	}
	t.stats.handshakesOK.Add(1)
	select {
	case p.inbound <- c:
	case <-t.stop:
		t.closeConn(c)
	}
}

func (t *Transport) handshakePayload() []byte {
	return encodeHandshake(handshake{cluster: t.cfg.ClusterID, member: t.cfg.ID, limits: t.cfg.Limits})
}

// bindIdentity checks the certificate-to-identity binding when configured.
func (t *Transport) bindIdentity(id raft.NodeID, conn net.Conn) error {
	if t.cfg.BindIdentity == nil {
		return nil
	}
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return ErrIdentityMismatch
	}
	certs := tc.ConnectionState().PeerCertificates
	if len(certs) == 0 || !t.cfg.BindIdentity(id, certs[0]) {
		return fmt.Errorf("%w: member %d", ErrIdentityMismatch, id)
	}
	return nil
}

// dial establishes an outbound connection and runs the authenticated
// handshake. Limits are re-negotiated on every reconnect (§8).
func (t *Transport) dial(p *peer) (*connState, error) {
	p.mu.Lock()
	addr := p.addr
	p.mu.Unlock()
	d := &net.Dialer{Timeout: t.cfg.DialTimeout}
	raw, err := d.DialContext(t.ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn := tls.Client(raw, t.clientTLS)
	if err := conn.HandshakeContext(t.ctx); err != nil {
		raw.Close()
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(t.cfg.HandshakeTimeout))
	if err := writeFrame(conn, frameHandshake, t.handshakePayload(), time.Now().Add(t.cfg.WriteTimeout)); err != nil {
		conn.Close()
		return nil, err
	}
	kind, payload, err := readFrame(conn, handshakeCap)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if kind == frameReject {
		conn.Close()
		return nil, decodeReject(payload)
	}
	if kind != frameHandshake {
		conn.Close()
		return nil, ErrHandshake
	}
	h, err := decodeHandshake(payload)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if h.cluster != t.cfg.ClusterID {
		conn.Close()
		return nil, fmt.Errorf("%w: peer cluster %d, local %d", ErrClusterMismatch, h.cluster, t.cfg.ClusterID)
	}
	if h.member != p.id {
		conn.Close()
		return nil, fmt.Errorf("%w: dialed %d, handshake claims %d", ErrIdentityMismatch, p.id, h.member)
	}
	if err := t.bindIdentity(p.id, conn); err != nil {
		conn.Close()
		return nil, err
	}
	if err := t.cfg.Limits.Compatible(h.limits); err != nil {
		_ = writeFrame(conn, frameReject, encodeReject(rejectLimits, err.Error()), time.Now().Add(t.cfg.WriteTimeout))
		conn.Close()
		return nil, err
	}
	conn.SetDeadline(time.Time{})
	c := &connState{conn: conn, initiator: t.cfg.ID, remote: h.limits}
	if !t.track(c) {
		conn.Close()
		return nil, ErrClosed
	}
	t.stats.handshakesOK.Add(1)
	return c, nil
}

// run is the peer supervisor: it owns the current connection, serializes
// writes, adopts inbound connections with a deterministic tie-break, and
// throttles redials. It never blocks the protocol event loop.
func (p *peer) run() {
	defer p.t.wg.Done()
	var cur *connState
	var lastDial time.Time
	closeCur := func() {
		if cur != nil {
			p.t.closeConn(cur)
			cur = nil
			p.mu.Lock()
			p.connected = false
			p.haveRemote = false
			p.mu.Unlock()
		}
	}
	adopt := func(c *connState) {
		cur = c
		p.mu.Lock()
		p.remote = c.remote
		p.haveRemote = true
		p.connected = true
		p.mu.Unlock()
		p.t.wg.Add(1)
		go p.readLoop(c)
	}
	defer func() {
		closeCur()
		for {
			select {
			case c := <-p.inbound:
				p.t.closeConn(c)
			case c := <-p.broken:
				p.t.closeConn(c)
			default:
				return
			}
		}
	}()
	// preferInitiator resolves simultaneous dials: both sides keep the
	// connection initiated by the smaller member ID.
	prefer := p.t.cfg.ID
	if p.id < prefer {
		prefer = p.id
	}
	for {
		select {
		case <-p.t.stop:
			return
		case c := <-p.inbound:
			if cur != nil && cur.initiator == prefer && c.initiator != prefer {
				p.t.closeConn(c) // keep the preferred existing connection
				continue
			}
			closeCur()
			adopt(c)
		case c := <-p.broken:
			if cur == c {
				closeCur()
			} else {
				p.t.closeConn(c)
			}
		case item := <-p.sendCh:
			p.mu.Lock()
			addr := p.addr
			p.mu.Unlock()
			if cur == nil && addr != "" && time.Since(lastDial) >= p.t.cfg.MinRedialInterval {
				lastDial = time.Now()
				c, err := p.t.dial(p)
				if err != nil {
					p.recordErr(err)
					p.t.stats.dialErrors.Add(1)
				} else {
					adopt(c)
				}
			}
			if !p.write(cur, item) {
				closeCur()
			}
			p.release(item.size)
		}
	}
}

// write sends one queued item under the negotiated limits, dropping with a
// typed record when the connection is unavailable or the frame is too large.
// It reports whether the connection remains usable.
func (p *peer) write(cur *connState, item sendItem) bool {
	drop := func(err error) {
		p.recordErr(err)
		p.mu.Lock()
		p.dropped++
		p.mu.Unlock()
		p.t.stats.sendDropped.Add(1)
	}
	if cur == nil {
		// No connection: count the drop but keep the more informative dial
		// error (if any) as the peer's last error.
		p.mu.Lock()
		p.dropped++
		p.mu.Unlock()
		p.t.stats.sendDropped.Add(1)
		return true // nothing to close; a later send may redial
	}
	if item.size > cur.remote.MaxFrameBytes {
		drop(fmt.Errorf("%w: %d > peer cap %d", ErrFrameTooLarge, item.size, cur.remote.MaxFrameBytes))
		return true
	}
	if item.snap > cur.remote.MaxSnapshotBytes {
		drop(fmt.Errorf("%w: %d > peer snapshot cap %d", ErrSnapshotTooLarge, item.snap, cur.remote.MaxSnapshotBytes))
		return true
	}
	if err := writeFrame(cur.conn, frameMessage, item.buf, time.Now().Add(p.t.cfg.WriteTimeout)); err != nil {
		drop(err)
		return false
	}
	p.mu.Lock()
	p.sent++
	p.mu.Unlock()
	return true
}

// readLoop delivers inbound protocol messages under the local decode cap.
// Identity is bound to the authenticated channel: a frame claiming another
// sender closes the connection (§8).
func (p *peer) readLoop(c *connState) {
	defer p.t.wg.Done()
	for {
		kind, payload, err := readFrame(c.conn, p.t.cfg.Limits.MaxFrameBytes)
		if err != nil {
			if err != ErrHandshake {
				p.t.stats.frameDecodeErrors.Add(1)
			}
			p.t.closeConn(c)
			select {
			case p.broken <- c:
			default:
			}
			return
		}
		if kind != frameMessage {
			p.t.stats.frameDecodeErrors.Add(1)
			p.t.closeConn(c)
			select {
			case p.broken <- c:
			default:
			}
			return
		}
		m, err := raft.DecodeMessage(payload, p.t.cfg.Limits.MaxFrameBytes)
		if err != nil {
			p.recordErr(err)
			p.t.stats.frameDecodeErrors.Add(1)
			p.t.closeConn(c)
			select {
			case p.broken <- c:
			default:
			}
			return
		}
		if m.From != p.id || m.To != p.t.cfg.ID {
			p.recordErr(fmt.Errorf("%w: frame claims %d->%d on channel for %d", ErrIdentityMismatch, m.From, m.To, p.id))
			p.t.closeConn(c)
			select {
			case p.broken <- c:
			default:
			}
			return
		}
		select {
		case p.t.messages <- m:
			p.t.stats.messagesDelivered.Add(1)
		default:
			// Bounded inbound queue: drop rather than block; Raft retransmits.
			p.t.stats.messagesDropped.Add(1)
		}
	}
}
