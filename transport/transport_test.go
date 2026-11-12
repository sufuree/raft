package transport

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	raft "github.com/sufuree/raft"
	"github.com/sufuree/raft/driver"
)

// ---- test PKI ----

type testPKI struct {
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	pool   *x509.CertPool
}

func newPKI(t *testing.T) *testPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "raft-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &testPKI{caCert: ca, caKey: key, pool: pool}
}

func (p *testPKI) tlsConfig(t *testing.T, id raft.NodeID) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(int64(id) + 100),
		Subject:      pkix.Name{CommonName: fmt.Sprintf("node-%d", id)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		RootCAs:      p.pool,
		ClientCAs:    p.pool,
		ServerName:   "localhost",
	}
}

// ---- helpers ----

const testCluster = uint64(0xC1057E)

func newTransport(t *testing.T, pki *testPKI, id raft.NodeID, peers map[raft.NodeID]string, limits Limits) *Transport {
	t.Helper()
	tr, err := New(Config{
		ID: id, ClusterID: testCluster, Listen: "127.0.0.1:0",
		Peers: peers, TLS: pki.tlsConfig(t, id), Limits: limits,
		MinRedialInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

// newAdmittingTransport admits any peer, for adversarial handshake tests that
// must reach the version/limits/identity checks past admission.
func newAdmittingTransport(t *testing.T, pki *testPKI, id raft.NodeID, limits Limits) *Transport {
	t.Helper()
	tr, err := New(Config{
		ID: id, ClusterID: testCluster, Listen: "127.0.0.1:0",
		TLS: pki.tlsConfig(t, id), Limits: limits,
		Admit:             func(other raft.NodeID) bool { return other != id },
		MinRedialInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr
}

func waitPeerConnected(t *testing.T, tr *Transport, id raft.NodeID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := tr.PeerStats(id); ok && s.Connected {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	s, _ := tr.PeerStats(id)
	t.Fatalf("peer %d not connected; last error: %s", id, s.LastError)
}

func waitMessage(t *testing.T, tr *Transport) raft.Message {
	t.Helper()
	select {
	case m := <-tr.Messages():
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
		return raft.Message{}
	}
}

// rawPeer is a manually controlled TLS endpoint for adversarial handshakes.
type rawPeer struct {
	conn *tls.Conn
}

func dialRaw(t *testing.T, pki *testPKI, id raft.NodeID, addr string) *rawPeer {
	t.Helper()
	cfg := pki.tlsConfig(t, id)
	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	return &rawPeer{conn: conn}
}

func (r *rawPeer) sendHandshake(cluster uint64, member raft.NodeID, limits Limits) {
	limits.setDefaults()
	writeFrame(r.conn, frameHandshake, encodeHandshake(handshake{cluster: cluster, member: member, limits: limits}), time.Now().Add(5*time.Second))
}

func (r *rawPeer) readReject() error {
	kind, payload, err := readFrame(r.conn, handshakeCap)
	if err != nil {
		return err
	}
	if kind != frameReject {
		return fmt.Errorf("expected reject frame, got kind %d", kind)
	}
	return decodeReject(payload)
}

// ---- tests ----

// TestMessageExchange verifies authenticated bidirectional delivery.
func TestMessageExchange(t *testing.T) {
	pki := newPKI(t)
	a := newTransport(t, pki, 1, nil, Limits{})
	b := newTransport(t, pki, 2, nil, Limits{})
	a.AddPeer(2, b.Addr().String())
	b.AddPeer(1, a.Addr().String())

	m := raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 3, Commit: 9}
	if err := a.Send(m); err != nil {
		t.Fatal(err)
	}
	got := waitMessage(t, b)
	if got.Type != raft.MsgHeartbeat || got.From != 1 || got.To != 2 || got.Term != 3 || got.Commit != 9 {
		t.Fatalf("mismatch: %+v", got)
	}
	waitPeerConnected(t, a, 2)
	if err := b.Send(raft.Message{Type: raft.MsgHeartbeatResp, From: 2, To: 1, Term: 3}); err != nil {
		t.Fatal(err)
	}
	got = waitMessage(t, a)
	if got.Type != raft.MsgHeartbeatResp || got.From != 2 {
		t.Fatalf("mismatch: %+v", got)
	}
}

// TestOversizedFrameRejected: a length claim beyond the decode cap must close
// the connection without allocating (§8).
func TestOversizedFrameRejected(t *testing.T) {
	pki := newPKI(t)
	srv := newAdmittingTransport(t, pki, 1, Limits{})
	raw := dialRaw(t, pki, 2, srv.Addr().String())
	raw.sendHandshake(testCluster, 2, Limits{})
	// Server writes its handshake in response.
	if kind, _, err := readFrame(raw.conn, handshakeCap); err != nil || kind != frameHandshake {
		t.Fatalf("handshake failed: kind=%d err=%v", kind, err)
	}
	// Claim a ~4 GiB frame; send nothing else.
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], 0xFFFFFFF0)
	raw.conn.Write(hdr[:])
	// The server must close the connection.
	raw.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := raw.conn.Read(buf); err == nil {
		t.Fatal("expected connection close after oversized claim")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.Stats().FrameDecodeErrors > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("oversized frame not counted as decode error")
}

// TestIncompatibleLimitsRejected: a peer whose batch exceeds our decode cap
// is rejected at the handshake with a typed error (§8).
func TestIncompatibleLimitsRejected(t *testing.T) {
	pki := newPKI(t)
	srv := newAdmittingTransport(t, pki, 1, Limits{MaxFrameBytes: 4 << 20, MaxBatchBytes: 2 << 20, MaxSnapshotBytes: 1 << 20})
	raw := dialRaw(t, pki, 2, srv.Addr().String())
	big := Limits{MaxCommandBytes: 1 << 20, MaxBatchBytes: 32 << 20, MaxFrameBytes: 64 << 20, MaxSnapshotBytes: 8 << 20}
	raw.sendHandshake(testCluster, 2, big)
	err := raw.readReject()
	if !errors.Is(err, ErrIncompatibleLimits) {
		t.Fatalf("want ErrIncompatibleLimits, got %v", err)
	}
	if srv.Stats().HandshakeRejected == 0 {
		t.Fatal("rejection not counted")
	}
}

// TestDialerRejectsIncompatibleLimits: the dialer also validates the peer's
// advertised limits (symmetric chain) and refuses the connection.
func TestDialerRejectsIncompatibleLimits(t *testing.T) {
	pki := newPKI(t)
	// Raw server advertising a batch larger than the client's decode cap.
	srvTLS := pki.tlsConfig(t, 2)
	srvTLS.ClientAuth = tls.RequireAndVerifyClientCert
	srvTLS.NextProtos = []string{alpn}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		readFrame(conn, handshakeCap)
		big := Limits{MaxCommandBytes: 1 << 20, MaxBatchBytes: 32 << 20, MaxFrameBytes: 64 << 20, MaxSnapshotBytes: 8 << 20}
		writeFrame(conn, frameHandshake, encodeHandshake(handshake{cluster: testCluster, member: 2, limits: big}), time.Now().Add(5*time.Second))
		// Hold until the client closes.
		buf := make([]byte, 1)
		conn.Read(buf)
	}()

	cli := newTransport(t, pki, 1, map[raft.NodeID]string{2: ln.Addr().String()},
		Limits{MaxFrameBytes: 4 << 20, MaxBatchBytes: 2 << 20, MaxSnapshotBytes: 1 << 20})
	cli.Send(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s, ok := cli.PeerStats(2); ok && strings.Contains(s.LastError, "incompatible") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	s, _ := cli.PeerStats(2)
	t.Fatalf("dialer did not reject incompatible limits; last error: %q", s.LastError)
}

// TestClusterMismatchRejected: a peer from another cluster incarnation never
// participates (§9).
func TestClusterMismatchRejected(t *testing.T) {
	pki := newPKI(t)
	srv := newTransport(t, pki, 1, nil, Limits{})
	raw := dialRaw(t, pki, 2, srv.Addr().String())
	raw.sendHandshake(testCluster+1, 2, Limits{})
	if err := raw.readReject(); !errors.Is(err, ErrClusterMismatch) {
		t.Fatalf("want ErrClusterMismatch, got %v", err)
	}
}

// TestWireVersionRejected: incompatible wire versions are refused before
// protocol participation (§8).
func TestWireVersionRejected(t *testing.T) {
	pki := newPKI(t)
	srv := newTransport(t, pki, 1, nil, Limits{})
	raw := dialRaw(t, pki, 2, srv.Addr().String())
	limits := Limits{}
	limits.setDefaults()
	payload := encodeHandshake(handshake{cluster: testCluster, member: 2, limits: limits})
	payload[4] = raft.WireVersion + 1
	writeFrame(raw.conn, frameHandshake, payload, time.Now().Add(5*time.Second))
	if err := raw.readReject(); !errors.Is(err, raft.ErrWireVersion) {
		t.Fatalf("want ErrWireVersion, got %v", err)
	}
}

// TestAdmissionDistinctFromMembership: an unconfigured peer is refused unless
// explicitly admitted; admitted learners are accepted (§8).
func TestAdmissionDistinctFromMembership(t *testing.T) {
	pki := newPKI(t)
	strict := newTransport(t, pki, 1, nil, Limits{})
	raw := dialRaw(t, pki, 3, strict.Addr().String())
	raw.sendHandshake(testCluster, 3, Limits{})
	if err := raw.readReject(); !errors.Is(err, ErrNotAdmitted) {
		t.Fatalf("want ErrNotAdmitted, got %v", err)
	}

	// A transport that admits learner 3 without configuring it as a peer.
	learner, err := New(Config{
		ID: 1, ClusterID: testCluster, Listen: "127.0.0.1:0",
		TLS: pki.tlsConfig(t, 1),
		Admit: func(id raft.NodeID) bool {
			return id == 3
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer learner.Close()
	raw2 := dialRaw(t, pki, 3, learner.Addr().String())
	raw2.sendHandshake(testCluster, 3, Limits{})
	kind, _, err := readFrame(raw2.conn, handshakeCap)
	if err != nil || kind != frameHandshake {
		t.Fatalf("admitted learner rejected: kind=%d err=%v", kind, err)
	}
	// The learner can now send protocol messages.
	m := raft.Message{Type: raft.MsgAppendResp, From: 3, To: 1, Term: 2, MatchIndex: 5}
	writeFrame(raw2.conn, frameMessage, raft.EncodeMessage(m), time.Now().Add(5*time.Second))
	got := waitMessage(t, learner)
	if got.From != 3 || got.MatchIndex != 5 {
		t.Fatalf("learner message mismatch: %+v", got)
	}
}

// TestIdentityBindingRejected: a valid certificate that does not bind the
// claimed member ID is refused (§8).
func TestIdentityBindingRejected(t *testing.T) {
	pki := newPKI(t)
	srv, err := New(Config{
		ID: 1, ClusterID: testCluster, Listen: "127.0.0.1:0",
		TLS:   pki.tlsConfig(t, 1),
		Admit: func(other raft.NodeID) bool { return other != 1 },
		BindIdentity: func(id raft.NodeID, cert *x509.Certificate) bool {
			return cert.Subject.CommonName == fmt.Sprintf("node-%d", id)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	// Certificate says node-2 but handshake claims node-9.
	raw := dialRaw(t, pki, 2, srv.Addr().String())
	raw.sendHandshake(testCluster, 9, Limits{})
	if err := raw.readReject(); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("want ErrIdentityMismatch, got %v", err)
	}
}

// TestFrameIdentityMismatch: an authenticated channel may not carry messages
// claiming a different sender.
func TestFrameIdentityMismatch(t *testing.T) {
	pki := newPKI(t)
	srv := newAdmittingTransport(t, pki, 1, Limits{})
	raw := dialRaw(t, pki, 2, srv.Addr().String())
	raw.sendHandshake(testCluster, 2, Limits{})
	if kind, _, err := readFrame(raw.conn, handshakeCap); err != nil || kind != frameHandshake {
		t.Fatalf("handshake failed: kind=%d err=%v", kind, err)
	}
	m := raft.Message{Type: raft.MsgHeartbeat, From: 99, To: 1, Term: 1}
	writeFrame(raw.conn, frameMessage, raft.EncodeMessage(m), time.Now().Add(5*time.Second))
	raw.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := raw.conn.Read(buf); err == nil {
		t.Fatal("expected close for identity-mismatched frame")
	}
}

// TestPeerByteBudget: a burst to a stalled peer stays within the byte budget
// and returns typed overload (§8).
func TestPeerByteBudget(t *testing.T) {
	pki := newPKI(t)
	// Black-hole server: completes the handshake, then never reads.
	srvTLS := pki.tlsConfig(t, 2)
	srvTLS.ClientAuth = tls.RequireAndVerifyClientCert
	srvTLS.NextProtos = []string{alpn}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srvTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				readFrame(conn, handshakeCap)
				writeFrame(conn, frameHandshake, encodeHandshake(handshake{cluster: testCluster, member: 2, limits: Limits{}}), time.Now().Add(5*time.Second))
				conn.SetDeadline(time.Time{})
				// Never read: the client's writes stall once buffers fill.
				time.Sleep(30 * time.Second)
			}()
		}
	}()

	const budget = 1 << 20
	cli := newTransport(t, pki, 1, map[raft.NodeID]string{2: ln.Addr().String()},
		Limits{MaxPeerInflightBytes: budget, MaxPeerInflightMsgs: 1024})
	payload := bytes.Repeat([]byte("x"), 256<<10)
	overloaded := false
	for i := 0; i < 200; i++ {
		err := cli.Send(raft.Message{Type: raft.MsgAppend, From: 1, To: 2, Term: 1,
			Entries: []raft.Entry{{Term: 1, Index: raft.Index(i + 1), Data: payload}}})
		if errors.Is(err, ErrOverload) {
			overloaded = true
			break
		}
		if err != nil {
			t.Fatalf("unexpected send error: %v", err)
		}
		s, _ := cli.PeerStats(2)
		if s.InflightBytes > budget {
			t.Fatalf("inflight %d exceeds budget %d", s.InflightBytes, budget)
		}
	}
	if !overloaded {
		t.Fatal("burst never hit the byte budget")
	}
	if cli.Stats().SendOverload == 0 {
		t.Fatal("overload not counted")
	}
	s, _ := cli.PeerStats(2)
	if s.InflightBytes > budget || s.InflightMsgs > 1024 {
		t.Fatalf("budget exceeded at overload: %+v", s)
	}
}

// TestReconnectRenegotiatesLimits: a peer restarted with incompatible lower
// limits is rejected at the reconnect handshake (§8 acceptance gate).
func TestReconnectRenegotiatesLimits(t *testing.T) {
	pki := newPKI(t)
	a := newTransport(t, pki, 1, nil, Limits{})
	b := newTransport(t, pki, 2, nil, Limits{})
	a.AddPeer(2, b.Addr().String())
	b.AddPeer(1, a.Addr().String())
	if err := a.Send(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1}); err != nil {
		t.Fatal(err)
	}
	waitMessage(t, b)
	waitPeerConnected(t, a, 2)

	// Restart node 2 with a decode cap below node 1's batch size.
	addr := b.Addr().String()
	b.Close()
	small := Limits{MaxCommandBytes: 64 << 10, MaxBatchBytes: 128 << 10, MaxFrameBytes: 256 << 10, MaxSnapshotBytes: 128 << 10}
	b2, err := New(Config{
		ID: 2, ClusterID: testCluster, Listen: addr,
		Peers: map[raft.NodeID]string{1: a.Addr().String()},
		TLS:   pki.tlsConfig(t, 2), Limits: small,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()

	// Force a reconnect: the old connection died with b.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		a.Send(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1})
		if s, ok := a.PeerStats(2); ok && strings.Contains(s.LastError, "incompatible") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s, _ := a.PeerStats(2)
	t.Fatalf("reconnect with incompatible limits not rejected; last error: %q", s.LastError)
}

// TestSimultaneousDial: both sides dialing at once converge to one usable
// connection per direction pair.
func TestSimultaneousDial(t *testing.T) {
	pki := newPKI(t)
	a := newTransport(t, pki, 1, nil, Limits{})
	b := newTransport(t, pki, 2, nil, Limits{})
	a.AddPeer(2, b.Addr().String())
	b.AddPeer(1, a.Addr().String())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			a.Send(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: raft.Term(i + 1)})
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			b.Send(raft.Message{Type: raft.MsgHeartbeat, From: 2, To: 1, Term: raft.Term(i + 1)})
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	// At least one message must flow each way after convergence.
	gotB := waitMessage(t, b)
	gotA := waitMessage(t, a)
	if gotB.From != 1 || gotA.From != 2 {
		t.Fatalf("unexpected messages: %+v %+v", gotA, gotB)
	}
}

// ---- full driver integration over TCP+mTLS ----

type tlsFSM struct {
	mu      sync.Mutex
	applied raft.Index
	cmds    map[raft.Index][]byte
}

func (f *tlsFSM) Apply(entries []raft.Entry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range entries {
		if e.Type == raft.EntryNormal {
			f.cmds[e.Index] = append([]byte(nil), e.Data...)
		}
		if e.Index > f.applied {
			f.applied = e.Index
		}
	}
	return nil
}
func (f *tlsFSM) Snapshot() ([]byte, error) { return nil, nil }
func (f *tlsFSM) Restore(s raft.SnapshotMeta, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = s.Index
	return nil
}

type tlsNode struct {
	d   *driver.Driver
	tr  *Transport
	fsm *tlsFSM
}

func startTLSCluster(t *testing.T, n int) []*tlsNode {
	t.Helper()
	pki := newPKI(t)
	nodes := make([]*tlsNode, n)
	voters := make(raft.NodeSet, n)
	for i := range voters {
		voters[i] = raft.NodeID(i + 1)
	}
	for i := 0; i < n; i++ {
		id := raft.NodeID(i + 1)
		tr := newTransport(t, pki, id, nil, Limits{})
		nodes[i] = &tlsNode{tr: tr, fsm: &tlsFSM{cmds: make(map[raft.Index][]byte)}}
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				nodes[i].tr.AddPeer(raft.NodeID(j+1), nodes[j].tr.Addr().String())
			}
		}
	}
	for i := 0; i < n; i++ {
		id := raft.NodeID(i + 1)
		store := raft.NewMemStorage()
		store.SetInitialConfig(raft.Configuration{Voters: voters})
		d, err := driver.New(driver.Config{
			ID: id, ElectionTick: 10, HeartbeatTick: 1, PreVote: true, CheckQuorum: true,
			Seed: int64(id), TickInterval: 5 * time.Millisecond,
		}, store, nodes[i].fsm, nodes[i].tr)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i].d = d
		d.Start()
		t.Cleanup(func() { d.Stop() })
	}
	return nodes
}

func waitLeader(t *testing.T, nodes []*tlsNode) *tlsNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.d.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected over TCP+mTLS")
	return nil
}

// TestTLSClusterCommitAndRead: a real three-node cluster over TCP+mTLS elects
// a leader, commits commands, and serves a linearizable read barrier.
func TestTLSClusterCommitAndRead(t *testing.T) {
	nodes := startTLSCluster(t, 3)
	leader := waitLeader(t, nodes)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		res, err := leader.d.Propose(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
		if err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
		if res.Outcome != raft.OutcomeApplied {
			t.Fatalf("propose %d outcome %v", i, res.Outcome)
		}
	}
	idx, err := leader.d.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if idx < 11 { // noop + 10 commands
		t.Fatalf("read index %d too low", idx)
	}
	// Every follower applies the committed prefix.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, n := range nodes {
			_, _, _, _, ap := n.d.Watermarks()
			if ap < 11 {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("followers did not apply the committed prefix")
}

// TestLaggingFollowerCannotStallMajority: a stopped minority member does not
// prevent the healthy majority from committing (§8, §11 progress gate).
func TestLaggingFollowerCannotStallMajority(t *testing.T) {
	nodes := startTLSCluster(t, 3)
	leader := waitLeader(t, nodes)
	// Stop one follower's driver; its transport keeps running but the node
	// makes no protocol progress.
	var lagging *tlsNode
	for _, n := range nodes {
		if n != leader {
			lagging = n
			break
		}
	}
	lagging.d.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < 30; i++ {
		res, err := leader.d.Propose(ctx, []byte(fmt.Sprintf("lag-%d", i)))
		if err != nil {
			t.Fatalf("propose %d with lagging follower: %v", i, err)
		}
		if res.Outcome != raft.OutcomeApplied {
			t.Fatalf("propose %d outcome %v", i, res.Outcome)
		}
	}
}
