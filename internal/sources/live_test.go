package sources_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

const aggPeer = "agg"

// liveConfig is a validated configuration for the lab: the input table is
// lab_in; prod_in and proxy_in are shared too but not configured, so they
// must be ignored.
func liveConfig(t *testing.T, period time.Duration, srcs ...config.FileSource) config.Config {
	t.Helper()
	d := func(v time.Duration) *config.Duration { c := config.Duration(v); return &c }
	cfg, err := config.File{
		LocalPeer:                    aggPeer,
		InsecurePlaintextLoopbackLab: true,
		Listen:                       "127.0.0.1:0",
		Sources:                      srcs,
		Tables:                       []config.FileTable{{Name: lab.LabTable, Period: d(period)}},
		IdleTimeout:                  d(config.MinIdleTimeout),
		ReconnectMin:                 d(50 * time.Millisecond),
		ReconnectMax:                 d(time.Second),
	}.Validate()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// startLiveLab starts the two-node lab with an aggregator peer whose
// per-node addresses are given.
func startLiveLab(t *testing.T, nodeAddrs map[string]string) *lab.Lab {
	t.Helper()
	return labtest.Start(t, lab.Options{Aggregator: &lab.Aggregator{Name: aggPeer, NodeAddrs: nodeAddrs}})
}

// peerState is HAProxy's view of one remote peer from "show peers".
type peerState struct {
	Active bool
	Status string
	Fields map[string]string
	// Tables maps a table name to its per-peer fields: HAProxy's own
	// table ID (local_id), the ID the aggregator announced for it
	// (remote_id, 0 if none), and the cursor (last_acked, last_pushed,
	// last_get, teaching_origin, update).
	Tables map[string]map[string]string
}

var (
	peerLineRE  = regexp.MustCompile(`^\s*0x[0-9a-f]+: id=([^(]+)\((local|remote),(active|inactive)\)`)
	tableLineRE = regexp.MustCompile(`table:0x[0-9a-f]+ id=(\S+)`)
	kvRE        = regexp.MustCompile(`([a-z_]+)=(\S+)`)
)

// showPeer parses "show peers" for the node's aggregator peer.
func showPeer(t *testing.T, n *lab.Node) peerState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, err := n.Runtime(ctx, "show peers")
	if err != nil {
		t.Fatal(err)
	}
	var st *peerState
	var cursor map[string]string
	for line := range strings.SplitSeq(reply, "\n") {
		if m := peerLineRE.FindStringSubmatch(line); m != nil {
			if st != nil {
				break
			}
			if m[1] != aggPeer {
				continue
			}
			st = &peerState{Active: m[3] == "active", Fields: map[string]string{}, Tables: map[string]map[string]string{}}
		}
		if st == nil {
			continue
		}
		if m := tableLineRE.FindStringSubmatch(line); m != nil && cursor != nil {
			st.Tables[m[1]] = cursor
			cursor = nil
			continue
		}
		kvs := kvRE.FindAllStringSubmatch(line, -1)
		// A shared table is three lines: its IDs (local_id ...
		// remote_data=), its cursor (last_acked=...), and its name.
		if strings.Contains(line, "remote_data=") || strings.Contains(line, "last_acked=") {
			if cursor == nil {
				cursor = map[string]string{}
			}
			for _, kv := range kvs {
				cursor[kv[1]] = kv[2]
			}
			continue
		}
		for _, kv := range kvs {
			if _, dup := st.Fields[kv[1]]; !dup {
				st.Fields[kv[1]] = kv[2]
			}
		}
	}
	if st == nil {
		t.Fatalf("show peers on node %s has no peer %s:\n%s", n.Name, aggPeer, reply)
	}
	st.Status = st.Fields["last_status"]
	return *st
}

func (p peerState) num(t *testing.T, field string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(p.Fields[field], 10, 64)
	if err != nil {
		t.Fatalf("show peers field %s=%q: %v", field, p.Fields[field], err)
	}
	return v
}

// waitPeer polls show peers until pred holds.
func waitPeer(t *testing.T, n *lab.Node, what string, pred func(peerState) bool) peerState {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		st := showPeer(t, n)
		if pred(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s: timed out waiting for %s; last state %+v", n.Name, what, st)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// lastUpdate returns the last EntryUpdated event for source, table, key.
func lastUpdate(evs []sources.Event, source, table string, key peermsg.Key) (peersession.EntryUpdated, bool) {
	var last peersession.EntryUpdated
	found := false
	for _, ev := range evs {
		if u, ok := ev.Body.(peersession.EntryUpdated); ok && ev.Source == source && u.Table == table &&
			u.Update.Key == key {
			last, found = u, true
		}
	}
	return last, found
}

func reqCnt(u peersession.EntryUpdated) uint32 {
	v, _ := u.Update.Value(peermsg.DataHTTPReqCnt)
	return v.Uint
}

func countIs(source string, key peermsg.Key, want uint32) func([]sources.Event) bool {
	return func(evs []sources.Event) bool {
		u, ok := lastUpdate(evs, source, lab.LabTable, key)
		return ok && reqCnt(u) == want
	}
}

func upCount(source string, n int) func([]sources.Event) bool {
	return func(evs []sources.Event) bool { return len(ups(evs, source)) >= n }
}

func mustKey(t *testing.T, s string) peermsg.Key {
	t.Helper()
	k, err := peermsg.KeyFromAddr(netip.MustParseAddr(s))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func sendTraffic(t *testing.T, addr, client string, n int) {
	t.Helper()
	c, err := lab.NewClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseIdle()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := c.SendN(ctx, addr, client, n); err != nil {
		t.Fatalf("traffic to %s: %v", addr, err)
	}
}

// TestLiveSessions runs both connection directions against the two stock
// HAProxy lab nodes: node a dials the daemon, the daemon dials node b. It
// checks the sessions, the resync exchange, input events and their
// acknowledgements, that unconfigured tables are ignored, that
// heartbeat-only traffic keeps quiet sessions alive on both ends, and that
// closing and reopening each session keeps one logical source identity.
func TestLiveSessions(t *testing.T) {
	ln := listen(t)
	l := startLiveLab(t, map[string]string{"a": ln.Addr().String(), "b": closedAddr(t)})
	a, b := l.Node("a"), l.Node("b")
	cfg := liveConfig(t, l.Period,
		config.FileSource{Name: "a"},
		config.FileSource{Name: "b", Address: b.PeersAddr})
	m, r := startManager(t, sources.Options{Config: cfg, Listener: ln})

	evs := r.waitFor("both sessions up and resynced", 20*time.Second, func(evs []sources.Event) bool {
		return count[peersession.SyncFinished](evs, "a") == 1 && count[peersession.SyncFinished](evs, "b") == 1
	})
	for _, src := range []string{"a", "b"} {
		up := ups(evs, src)[0]
		want := map[string]peersession.Direction{"a": peersession.Inbound, "b": peersession.Outbound}[src]
		if b, _ := up.Body.(peersession.SessionUp); b.Direction != want || up.Session != 1 {
			t.Errorf("source %s: first session %d is %v, want session 1 %v", src, up.Session, b.Direction, want)
		}
		if n := count[peersession.TableDefined](evs, src); n != 1 {
			t.Errorf("source %s: %d input table definitions, want 1 (lab_in only)", src, n)
		}
	}
	for _, ev := range evs {
		if d, ok := ev.Body.(peersession.TableDefined); ok && d.Definition.Name != lab.LabTable {
			t.Errorf("event for unconfigured table %s", d.Definition.Name)
		}
		if s, ok := ev.Body.(peersession.SyncFinished); ok {
			t.Logf("source %s resync reply: partial=%v", ev.Source, s.Partial)
		}
	}
	upA, _ := ups(evs, "a")[0].Body.(peersession.SessionUp)
	if upA.RemotePID != uint32(a.PID()) {
		t.Errorf("source a announced PID %d, node a is PID %d", upA.RemotePID, a.PID())
	}
	for _, n := range []*lab.Node{a, b} {
		st := waitPeer(t, n, "established session", func(p peerState) bool { return p.Active && p.Status == "ESTA" })
		t.Logf("node %s sees %s: %v", n.Name, aggPeer, st.Fields)
	}

	// Traffic on the input table and on an unconfigured table.
	keyA, keyB := mustKey(t, "2001:db8:a::"), mustKey(t, "2001:db8:b::")
	sendTraffic(t, a.LabAddr, "2001:db8:a::1", 10)
	sendTraffic(t, b.LabAddr, "2001:db8:b::1", 20)
	sendTraffic(t, a.ProdAddr4, "", 3)
	r.waitFor("lab_in counts 10 and 20", 15*time.Second, func(evs []sources.Event) bool {
		return countIs("a", keyA, 10)(evs) && countIs("b", keyB, 20)(evs)
	})
	waitSkipped(t, m, "a")

	// Acknowledgements: HAProxy's per-peer cursor for lab_in reaches the
	// last update ID the daemon accepted.
	evs = r.snapshot()
	for _, src := range []struct {
		n   *lab.Node
		key peermsg.Key
	}{{a, keyA}, {b, keyB}} {
		u, _ := lastUpdate(evs, src.n.Name, lab.LabTable, src.key)
		want := strconv.FormatUint(uint64(u.Update.ID), 10)
		st := waitPeer(t, src.n, "lab_in acknowledged up to "+want, func(p peerState) bool {
			return p.Tables[lab.LabTable]["update"] == want
		})
		t.Logf("node %s lab_in cursor %v (last accepted update %s)", src.n.Name, st.Tables[lab.LabTable], want)
	}

	// Quiet period: only heartbeats flow for three idle timeouts.
	before := map[string]peersession.Stats{"a": status(t, m, "a").Stats, "b": status(t, m, "b").Stats}
	hbBefore := map[string]peerState{"a": showPeer(t, a), "b": showPeer(t, b)}
	quiet := 3 * cfg.IdleTimeout
	time.Sleep(quiet)
	evs = r.snapshot()
	for _, n := range []*lab.Node{a, b} {
		src := n.Name
		if d := downs(evs, src); len(d) != 0 {
			t.Fatalf("source %s went down during a quiet period: %v", src, d[0].Err)
		}
		s := status(t, m, src)
		if !s.Up || s.Session != 1 {
			t.Fatalf("source %s after quiet period: %+v", src, s)
		}
		rx := s.Stats.RxHeartbeats - before[src].RxHeartbeats
		tx := s.Stats.TxHeartbeats - before[src].TxHeartbeats
		minBeats := uint64(quiet / (2 * cfg.Heartbeat)) // at least one per two intervals, with slack
		if rx < minBeats || tx < minBeats {
			t.Errorf("source %s: %d heartbeats received, %d sent over %v; want at least %d each",
				src, rx, tx, quiet, minBeats)
		}
		if s.Stats.RxMessages-before[src].RxMessages != rx {
			t.Errorf("source %s: non-heartbeat messages during the quiet period", src)
		}
		st := showPeer(t, n)
		if !st.Active || st.Status != "ESTA" || st.num(t, "no_hbt") != 0 ||
			st.num(t, "rx_hbt") <= hbBefore[src].num(t, "rx_hbt") || st.num(t, "tx_hbt") <= hbBefore[src].num(t, "tx_hbt") {
			t.Errorf("node %s after quiet period: %v", src, st.Fields)
		}
		t.Logf("source %s quiet %v: daemon rx %d / tx %d heartbeats; node %s rx_hbt %s tx_hbt %s no_hbt %s",
			src, quiet, rx, tx, src, st.Fields["rx_hbt"], st.Fields["tx_hbt"], st.Fields["no_hbt"])
	}

	// Close and reopen each session: node a redials the daemon, the
	// daemon redials node b. Each comes back as the same source.
	for _, src := range []string{"a", "b"} {
		if !m.Disconnect(src) {
			t.Fatalf("source %s had no session to disconnect", src)
		}
	}
	evs = r.waitFor("second sessions up and resynced", 20*time.Second, func(evs []sources.Event) bool {
		return count[peersession.SyncFinished](evs, "a") == 2 && count[peersession.SyncFinished](evs, "b") == 2
	})
	for _, src := range []string{"a", "b"} {
		u := ups(evs, src)
		if len(u) != 2 || u[1].Session != 2 {
			t.Fatalf("source %s: sessions %v after reopening", src, describe(u))
		}
		if d := downs(evs, src); len(d) != 1 {
			t.Fatalf("source %s: %d SessionDown events, want 1", src, len(d))
		}
	}
	// The resync replays the entries into the new session, and new
	// traffic continues under it.
	sendTraffic(t, a.LabAddr, "2001:db8:a::1", 1)
	sendTraffic(t, b.LabAddr, "2001:db8:b::1", 1)
	evs = r.waitFor("counts 11 and 21 in session 2", 15*time.Second, func(evs []sources.Event) bool {
		return countIs("a", keyA, 11)(evs) && countIs("b", keyB, 21)(evs)
	})
	for _, ev := range evs {
		if u, ok := ev.Body.(peersession.EntryUpdated); ok && reqCnt(u) > 10 && ev.Source == "a" && ev.Session != 2 {
			t.Errorf("update after reopening attributed to session %d", ev.Session)
		}
	}
	checkLifecycle(t, evs)
	for _, n := range []*lab.Node{a, b} {
		st := showPeer(t, n)
		t.Logf("node %s after reopening: %v", n.Name, st.Fields)
	}
}

func waitSkipped(t *testing.T, m *sources.Manager, source string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for status(t, m, source).Stats.SkippedUpdates == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("source %s: no prod_in update was received and skipped", source)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// gateListener holds the reads of every accepted connection until opened,
// and records whether anything was written to them.
type gateListener struct {
	net.Listener
	open     chan struct{}
	accepted chan struct{}
	conns    chan *gateConn
}

type gateConn struct {
	net.Conn
	gate      chan struct{}
	written   chan []byte
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *gateConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func newGate(ln net.Listener) *gateListener {
	return &gateListener{
		Listener: ln, open: make(chan struct{}), accepted: make(chan struct{}, 16),
		conns: make(chan *gateConn, 16),
	}
}

func (g *gateListener) Accept() (net.Conn, error) {
	c, err := g.Listener.Accept()
	if err != nil {
		return nil, err
	}
	gc := &gateConn{Conn: c, gate: g.open, written: make(chan []byte, 16), closed: make(chan struct{})}
	g.conns <- gc
	g.accepted <- struct{}{}
	return gc, nil
}

func (c *gateConn) Read(b []byte) (int, error) {
	<-c.gate
	return c.Conn.Read(b)
}

func (c *gateConn) Write(b []byte) (int, error) {
	c.written <- append([]byte(nil), b...)
	return c.Conn.Write(b)
}

// TestLiveSimultaneous makes node a and the daemon dial each other at
// once. HAProxy's connection is accepted first but held; the daemon's
// outbound session completes, which makes HAProxy drop its own attempt.
// The held connection is then refused as stale without a status line, the
// outbound session survives as the only session, and later
// close-and-reopen cycles with both sides redialing always converge to
// exactly one session.
func TestLiveSimultaneous(t *testing.T) {
	gate := newGate(listen(t))
	l := startLiveLab(t, map[string]string{"a": gate.Addr().String(), "b": closedAddr(t)})
	a := l.Node("a")
	cfg := liveConfig(t, l.Period, config.FileSource{Name: "a", Address: a.PeersAddr})
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		select {
		case <-gate.accepted: // HAProxy's attempt is in, held at the gate
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
	logs := &syncBuffer{}
	m, r := startManager(t, sources.Options{
		Config: cfg, Listener: gate, Dial: dial,
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
	})
	held := <-gate.conns

	evs := r.waitFor("outbound session up", 15*time.Second, upCount("a", 1))
	if d := direction(ups(evs, "a")[0]); d != peersession.Outbound {
		t.Fatalf("first session is %v, want outbound", d)
	}
	st := waitPeer(t, a, "collision recorded", func(p peerState) bool { return p.Active && p.Status == "ESTA" })
	t.Logf("node a after the collision: %v", st.Fields)
	if st.num(t, "coll") < 1 {
		t.Errorf("HAProxy recorded no collision: %v", st.Fields)
	}

	close(gate.open)
	select {
	case <-held.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("stale inbound connection was not closed")
	}
	select {
	case w := <-held.written:
		t.Fatalf("stale inbound connection was answered with %q", w)
	default:
	}
	time.Sleep(2 * time.Second)
	if !strings.Contains(logs.String(), sources.ErrOutboundCurrent.Error()) {
		t.Errorf("held connection was not refused as stale; log:\n%s", logs.String())
	}
	evs = r.snapshot()
	if n := len(ups(evs, "a")); n != 1 || len(downs(evs, "a")) != 0 {
		t.Fatalf("after the collision: %d sessions, %d ends\n%s", n, len(downs(evs, "a")), describe(evs))
	}

	// Both ends redial after each close. Whichever wins, the result is
	// one session that stays up.
	for i := range 5 {
		before := len(ups(evs, "a"))
		lastSession := status(t, m, "a").Session
		if !m.Disconnect("a") {
			t.Fatalf("cycle %d: no session", i)
		}
		r.waitFor(fmt.Sprintf("cycle %d: new session", i), 20*time.Second, upCount("a", before+1))
		// Settle: the survivor must stay up, and HAProxy must agree.
		time.Sleep(time.Second)
		s := status(t, m, "a")
		for !s.Up {
			r.waitFor(fmt.Sprintf("cycle %d: session after churn", i), 20*time.Second,
				upCount("a", len(ups(r.snapshot(), "a"))+1))
			time.Sleep(time.Second)
			s = status(t, m, "a")
		}
		p := waitPeer(t, a, "established", func(p peerState) bool { return p.Active && p.Status == "ESTA" })
		evs = r.snapshot()
		var seq []string
		for _, ev := range evs {
			if ev.Session <= lastSession {
				continue
			}
			switch b := ev.Body.(type) {
			case peersession.SessionUp:
				seq = append(seq, fmt.Sprintf("up %d %v", ev.Session, b.Direction))
			case peersession.SessionDown:
				seq = append(seq, fmt.Sprintf("down %d (%v)", ev.Session, b.Err))
			}
		}
		t.Logf("cycle %d: sessions %v, last survives; node a coll=%s new_conn=%s",
			i, seq, p.Fields["coll"], p.Fields["new_conn"])
		checkLifecycle(t, evs)
	}
}

// TestLiveHandshakeRejects checks identity validation in both directions
// against stock HAProxy. The daemon refuses node a, an unconfigured
// source (504), and node b, which addresses a peer name the daemon does
// not have (503); HAProxy records both statuses. HAProxy in turn refuses
// the daemon's hellos for a wrong peer name (503) and from an unknown
// local name (504), which the daemon records.
func TestLiveHandshakeRejects(t *testing.T) {
	ln1, ln2 := listen(t), listen(t)
	l := startLiveLab(t, map[string]string{"a": ln1.Addr().String(), "b": ln2.Addr().String()})
	a, b := l.Node("a"), l.Node("b")

	cfg1 := liveConfig(t, l.Period, config.FileSource{Name: "b"}, config.FileSource{Name: "wrong", Address: a.PeersAddr})
	m1, r1 := startManager(t, sources.Options{Config: cfg1, Listener: ln1})
	cfg2 := liveConfig(t, l.Period, config.FileSource{Name: "a", Address: a.PeersAddr}, config.FileSource{Name: "b"})
	cfg2.LocalPeer = "other"
	m2, r2 := startManager(t, sources.Options{Config: cfg2, Listener: ln2})

	stA := waitPeer(t, a, "status UNKN", func(p peerState) bool { return p.Status == "UNKN" })
	stB := waitPeer(t, b, "status NAME", func(p peerState) bool { return p.Status == "NAME" })
	t.Logf("node a: %s, node b: %s", stA.Status, stB.Status)
	for _, c := range []struct {
		m      *sources.Manager
		source string
		code   peerwire.StatusCode
	}{{m1, "wrong", peerwire.StatusHostMismatch}, {m2, "a", peerwire.StatusUnknownPeer}} {
		deadline := time.Now().Add(10 * time.Second)
		for {
			var he *peersession.HandshakeError
			err := status(t, c.m, c.source).LastErr
			if errors.As(err, &he) && he.Code == c.code && !he.Sent {
				t.Logf("source %s: %v", c.source, err)
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("source %s: last error %v, want received status %d", c.source, err, c.code)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	for _, r := range []*recorder{r1, r2} {
		if evs := r.snapshot(); len(evs) != 0 {
			t.Fatalf("refused handshakes produced events:\n%s", describe(evs))
		}
	}
}

// TestLiveFaultIsolation runs a stock HAProxy session next to scripted
// sources that send an unknown control and a malformed frame: each faulty
// session ends with a protocol error, and the HAProxy session continues.
func TestLiveFaultIsolation(t *testing.T) {
	ln := listen(t)
	l := startLiveLab(t, map[string]string{"a": ln.Addr().String(), "b": closedAddr(t)})
	a := l.Node("a")
	cfg := liveConfig(t, l.Period, config.FileSource{Name: "a"}, config.FileSource{Name: "f"})
	_, r := startManager(t, sources.Options{Config: cfg, Listener: ln})
	r.waitFor("node a up", 15*time.Second, upCount("a", 1))

	for i, bad := range [][]byte{{0x00, 0x09}, {0x0a, 0x82, 0x01, 0x01}} {
		f := connectFake(t, ln.Addr().String(), "f")
		f.established()
		f.send(bad)
		got := f.expectClose()
		if len(got) != 1 || got[0].Class != peerwire.ClassError || got[0].Type != peerwire.ErrorTypeProtocol {
			t.Fatalf("fault %d: daemon sent %v before closing", i, got)
		}
		r.waitFor(fmt.Sprintf("fault %d: session down", i), 5*time.Second, func(evs []sources.Event) bool {
			return len(downs(evs, "f")) == i+1
		})
		if err := downs(r.snapshot(), "f")[i].Err; !errors.Is(err, peersession.ErrProtocol) {
			t.Fatalf("fault %d ended with %v", i, err)
		}
	}
	sendTraffic(t, a.LabAddr, "2001:db8:a::1", 2)
	evs := r.waitFor("node a updates continue", 10*time.Second, countIs("a", mustKey(t, "2001:db8:a::"), 2))
	if d := downs(evs, "a"); len(d) != 0 {
		t.Fatalf("HAProxy session ended: %v", d[0].Err)
	}
	if st := showPeer(t, a); !st.Active || st.Status != "ESTA" || st.num(t, "proto_err") != 0 {
		t.Fatalf("node a: %v", st.Fields)
	}
}

// TestLiveAbandonedAttempt reproduces a collision seen with stock HAProxy:
// node b starts before the daemon listens, so its connection attempt is
// refused and retried. The daemon's outbound session is accepted
// meanwhile, which makes HAProxy abandon that attempt, yet the retried
// TCP connect still completes later and delivers the abandoned attempt's
// hello. The daemon must refuse it rather than replace its live outbound
// session.
func TestLiveAbandonedAttempt(t *testing.T) {
	// Off 127.0.0.1, so that no connection's ephemeral source port takes
	// the address before the daemon binds it.
	addr := closedAddrOn(t, "127.0.0.2")
	l := startLiveLab(t, map[string]string{"a": closedAddr(t), "b": addr})
	time.Sleep(200 * time.Millisecond) // node b's first attempt is refused
	b := l.Node("b")
	cfg := liveConfig(t, l.Period, config.FileSource{Name: "b", Address: b.PeersAddr})
	cfg.Listen = addr
	logs := &syncBuffer{}
	_, r := startManager(t, sources.Options{Config: cfg, Logger: slog.New(slog.NewTextHandler(logs, nil))})
	r.waitFor("outbound session up", 15*time.Second, upCount("b", 1))
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), sources.ErrOutboundCurrent.Error()) {
		if time.Now().After(deadline) {
			t.Fatalf("no inbound attempt was refused; log:\n%s", logs.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)
	evs := r.snapshot()
	if len(ups(evs, "b")) != 1 || len(downs(evs, "b")) != 0 {
		t.Fatalf("the abandoned attempt disturbed the outbound session:\n%s\n%s", describe(evs), logs.String())
	}
	st := waitPeer(t, b, "established", func(p peerState) bool { return p.Active && p.Status == "ESTA" })
	t.Logf("node b: %v", st.Fields)
	for line := range strings.SplitSeq(logs.String(), "\n") {
		if strings.Contains(line, "refused") {
			t.Log(line)
		}
	}
}
