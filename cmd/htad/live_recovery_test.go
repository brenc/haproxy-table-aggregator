package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/aggregate"
	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// applyLog records the events a daemon's store applied, with the local
// time each was applied.
type applyLog struct {
	mu     sync.Mutex
	events []appliedEvent
}

type appliedEvent struct {
	ev  sources.Event
	err error
	at  time.Time
}

func (l *applyLog) applied(ev sources.Event, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, appliedEvent{ev: ev, err: err, at: time.Now()})
}

func (l *applyLog) snapshot() []appliedEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]appliedEvent(nil), l.events...)
}

// teachStats summarizes one source's first session in the log: when its
// finished resync reply was applied, and how many accepted updates came
// before it as timed records (a teach) and as ordinary ones (live
// pushes).
type teachStats struct {
	session       uint64
	finished      time.Time
	timed, normal int
}

func (l *applyLog) firstSession(source string) teachStats {
	var s teachStats
	for _, a := range l.snapshot() {
		if a.ev.Source != source || a.err != nil {
			continue
		}
		if s.session == 0 {
			if _, ok := a.ev.Body.(peersession.SessionUp); ok {
				s.session = a.ev.Session
			}
			continue
		}
		if a.ev.Session != s.session || !s.finished.IsZero() {
			continue
		}
		switch b := a.ev.Body.(type) {
		case peersession.EntryUpdated:
			if b.Update.Timed {
				s.timed++
			} else {
				s.normal++
			}
		case peersession.SyncFinished:
			if !b.Partial {
				s.finished = a.at
			}
		}
	}
	return s
}

// leaseWatch records when a daemon's publisher first granted a lease.
type leaseWatch struct {
	first atomic.Pointer[time.Time] // wall clock the first lease certified
	stop  chan struct{}
	done  chan struct{}
}

func watchLease(d *daemon) *leaseWatch {
	w := &leaseWatch{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			if st := d.pub.Stats(); st.Leases > 0 && w.first.Load() == nil {
				at := st.LastLease
				w.first.Store(&at)
			}
			select {
			case <-w.stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	return w
}

func (w *leaseWatch) end() *time.Time {
	close(w.stop)
	<-w.done
	return w.first.Load()
}

// liveMarker reports whether a probe saw a live lease marker.
func liveMarker(p lab.ProbeResponse) bool {
	return p.MetaVersion == output.SchemaVersion && p.MetaGen != 0 && p.LeaseLeft <= output.LeaseWindowMillis
}

// sendTo sends n requests for client to node n's lab listener, all 200.
func (e *e2e) sendTo(n *lab.Node, client string, count int) {
	e.t.Helper()
	for range count {
		if s := e.send(n, client); s.r.Status != http.StatusOK {
			e.t.Fatalf("node %s: %+v", n.Name, s.r)
		}
	}
}

// matchTables waits until the daemon's store holds, for each node,
// exactly the node's live lab_in table: the same keys with the same
// counts, one entry per key, nothing else.
func (e *e2e) matchTables(what string) {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		diff := e.tableDiff()
		if diff == "" {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("%s: store does not match HAProxy: %s", what, diff)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *e2e) tableDiff() string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, n := range []*lab.Node{e.a, e.b} {
		tb, err := n.ShowTable(ctx, lab.LabTable)
		if err != nil {
			e.t.Fatal(err)
		}
		rep, _ := e.d.snap.Source(n.Name)
		if rep.Entries != len(tb.Entries) {
			return fmt.Sprintf("node %s: store %d entries, HAProxy %d", n.Name, rep.Entries, len(tb.Entries))
		}
		for _, en := range tb.Entries {
			k, err := peermsg.KeyFromAddr(netip.MustParseAddr(en.Key))
			if err != nil {
				e.t.Fatal(err)
			}
			got, ok := e.d.snap.Lookup(n.Name, lab.LabTable, k)
			if !ok || int64(got.Count) != en.Data["http_req_cnt"] {
				return fmt.Sprintf("node %s key %s: store %d (present %v), HAProxy %d", n.Name, en.Key, got.Count, ok,
					en.Data["http_req_cnt"])
			}
		}
	}
	return ""
}

// preload creates n keys in node's lab_in through the runtime API, with
// counts 1..40, so that a teach carries more than the test's traffic.
func preload(t *testing.T, node *lab.Node, prefix uint16, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range n {
		cmd := fmt.Sprintf("set table %s key 2001:db8:%x:%x:: data.http_req_cnt %d", lab.LabTable, prefix, i, i%40+1)
		if _, err := node.Runtime(ctx, cmd); err != nil {
			t.Fatal(err)
		}
	}
}

// waitSource polls the daemon's store until pred holds for src.
func (e *e2e) waitSource(src, what string, timeout time.Duration, pred func(snapshot.SourceReport) bool,
) snapshot.SourceReport {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		r, _ := e.d.snap.Source(src)
		if pred(r) {
			return r
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("source %s: no %s within %v; %+v", src, what, timeout, r)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// responderTotal is the lab traffic for client's key that reached the
// responder through the lab listeners of both nodes.
func (e *e2e) responderTotal(client string) int {
	k := key(e.t, client).String()
	return e.l.Responder.Count(lab.Observation{Node: "a", Listener: "lab", Key: k}) +
		e.l.Responder.Count(lab.Observation{Node: "b", Listener: "lab", Key: k})
}

// TestLiveRestartRecovery restarts the daemon with empty memory while
// both HAProxy nodes keep their input tables and traffic continues: the
// new daemon rebuilds every retained entry from the teaches without
// adding replayed values, grants no authority while one required source
// is still missing, and certifies its first lease only after every
// source's finished resync.
func TestLiveRestartRecovery(t *testing.T) {
	var g gate
	var dialer net.Dialer
	h := hooks{dial: func(ctx context.Context, address string) (net.Conn, error) {
		if g.refuses(address) {
			return nil, fmt.Errorf("test gate closed for %s", address)
		}
		return dialer.DialContext(ctx, "tcp", address)
	}}
	e := startE2E(t, h)
	e.waitLeased()
	const preloaded = 1000
	preload(t, e.a, 0xa000, preloaded)
	preload(t, e.b, 0xb000, preloaded)
	const client = "2001:db8:c1::1"
	e.sendTo(e.a, client, 6)
	e.sendTo(e.b, client, 4)
	e.matchTables("before the restart")

	stopped := time.Now()
	e.stop()
	t.Logf("daemon stopped in %v", time.Since(stopped).Round(time.Millisecond))
	// Requests while no daemon runs: counted by HAProxy only.
	e.sendTo(e.a, client, 3)
	e.sendTo(e.b, client, 2)

	// Restart with empty memory; node b stays unreachable for 3 s.
	g.set(e.b.PeersAddr, true)
	log := &applyLog{}
	h.applied = log.applied
	restarted := time.Now()
	e.launch(h)
	lw := watchLease(e.d)
	stop := make(chan struct{})
	paced := make(chan []sample, 1)
	go func() {
		// 3 requests per second per node: below every limit.
		paced <- e.pace([]*lab.Node{e.a, e.b}, client, time.Second/3, stop)
	}()
	e.waitSource("a", "ready", 10*time.Second, func(r snapshot.SourceReport) bool { return r.State == snapshot.Ready })
	aReady := time.Now()
	for time.Since(aReady) < 3*time.Second {
		st := e.d.pub.Stats()
		r := e.d.snap.Roster()
		if st.Leases != 0 || st.Leased || r.Ready {
			t.Fatalf("authority with b missing: publisher %+v roster ready %v", st, r.Ready)
		}
		for _, n := range []*lab.Node{e.a, e.b} {
			if p := e.probe(n, client); liveMarker(p) || p.Aggregate() {
				t.Fatalf("node %s saw authority with b missing: %+v", n.Name, p)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	g.set(e.b.PeersAddr, false)
	e.waitLeased()
	leased := time.Now()
	close(stop)
	samples := <-paced
	first := lw.end()
	e.matchTables("after the restart")

	sa, sb := log.firstSession("a"), log.firstSession("b")
	if first == nil || sa.finished.IsZero() || sb.finished.IsZero() {
		t.Fatalf("first lease %v, finished replies a %v b %v", first, sa.finished, sb.finished)
	}
	if first.Before(sa.finished) || first.Before(sb.finished) {
		t.Errorf("first lease certified data at %v, before a finished %v / b finished %v", first,
			sa.finished.Sub(*first), sb.finished.Sub(*first))
	}
	for _, s := range []teachStats{sa, sb} {
		if s.timed < preloaded+1 {
			t.Errorf("teach of %d timed records, want at least %d", s.timed, preloaded+1)
		}
	}
	tot, err := aggregate.Count(e.d.snap, lab.LabTable, key(t, client))
	if err != nil {
		t.Fatal(err)
	}
	want := e.responderTotal(client)
	if !tot.Complete || tot.Uncertain || tot.Sum != uint64(want) {
		t.Errorf("total %+v, responder %d", tot, want)
	}
	for _, c := range tot.Sources {
		if c.HeldOver || c.Session != 1 {
			t.Errorf("contribution %+v", c)
		}
	}
	for _, src := range []string{"a", "b"} {
		if r, _ := e.d.snap.Source(src); r.Released != 0 || r.Loss.Absent+r.Loss.Recreated != 0 {
			t.Errorf("restart reported lost history for %s: %+v", src, r)
		}
	}
	t.Logf("restart: a ready %v after launch; b gated 3 s; first lease certified %v after b's finished reply, "+
		"authority on both nodes %v after launch; teaches a %d timed + %d live, b %d timed + %d live before "+
		"finished; %d paced requests during recovery; total %d = responder %d",
		aReady.Sub(restarted).Round(time.Millisecond), first.Sub(sb.finished).Round(time.Millisecond),
		leased.Sub(restarted).Round(time.Millisecond), sa.timed, sa.normal, sb.timed, sb.normal, len(samples),
		tot.Sum, want)
}

// TestLiveRestartExpiry checks that a teach after a restart carries the
// remaining lifetime, so an entry keeps its source's deadline rather
// than getting a new full lifetime, and that once expired it does not
// reappear from a later resync or restart.
func TestLiveRestartExpiry(t *testing.T) {
	log := &applyLog{}
	e := startE2E(t, hooks{})
	e.waitLeased()
	const client = "2001:db8:e1::1"
	k := key(t, client)
	e.sendTo(e.a, client, 1)
	var d1 time.Time
	e.waitSource("a", "the key", 5*time.Second, func(snapshot.SourceReport) bool {
		en, ok := e.d.snap.Lookup("a", lab.LabTable, k)
		d1 = en.Deadline
		return ok
	})
	time.Sleep(12 * time.Second)
	e.stop()
	e.launch(hooks{applied: log.applied})
	e.waitLeased()
	restarted := time.Now()
	en, ok := e.d.snap.Lookup("a", lab.LabTable, k)
	if !ok {
		t.Fatal("retained key not recovered")
	}
	var timed bool
	var remaining peermsg.Millis
	for _, a := range log.snapshot() {
		if u, isUpd := a.ev.Body.(peersession.EntryUpdated); isUpd && a.ev.Source == "a" && u.Update.Key == k {
			timed, remaining = u.Update.Timed, u.Update.Remaining
		}
	}
	hap, ok := tableEntry(t, e.a, lab.LabTable, k)
	if !ok {
		t.Fatal("HAProxy lost the key")
	}
	shift := en.Deadline.Sub(d1)
	if !timed || shift < -250*time.Millisecond || shift > 250*time.Millisecond {
		t.Errorf("replayed entry: timed %v, deadline moved %v (remaining %v)", timed, shift, remaining)
	}
	if left := time.Until(en.Deadline); left > 19*time.Second || (left-hap.Exp).Abs() > 250*time.Millisecond {
		t.Errorf("replayed entry has %v left; HAProxy %v", left, hap.Exp)
	}
	t.Logf("teach after restart: timed record, %v remaining; deadline moved %v; HAProxy exp %v; daemon %v",
		remaining, shift.Round(time.Millisecond), hap.Exp, time.Until(en.Deadline).Round(time.Millisecond))

	// After the deadline the key is gone here and at the source, and
	// neither a resync nor another restart brings it back.
	time.Sleep(time.Until(en.Deadline) + 500*time.Millisecond)
	if _, ok := e.d.snap.Lookup("a", lab.LabTable, k); ok {
		t.Fatal("key outlived its deadline")
	}
	if _, ok := tableEntry(t, e.a, lab.LabTable, k); ok {
		t.Fatal("HAProxy still holds the key")
	}
	session := status(t, e, "a").Session
	if !e.d.m.Disconnect("a") {
		t.Fatal("a had no session")
	}
	e.waitSource("a", "resynchronized", 10*time.Second, func(r snapshot.SourceReport) bool {
		return r.Session > session && r.State == snapshot.Ready
	})
	e.stop()
	e.launch(hooks{})
	e.waitLeased()
	if _, ok := e.d.snap.Lookup("a", lab.LabTable, k); ok {
		t.Fatal("expired key resurrected")
	}
	t.Logf("expired %v after the restart; absent after a resync and another restart",
		en.Deadline.Sub(restarted).Round(time.Millisecond))
}

// partition silently drops one dialed address's traffic in both
// directions: no data, no FIN, no RST, as a network partition would.
// New dials to the address fail while it is on.
type partition struct {
	mu   sync.Mutex
	addr string
	on   bool
}

func (p *partition) set(addr string, on bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.addr, p.on = addr, on
}

func (p *partition) cuts(addr string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.on && p.addr == addr
}

// partitionConn is a dialed connection that goes dead for good once its
// address is partitioned: reads deliver nothing until their deadline,
// and writes are discarded.
type partitionConn struct {
	net.Conn
	addr     string
	p        *partition
	dead     atomic.Bool
	closed   atomic.Bool
	deadline atomic.Pointer[time.Time]
}

func (c *partitionConn) isDead() bool {
	if !c.dead.Load() && c.p.cuts(c.addr) {
		c.dead.Store(true)
	}
	return c.dead.Load()
}

func (c *partitionConn) SetReadDeadline(t time.Time) error {
	c.deadline.Store(&t)
	return c.Conn.SetReadDeadline(t)
}

func (c *partitionConn) SetDeadline(t time.Time) error {
	c.deadline.Store(&t)
	return c.Conn.SetDeadline(t)
}

func (c *partitionConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func (c *partitionConn) Read(b []byte) (int, error) {
	if !c.isDead() {
		return c.Conn.Read(b)
	}
	scratch := make([]byte, 4096)
	for {
		_, err := c.Conn.Read(scratch)
		var ne net.Error
		switch {
		case err == nil:
			continue // dropped by the partition
		case errors.As(err, &ne) && ne.Timeout(), errors.Is(err, net.ErrClosed):
			return 0, err
		}
		// The peer gave up (EOF or reset), which a partition hides:
		// deliver nothing until the read deadline or a local close.
		for !c.closed.Load() {
			if d := c.deadline.Load(); d != nil && !d.IsZero() && !time.Now().Before(*d) {
				return 0, os.ErrDeadlineExceeded
			}
			time.Sleep(time.Millisecond)
		}
		return 0, net.ErrClosed
	}
}

func (c *partitionConn) Write(b []byte) (int, error) {
	if c.isDead() {
		return len(b), nil
	}
	return c.Conn.Write(b)
}

// TestLiveQuietAndPartition keeps both sources connected but entirely
// quiet for more than three rate periods, during which they stay Ready
// and authority never lapses; then it silently partitions node a. Node
// b must fall back to local protection through the revocation marker
// once a's health bound passes, and node a when its last lease marker
// runs out, both within 10 s; after the partition heals, a new session
// resynchronizes and authority returns.
func TestLiveQuietAndPartition(t *testing.T) {
	var p partition
	var dialer net.Dialer
	e := startE2E(t, hooks{dial: func(ctx context.Context, address string) (net.Conn, error) {
		if p.cuts(address) {
			return nil, fmt.Errorf("test partition cuts %s", address)
		}
		c, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		return &partitionConn{Conn: c, addr: address, p: &p}, nil
	}})
	e.waitLeased()
	const client = "2001:db8:f1::1"
	sessions := map[string]uint64{"a": status(t, e, "a").Session, "b": status(t, e, "b").Session}
	revocations := e.d.pub.Stats().Revocations
	quiet := 35 * time.Second
	start := time.Now()
	probes := 0
	for time.Since(start) < quiet {
		r := e.d.snap.Roster()
		st := e.d.pub.Stats()
		if !r.Ready || !st.Leased || st.Revocations != revocations {
			t.Fatalf("quiet sources lost readiness after %v: roster %+v publisher %+v", time.Since(start), r, st)
		}
		for _, n := range []*lab.Node{e.a, e.b} {
			if pr := e.probe(n, client); !liveMarker(pr) {
				t.Fatalf("node %s without a live marker after %v quiet: %+v", n.Name, time.Since(start), pr)
			}
			probes++
		}
		time.Sleep(100 * time.Millisecond)
	}
	for src, s := range sessions {
		if st := status(t, e, src); st.Session != s {
			t.Fatalf("source %s reconnected while quiet: session %d, was %d", src, st.Session, s)
		}
	}
	t.Logf("quiet for %v (3.5 periods): roster ready, lease held, %d probes saw a live marker", quiet, probes)

	cut := time.Now()
	p.set(e.a.PeersAddr, true)
	var aLocal, bLocal time.Time
	for aLocal.IsZero() || bLocal.IsZero() {
		if time.Since(cut) > 10*time.Second {
			t.Fatalf("no local fallback within 10 s: a %v, b %v; publisher %+v", aLocal, bLocal, e.d.pub.Stats())
		}
		if pr := e.probe(e.a, client); aLocal.IsZero() && !liveMarker(pr) {
			aLocal = time.Now()
		}
		if pr := e.probe(e.b, client); bLocal.IsZero() && !liveMarker(pr) {
			if pr.MetaGen != 0 {
				t.Errorf("node b fell back without the revocation marker: %+v", pr)
			}
			bLocal = time.Now()
		}
		time.Sleep(5 * time.Millisecond)
	}
	revoked := e.d.pub.Stats()
	rep, _ := e.d.snap.Source("a")
	if aLocal.Sub(cut) > output.MaxLease+500*time.Millisecond {
		t.Errorf("node a fell back %v after the partition; its marker should run out within %v", aLocal.Sub(cut),
			output.MaxLease)
	}
	// Local protection still applies on the partitioned node. (Exact
	// local-limit behavior is phase 10's TestLiveLocalProtection; here
	// only the fallback matters.)
	b := e.burst(e.a, "2001:db8:f2::1")
	last := b[len(b)-1]
	if last.r.Deny != "local" || last.r.Authority != "local" || len(b) < limit || len(b) > limit+2 {
		t.Errorf("partitioned a: first 429 after %d requests: %+v", len(b), last.r)
	}
	t.Logf("partition: node a local %v after the cut (lease ran out), node b local %v after (revocation; a %s: %s); "+
		"publisher revocations %d; burst on a denied locally at request %d (local rate %d)",
		aLocal.Sub(cut).Round(time.Millisecond), bLocal.Sub(cut).Round(time.Millisecond), rep.State, rep.Reason,
		revoked.Revocations-revocations, len(b), last.r.LocalRate)

	healed := time.Now()
	p.set(e.a.PeersAddr, false)
	e.waitLeased()
	if st := status(t, e, "a"); st.Session <= sessions["a"] {
		t.Errorf("a's session %d did not change across the partition", st.Session)
	}
	t.Logf("healed: authority back on both nodes %v later", time.Since(healed).Round(time.Millisecond))
}

// TestLiveColdRestart kills node a (as a crash would) and starts a
// fresh HAProxy process on the same listeners: its input table starts
// empty. The daemon must not call the lost history recovered: the
// absent key and a key recreated at a lower count are reported as lost,
// node a stays degraded and the lease revoked until their rates could no
// longer matter, and the lost counts are not restored.
func TestLiveColdRestart(t *testing.T) {
	var g gate
	var dialer net.Dialer
	e := startE2EWith(t, hooks{dial: func(ctx context.Context, address string) (net.Conn, error) {
		if g.refuses(address) {
			return nil, fmt.Errorf("test gate closed for %s", address)
		}
		return dialer.DialContext(ctx, "tcp", address)
	}}, lab.Options{Restartable: true})
	e.waitLeased()
	const gone, recreated = "2001:db8:d1::1", "2001:db8:d2::1"
	e.sendTo(e.a, gone, 20)
	e.sendTo(e.a, recreated, 20)
	e.sendTo(e.b, gone, 5)
	e.matchTables("before the cold restart")
	pid := e.a.PID()

	g.set(e.a.PeersAddr, true)
	killed := time.Now()
	if err := e.a.Restart(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	// Traffic recreates one key on the fresh process before it can
	// reconnect.
	e.sendTo(e.a, recreated, 2)
	if hap, ok := tableEntry(t, e.a, lab.LabTable, key(t, gone)); ok {
		t.Fatalf("fresh process holds %+v", hap)
	}
	g.set(e.a.PeersAddr, false)
	rep := e.waitSource("a", "lost history", 15*time.Second, func(r snapshot.SourceReport) bool {
		return r.Synced && r.Loss.Absent > 0
	})
	detected := time.Now()
	if rep.State != snapshot.Degraded || rep.Loss.Absent != 1 || rep.Loss.Recreated != 1 ||
		!rep.Loss.RateUntil.After(time.Now()) {
		t.Fatalf("after the cold restart: %+v", rep)
	}
	if _, ok := e.d.snap.Lookup("a", lab.LabTable, key(t, gone)); ok {
		t.Fatal("lost entry still held")
	}
	if en, _ := e.d.snap.Lookup("a", lab.LabTable, key(t, recreated)); en.Count != 2 {
		t.Fatalf("recreated entry %+v", en)
	}
	leases := e.d.pub.Stats().Leases
	for time.Now().Add(100 * time.Millisecond).Before(rep.Loss.RateUntil) {
		if st := e.d.pub.Stats(); st.Leases != leases || st.Leased {
			t.Fatalf("leased %v before the lost history decayed: %+v", time.Until(rep.Loss.RateUntil), st)
		}
		if pr := e.probe(e.b, gone); liveMarker(pr) {
			t.Fatalf("node b authoritative while a's history is lost: %+v", pr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	e.waitLeased()
	back := time.Now()
	if back.Before(rep.Loss.RateUntil) || back.Sub(rep.Loss.RateUntil) > 2*time.Second {
		t.Errorf("authority back %v after the loss window ended", back.Sub(rep.Loss.RateUntil))
	}
	tot, err := aggregate.Count(e.d.snap, lab.LabTable, key(t, gone))
	if err != nil {
		t.Fatal(err)
	}
	if tot.Sum != 5 || !tot.Complete || !tot.Uncertain {
		t.Errorf("total of the lost key %+v: want b's 5 alone, flagged uncertain", tot)
	}
	t.Logf("cold restart of a (pid %d -> %d): loss detected %v after the kill (%d absent, %d recreated lower); "+
		"degraded with the lease revoked for %v; authority back %v after the window; lost key total %d "+
		"(responder saw %d), uncertain until %v later", pid, e.a.PID(), detected.Sub(killed).Round(time.Millisecond),
		rep.Loss.Absent, rep.Loss.Recreated, rep.Loss.RateUntil.Sub(detected).Round(time.Millisecond),
		back.Sub(rep.Loss.RateUntil).Round(time.Millisecond), tot.Sum, e.responderTotal(gone),
		time.Until(rep.Loss.CountUntil).Round(time.Millisecond))
}

// TestLiveRepeatedReconnects cuts each source's session many times while
// traffic continues: every reconnect resynchronizes without inflating a
// count, each source keeps one contribution per key, the store matches
// HAProxy's tables, and the daemon's goroutines and heap stay bounded.
func TestLiveRepeatedReconnects(t *testing.T) {
	e := startE2E(t, hooks{})
	e.waitLeased()
	clients := []string{"2001:db8:a1::1", "2001:db8:a2::1", "2001:db8:a3::1"}
	for _, c := range clients {
		e.sendTo(e.a, c, 2)
		e.sendTo(e.b, c, 3)
	}
	e.matchTables("before the reconnects")
	measure := func() (int, uint64) {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return runtime.NumGoroutine(), m.HeapAlloc
	}
	g0, h0 := measure()
	const cycles = 16
	for i := range cycles {
		src := []string{"a", "b"}[i%2]
		n := map[string]*lab.Node{"a": e.a, "b": e.b}[src]
		session := status(t, e, src).Session
		if !e.d.m.Disconnect(src) {
			t.Fatalf("cycle %d: %s had no session", i, src)
		}
		e.sendTo(n, clients[i%len(clients)], 1)
		e.waitSource(src, "resynchronized", 10*time.Second, func(r snapshot.SourceReport) bool {
			return r.Session > session && r.State == snapshot.Ready
		})
		e.matchTables("cycle " + strconv.Itoa(i))
		for _, c := range clients {
			k := key(t, c)
			seen := map[string]bool{}
			for _, ct := range e.d.snap.Contributions(lab.LabTable, k) {
				if seen[ct.Source] {
					t.Fatalf("cycle %d: two contributions of %s for %s", i, ct.Source, c)
				}
				seen[ct.Source] = true
			}
			tot, err := aggregate.Count(e.d.snap, lab.LabTable, k)
			if err != nil {
				t.Fatal(err)
			}
			if want := e.responderTotal(c); tot.Sum != uint64(want) {
				t.Fatalf("cycle %d: %s total %d, responder %d", i, c, tot.Sum, want)
			}
		}
	}
	e.waitLeased()
	g1, h1 := measure()
	for _, src := range []string{"a", "b"} {
		if r, _ := e.d.snap.Source(src); r.Loss.Absent+r.Loss.Recreated != 0 || r.Entries != len(clients) {
			t.Errorf("source %s after %d reconnects: %+v", src, cycles/2, r)
		}
	}
	if g1 > g0+4 || h1 > h0+4<<20 {
		t.Errorf("after %d reconnects goroutines %d -> %d, heap %d -> %d", cycles, g0, g1, h0, h1)
	}
	t.Logf("%d reconnects: goroutines %d -> %d, heap %d KiB -> %d KiB; totals equal the responder throughout",
		cycles, g0, g1, h0>>10, h1>>10)
}
