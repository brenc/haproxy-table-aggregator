package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// limit is the full per-proxy threshold every live enforcement test
// configures: requests per 10 s period, locally and on the aggregate.
const limit = 100

// e2e is the complete one-table path: a two-node stock HAProxy lab whose
// lab listener enforces limit, and the daemon (runWith, as htad runs)
// dialing both nodes. The nodes never dial the daemon, so every session
// goes through hooks.dial.
type e2e struct {
	t      *testing.T
	l      *lab.Lab
	a, b   *lab.Node
	d      *daemon
	errOut *syncBuffer
	c      *lab.Client
	stop   func()
	cfg    string // the daemon's configuration file
}

// startE2E starts the lab and the daemon.
func startE2E(t *testing.T, h hooks) *e2e {
	t.Helper()
	return startE2EWith(t, h, lab.Options{})
}

// startE2EWith starts the lab with opts (its aggregator filled in) and
// the daemon.
func startE2EWith(t *testing.T, h hooks, opts lab.Options) *e2e {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := ln.Addr().String()
	_ = ln.Close()
	opts.Aggregator = &lab.Aggregator{Name: "agg", Addr: closedAddr, Limit: limit}
	l := labtest.Start(t, opts)
	e := &e2e{t: t, l: l, a: l.Node("a"), b: l.Node("b"), errOut: &syncBuffer{}}
	srcs := fmt.Sprintf(`{"name": "a", "address": %q}, {"name": "b", "address": %q}`, e.a.PeersAddr, e.b.PeersAddr)
	e.cfg = writeConfig(t, fmt.Sprintf(`{"local_peer": "agg", "insecure_plaintext_loopback_lab": true,
		"sources": [%s], "tables": [{"name": %q, "period": "10s"}],
		"outputs": [{"name": %q, "kind": "aggregate", "input": %q, "expire": "30s"},
		            {"name": %q, "kind": "metadata", "expire": "2s"}],
		"reconnect_min": "50ms", "reconnect_max": "500ms"}`,
		srcs, lab.LabTable, lab.OutputTable, lab.LabTable, lab.MetaTable))
	e.launch(h)
	if e.c, err = lab.NewClient(nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.c.CloseIdle)
	return e
}

// launch starts a daemon with empty memory on e's configuration, as a
// restarted htad process would, and makes it e.d. The previous one, if
// any, must have been stopped.
func (e *e2e) launch(h hooks) {
	t := e.t
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan *daemon, 1)
	done := make(chan error, 1)
	user := h.ready
	h.ready = func(d *daemon) {
		if user != nil {
			user(d)
		}
		ready <- d
	}
	go func() { done <- runWith(ctx, []string{"-config", e.cfg}, discard{}, e.errOut, h) }()
	select {
	case e.d = <-ready:
	case runErr := <-done:
		cancel()
		t.Fatalf("daemon: %v\n%s", runErr, e.errOut.String())
	}
	var once sync.Once
	e.stop = func() {
		once.Do(func() {
			cancel()
			if runErr := <-done; runErr != nil {
				t.Errorf("daemon: %v", runErr)
			}
		})
	}
	stop := e.stop
	t.Cleanup(func() {
		stop()
		if t.Failed() {
			t.Logf("daemon log:\n%s", e.errOut.String())
		}
	})
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// waitLeased waits until the publisher holds a lease and both nodes hold
// a live marker.
func (e *e2e) waitLeased() {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ok := e.d.pub.Stats().Leased
		for _, n := range []*lab.Node{e.a, e.b} {
			p, err := e.c.Probe(context.Background(), n.ProbeAddr, "2001:db8:ffff::1")
			ok = ok && err == nil && p.MetaVersion == output.SchemaVersion && p.LeaseLeft <= output.LeaseWindowMillis
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("no authority within 30 s: %+v\n%s", e.d.pub.Stats(), e.errOut.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// probe asks node n's probe listener about client's key.
func (e *e2e) probe(n *lab.Node, client string) lab.ProbeResponse {
	e.t.Helper()
	p, err := e.c.Probe(context.Background(), n.ProbeAddr, client)
	if err != nil {
		e.t.Fatal(err)
	}
	return p
}

// sample is one request's outcome on the enforcing lab listener.
type sample struct {
	at   time.Time // when the request was sent
	node string
	r    lab.Response
}

// send sends one request for client to node n's lab listener.
func (e *e2e) send(n *lab.Node, client string) sample {
	e.t.Helper()
	at := time.Now()
	r, err := e.c.Send(context.Background(), n.LabAddr, client)
	if err != nil {
		e.t.Fatal(err)
	}
	if r.Status != http.StatusOK && r.Status != http.StatusTooManyRequests {
		e.t.Fatalf("node %s: status %d", n.Name, r.Status)
	}
	return sample{at: at, node: n.Name, r: r}
}

// pace sends a request for client to each node every interval until
// stop, and returns every outcome in send order.
func (e *e2e) pace(nodes []*lab.Node, client string, interval time.Duration, stop <-chan struct{}) []sample {
	var mu sync.Mutex
	var out []sample
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Go(func() {
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for {
				at := time.Now()
				r, err := e.c.Send(context.Background(), n.LabAddr, client)
				if err != nil {
					e.t.Error(err)
					return
				}
				mu.Lock()
				out = append(out, sample{at: at, node: n.Name, r: r})
				mu.Unlock()
				select {
				case <-stop:
					return
				case <-tick.C:
				}
			}
		})
	}
	wg.Wait()
	slices.SortFunc(out, func(x, y sample) int { return x.at.Compare(y.at) })
	return out
}

func key(t *testing.T, client string) peermsg.Key {
	t.Helper()
	p, err := netip.MustParseAddr(client).Prefix(64)
	if err != nil {
		t.Fatal(err)
	}
	k, err := peermsg.KeyFromAddr(p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// tableEntry reads node n's entry for k in table.
func tableEntry(t *testing.T, n *lab.Node, table string, k peermsg.Key) (lab.Entry, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tb, err := n.ShowTable(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	return tb.Entry(k.String())
}

// TestLiveEnforcement runs the complete path against stock HAProxy: two
// nodes each stay below the local limit while their combined rate
// crosses it, and both answer 429 from the aggregate once it propagates;
// after traffic stops, decay alone lifts the aggregate denial. It also
// checks that the aggregate is never counted back into an input.
func TestLiveEnforcement(t *testing.T) {
	e := startE2E(t, hooks{})
	e.waitLeased()
	const client = "2001:db8:a10::1"
	k := key(t, client)

	// 6 requests per second on each node: about 60 per 10 s period each,
	// 120 together.
	stop := make(chan struct{})
	time.AfterFunc(20*time.Second, func() { close(stop) })
	samples := e.pace([]*lab.Node{e.a, e.b}, client, time.Second/6, stop)
	stopped := time.Now()

	latest := map[string]int64{}
	var cross time.Time
	firstDeny := map[string]time.Time{}
	sent := map[string]int{}
	admitted := map[string]int{}
	admittedAfterCross, flaps := 0, 0
	var aggAtDeny int64
	for _, s := range samples {
		sent[s.node]++
		r := s.r
		if r.Deny == "local" || r.LocalRate >= limit {
			t.Errorf("node %s denied locally or reached the local limit: %+v", s.node, r)
		}
		latest[s.node] = r.LocalRate
		if cross.IsZero() && latest["a"]+latest["b"] >= limit {
			cross = s.at
		}
		switch r.Status {
		case http.StatusOK:
			admitted[s.node]++
			if !cross.IsZero() {
				admittedAfterCross++
			}
			if !firstDeny[s.node].IsZero() {
				flaps++
			}
		case http.StatusTooManyRequests:
			if r.Deny != "aggregate" || r.Authority != "aggregate" || r.AggRate < limit {
				t.Errorf("node %s: 429 %+v, want an authoritative aggregate denial", s.node, r)
			}
			if firstDeny[s.node].IsZero() {
				firstDeny[s.node] = s.at
				if s.node == "a" {
					aggAtDeny = r.AggRate
				}
			}
		}
	}
	if cross.IsZero() || firstDeny["a"].IsZero() || firstDeny["b"].IsZero() {
		t.Fatalf("combined crossing %v, first aggregate denials %v; samples %d", cross, firstDeny, len(samples))
	}
	if flaps > 0 {
		t.Errorf("%d requests admitted after a node's first aggregate denial", flaps)
	}
	t.Logf("sent a=%d b=%d, admitted a=%d b=%d; combined local estimates reached %d at +%v",
		sent["a"], sent["b"], admitted["a"], admitted["b"], limit, cross.Sub(samples[0].at).Round(time.Millisecond))
	for _, n := range []string{"a", "b"} {
		t.Logf("convergence: node %s first aggregate 429 %v after the crossing (request interval %v)", n,
			firstDeny[n].Sub(cross).Round(time.Millisecond), time.Second/6)
	}
	t.Logf("overshoot: %d requests admitted after the crossing, aggregate %d at node a's first denial "+
		"(limit %d); admitted in total %d", admittedAfterCross, aggAtDeny, limit, admitted["a"]+admitted["b"])

	// Decay: with no further request, the aggregate falls below the
	// limit while the entry is still published and authoritative.
	var released time.Time
	for deadline := stopped.Add(25 * time.Second); released.IsZero(); {
		pa, pb := e.probe(e.a, client), e.probe(e.b, client)
		if pa.Aggregate() && pb.Aggregate() && pa.Rate <= limit-5 && pb.Rate <= limit-5 {
			released = time.Now()
			t.Logf("decay: aggregate %d/%d (below %d) %v after traffic stopped, entry version %d, authority %s",
				pa.Rate, pb.Rate, limit, released.Sub(stopped).Round(time.Millisecond), pa.Version, pa.Authority)
		}
		if time.Now().After(deadline) {
			t.Fatalf("aggregate never decayed below the limit: %+v %+v", pa, pb)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, n := range []*lab.Node{e.a, e.b} {
		s := e.send(n, client)
		sent[n.Name]++
		if s.r.Status != http.StatusOK || s.r.Authority != "aggregate" || s.r.Deny != "" {
			t.Errorf("node %s after decay: %+v, want 200 under aggregate authority", n.Name, s.r)
		} else {
			admitted[n.Name]++
		}
	}

	// Isolation: each node's input counts exactly its own requests
	// (denied ones included, since tracking precedes the decision); the
	// daemon's snapshot holds the same counts; the output table holds
	// only gpt values; HAProxy never received a definition or update of
	// an input table from the daemon; and the published aggregate is the
	// sum of the nodes' own rates, not twice it.
	rates := map[string]int64{}
	for _, n := range []*lab.Node{e.a, e.b} {
		in, ok := tableEntry(t, n, lab.LabTable, k)
		if !ok || in.Data["http_req_cnt"] != int64(sent[n.Name]) {
			t.Errorf("node %s lab_in %+v, want http_req_cnt %d (requests sent)", n.Name, in, sent[n.Name])
		}
		rates[n.Name] = in.Data["http_req_rate(10000)"]
		out, ok := tableEntry(t, n, lab.OutputTable, k)
		if !ok {
			t.Errorf("node %s has no lab_out entry", n.Name)
		}
		for f := range out.Data {
			if !strings.HasPrefix(f, "gpt") {
				t.Errorf("node %s lab_out entry carries %s: %+v", n.Name, f, out)
			}
		}
		if got := e.l.Responder.Count(lab.Observation{Node: n.Name, Listener: "lab", Key: k.String()}); got != admitted[n.Name] {
			t.Errorf("node %s: responder saw %d requests, %d admitted", n.Name, got, admitted[n.Name])
		}
		checkNoInputSent(t, n)
	}
	pub, _ := e.d.out.Get(lab.OutputTable, k)
	if sum := rates["a"] + rates["b"]; abs(int64(pub[output.SlotRate])-sum) > 5 {
		t.Errorf("published %d, nodes' own rates %d + %d", pub[output.SlotRate], rates["a"], rates["b"])
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		ea, oka := e.d.snap.Lookup("a", lab.LabTable, k)
		eb, okb := e.d.snap.Lookup("b", lab.LabTable, k)
		if oka && okb && int(ea.Count) == sent["a"] && int(eb.Count) == sent["b"] {
			t.Logf("snapshot counts a=%d b=%d equal requests sent; published %d, nodes' rates %d + %d",
				ea.Count, eb.Count, pub[output.SlotRate], rates["a"], rates["b"])
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot counts %d/%d, sent %v", ea.Count, eb.Count, sent)
		}
		time.Sleep(20 * time.Millisecond)
	}
	st := e.d.pub.Stats()
	t.Logf("publisher: %+v", st)
	if st.Overflows != 0 || st.Errors != 0 || st.Revocations != 0 || st.LeaseErrors != 0 {
		t.Errorf("publisher stats %+v", st)
	}
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

var (
	peersTableRE = regexp.MustCompile(`^\s*table:0x[0-9a-f]+ id=(\S+)`)
	peersKVRE    = regexp.MustCompile(`([a-z_]+)=(\S+)`)
)

// checkNoInputSent checks in "show peers" that the daemon never announced
// or updated an input table on node n: HAProxy records no remote ID and
// no received update for lab_in.
func checkNoInputSent(t *testing.T, n *lab.Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, err := n.Runtime(ctx, "show peers")
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]string{}
	found, inAgg := false, false
	for line := range strings.SplitSeq(reply, "\n") {
		// Only the aggregator peer's section lists what it sent.
		if strings.Contains(line, " id=agg(") {
			inAgg = true
			continue
		}
		if !inAgg {
			continue
		}
		if m := peersTableRE.FindStringSubmatch(line); m != nil {
			if m[1] == lab.LabTable {
				found = true
				break
			}
			fields = map[string]string{}
			continue
		}
		if strings.Contains(line, "remote_data=") || strings.Contains(line, "last_acked=") {
			for _, kv := range peersKVRE.FindAllStringSubmatch(line, -1) {
				fields[kv[1]] = kv[2]
			}
		}
	}
	if !found || fields["remote_id"] != "0" || fields["last_get"] != "0" {
		t.Errorf("node %s lab_in in show peers: %v (found %v); the daemon must never send an input table\n%s",
			n.Name, fields, found, reply)
	}
}

// burst sends requests for client to node n back to back until the first
// 429 (at most 2·limit) and returns every outcome.
func (e *e2e) burst(n *lab.Node, client string) []sample {
	e.t.Helper()
	var out []sample
	for range 2 * limit {
		s := e.send(n, client)
		out = append(out, s)
		if s.r.Status == http.StatusTooManyRequests {
			return out
		}
	}
	e.t.Fatalf("node %s: no 429 in %d requests", n.Name, 2*limit)
	return nil
}

// checkLocalDenial checks that a burst was denied by the local limit at
// exactly the full threshold: the request whose own rate reached limit.
func checkLocalDenial(t *testing.T, what string, b []sample) sample {
	t.Helper()
	last := b[len(b)-1]
	if len(b) != limit || last.r.Deny != "local" || last.r.LocalRate != limit {
		t.Errorf("%s: first 429 after %d requests: %+v; want the local limit at request %d", what, len(b), last.r, limit)
	}
	for _, s := range b[:len(b)-1] {
		if s.r.Status != http.StatusOK {
			t.Errorf("%s: %+v before the limit", what, s.r)
		}
	}
	return last
}

// gate refuses dials to one address while closed.
type gate struct {
	mu     sync.Mutex
	addr   string
	closed bool
}

func (g *gate) set(addr string, closed bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.addr, g.closed = addr, closed
}

func (g *gate) refuses(addr string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed && addr == g.addr
}

// TestLiveLocalProtection checks local protection with the full limit:
// a node exceeding it is denied before any global update arrives; while
// a required source is missing (roster unready) both nodes run local
// with the full limit, though their combined rate exceeds it; and with
// the daemon gone, local protection alone still applies.
func TestLiveLocalProtection(t *testing.T) {
	var g gate
	var dialer net.Dialer
	e := startE2E(t, hooks{dial: func(ctx context.Context, address string) (net.Conn, error) {
		if g.refuses(address) {
			return nil, fmt.Errorf("test gate closed for %s", address)
		}
		return dialer.DialContext(ctx, "tcp", address)
	}})
	e.waitLeased()

	t.Run("before a global update", func(t *testing.T) {
		const client = "2001:db8:a20::1"
		b := e.burst(e.a, client)
		last := checkLocalDenial(t, "authoritative, burst on a", b)
		lag := 0
		for _, s := range b {
			if s.r.AggRate < s.r.LocalRate {
				lag++
			}
		}
		t.Logf("burst of %d requests in %v: first 429 local at local rate %d, aggregate then %d (authority %s); "+
			"%d responses saw the aggregate behind the local rate", len(b), last.at.Sub(b[0].at).Round(time.Millisecond),
			last.r.LocalRate, last.r.AggRate, last.r.Authority, lag)
	})

	t.Run("roster unready", func(t *testing.T) {
		g.set(e.b.PeersAddr, true)
		revokedAt := time.Now()
		if !e.d.m.Disconnect("b") {
			t.Fatal("b had no session")
		}
		const client = "2001:db8:a30::1"
		var local time.Time
		for deadline := time.Now().Add(5 * time.Second); local.IsZero(); {
			if p := e.probe(e.a, "2001:db8:ffff::1"); p.MetaGen == 0 {
				local = time.Now()
			}
			if time.Now().After(deadline) {
				t.Fatalf("no revocation on node a: %+v", e.d.pub.Stats())
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Logf("revocation visible on node a %v after b was disconnected; publisher %+v",
			local.Sub(revokedAt).Round(time.Millisecond), e.d.pub.Stats())
		// 8 per second on each node: about 80 per period each, 160
		// together, above the limit but never denied: no aggregate,
		// and the local limit is not divided by the number of proxies.
		stop := make(chan struct{})
		time.AfterFunc(13*time.Second, func() { close(stop) })
		samples := e.pace([]*lab.Node{e.a, e.b}, client, time.Second/8, stop)
		maxRate := map[string]int64{}
		for _, s := range samples {
			if s.r.Status != http.StatusOK || s.r.Authority != "local" {
				t.Errorf("node %s while unready: %+v", s.node, s.r)
			}
			maxRate[s.node] = max(maxRate[s.node], s.r.LocalRate)
		}
		if maxRate["a"]+maxRate["b"] <= limit || maxRate["a"] < limit/2 || maxRate["b"] < limit/2 {
			t.Errorf("local rates %v did not exercise the full limit", maxRate)
		}
		t.Logf("unready: %d requests, all 200 under local authority; peak local rates %v (sum %d > %d)",
			len(samples), maxRate, maxRate["a"]+maxRate["b"], limit)
		b := e.burst(e.a, "2001:db8:a31::1")
		last := checkLocalDenial(t, "unready, burst on a", b)
		t.Logf("unready burst: first 429 local at local rate %d (authority %s)", last.r.LocalRate, last.r.Authority)
		g.set(e.b.PeersAddr, false)
		e.waitLeased()
	})

	t.Run("daemon gone", func(t *testing.T) {
		// Shutdown revokes the lease and waits (bounded) for every
		// session to write the revocation, so both nodes fall back at
		// once rather than when their last marker runs out.
		stopping := time.Now()
		e.stop()
		stopped := time.Now()
		if strings.Contains(e.errOut.String(), "revocation not written") {
			t.Errorf("shutdown did not deliver the revocation:\n%s", e.errOut.String())
		}
		for _, n := range []*lab.Node{e.a, e.b} {
			var local time.Time
			var p lab.ProbeResponse
			for deadline := stopped.Add(5 * time.Second); local.IsZero(); {
				p = e.probe(n, "2001:db8:ffff::1")
				if p.MetaGen == 0 || p.LeaseLeft > output.LeaseWindowMillis {
					local = time.Now()
				}
				if time.Now().After(deadline) {
					t.Fatalf("node %s: marker still live 5 s after the daemon stopped", n.Name)
				}
				time.Sleep(time.Millisecond)
			}
			revoked := p.MetaVersion == output.SchemaVersion && p.MetaGen == 0
			if !revoked || local.Sub(stopped) > 100*time.Millisecond {
				t.Errorf("node %s: %+v %v after shutdown; want the revocation marker at once", n.Name, p,
					local.Sub(stopped))
			}
			t.Logf("node %s: revocation marker read %v after shutdown began (%v after it returned)", n.Name,
				local.Sub(stopping).Round(time.Millisecond), local.Sub(stopped).Round(time.Millisecond))
		}
		b := e.burst(e.b, "2001:db8:a40::1")
		last := checkLocalDenial(t, "daemon gone, burst on b", b)
		t.Logf("daemon gone: first 429 local at local rate %d (authority %s)", last.r.LocalRate, last.r.Authority)
	})
}

// stallConn holds every write while its stall is on for the address it
// dialed, honoring write deadlines as a full socket buffer would.
type stallConn struct {
	net.Conn
	addr     string
	mu       sync.Mutex
	deadline time.Time
	stall    *stall
}

// stall is a switchable write stall for one dialed address.
type stall struct {
	mu      sync.Mutex
	addr    string
	release chan struct{} // nil when not stalled
}

func (s *stall) start(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addr, s.release = addr, make(chan struct{})
}

func (s *stall) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.release != nil {
		close(s.release)
		s.release = nil
	}
}

func (s *stall) wait(addr string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if addr != s.addr {
		return nil
	}
	return s.release
}

func (c *stallConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(t)
}

func (c *stallConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	c.mu.Unlock()
	return c.Conn.SetDeadline(t)
}

func (c *stallConn) Write(p []byte) (int, error) {
	if ch := c.stall.wait(c.addr); ch != nil {
		c.mu.Lock()
		d := c.deadline
		c.mu.Unlock()
		var timeout <-chan time.Time
		if !d.IsZero() {
			tm := time.NewTimer(time.Until(d))
			defer tm.Stop()
			timeout = tm.C
		}
		select {
		case <-ch:
		case <-timeout:
			return 0, os.ErrDeadlineExceeded
		}
	}
	return c.Conn.Write(p)
}

// propagation sends one request for a fresh client key to node from and
// returns how long, from just before the request, until node to's
// HAProxy observes the key's aggregate (rate at least 1) as authoritative
// through its own ACL lookups; and the same from the response.
func (e *e2e) propagation(from, to *lab.Node, client string) (fromSend, fromResponse time.Duration) {
	e.t.Helper()
	s := e.send(from, client)
	responded := time.Now()
	for deadline := s.at.Add(5 * time.Second); ; {
		p := e.probe(to, client)
		if p.Aggregate() && p.Rate >= 1 {
			now := time.Now()
			return now.Sub(s.at), now.Sub(responded)
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("key %s never authoritative on node %s: %+v", client, to.Name, p)
		}
	}
}

// percentiles summarizes durations.
func percentiles(d []time.Duration) string {
	if len(d) == 0 {
		return "no samples"
	}
	s := slices.Clone(d)
	slices.Sort(s)
	at := func(q float64) time.Duration { return s[min(len(s)-1, int(q*float64(len(s))))] }
	return fmt.Sprintf("n=%d p50=%v p90=%v p99=%v max=%v", len(s), at(0.5).Round(10*time.Microsecond),
		at(0.9).Round(10*time.Microsecond), at(0.99).Round(10*time.Microsecond), s[len(s)-1].Round(10*time.Microsecond))
}

func p99(d []time.Duration) time.Duration {
	s := slices.Clone(d)
	slices.Sort(s)
	return s[min(len(s)-1, int(0.99*float64(len(s))))]
}

// TestLivePropagation measures healthy end-to-end update propagation as
// HAProxy observes it: from a request on one node to the other node's
// (and the same node's) ACLs treating the new aggregate as authoritative.
func TestLivePropagation(t *testing.T) {
	e := startE2E(t, hooks{})
	e.waitLeased()
	var cross, crossResp, same []time.Duration
	for i := range 200 {
		from, to := e.a, e.b
		if i%2 == 1 {
			from, to = e.b, e.a
		}
		d, dr := e.propagation(from, to, fmt.Sprintf("2001:db8:b%x::1", i))
		cross, crossResp = append(cross, d), append(crossResp, dr)
		if i%4 == 0 {
			d, _ := e.propagation(from, from, fmt.Sprintf("2001:db8:c%x::1", i))
			same = append(same, d)
		}
	}
	t.Logf("propagation to the other node, from request start: %s", percentiles(cross))
	t.Logf("propagation to the other node, from the response: %s", percentiles(crossResp))
	t.Logf("propagation to the same node, from request start: %s", percentiles(same))
	if p := p99(cross); p > 250*time.Millisecond {
		t.Errorf("p99 propagation %v exceeds the 250 ms target", p)
	}
	t.Logf("publisher: %+v", e.d.pub.Stats())
}

// TestLiveSlowDestination stalls every write to node b for 3.5 s and
// checks that node a and the daemon's controls are unaffected: node a
// keeps its authority and its propagation latency, node b's input is
// still read (its traffic reaches node a's aggregate) so the roster stays
// ready, node b alone falls back to local protection once its last marker
// runs out, and recovers on the same session once the stall ends.
func TestLiveSlowDestination(t *testing.T) {
	st := &stall{}
	var dialer net.Dialer
	e := startE2E(t, hooks{dial: func(ctx context.Context, address string) (net.Conn, error) {
		c, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		return &stallConn{Conn: c, addr: address, stall: st}, nil
	}})
	e.waitLeased()
	const client = "2001:db8:a50::1"
	k := key(t, client)
	for _, n := range []*lab.Node{e.a, e.b} {
		e.send(n, client)
	}
	for !e.probe(e.b, client).Aggregate() {
		time.Sleep(10 * time.Millisecond)
	}
	var base []time.Duration
	for i := range 20 {
		d, _ := e.propagation(e.b, e.a, fmt.Sprintf("2001:db8:d%x::1", i))
		base = append(base, d)
	}
	sessionB := status(t, e, "b").Session

	st.start(e.b.PeersAddr)
	stalled := time.Now()
	var during []time.Duration
	aLocal, bLocal, probes := 0, time.Time{}, 0
	ready := true
	var bSeen time.Time
	for i := 0; time.Since(stalled) < 3500*time.Millisecond; i++ {
		// Node b's traffic, a few requests per second: its updates
		// must still be read.
		if i%10 == 0 {
			if s := e.send(e.b, client); s.r.Status != http.StatusOK {
				t.Errorf("node b: %+v", s.r)
			}
		}
		if en, ok := e.d.snap.Lookup("b", lab.LabTable, k); ok && en.Received.After(stalled) && bSeen.IsZero() {
			bSeen = en.Received
		}
		if i%3 == 0 {
			d, _ := e.propagation(e.b, e.a, fmt.Sprintf("2001:db8:e%x::1", i))
			during = append(during, d)
		}
		pa := e.probe(e.a, client)
		probes++
		if !pa.Aggregate() {
			aLocal++
		}
		if bLocal.IsZero() && !e.probe(e.b, client).Aggregate() {
			bLocal = time.Now()
		}
		ready = ready && e.d.pub.Stats().Ready
		time.Sleep(20 * time.Millisecond)
	}
	sb := status(t, e, "b").Stats
	st.end()
	released := time.Now()
	var bBack time.Time
	for deadline := released.Add(5 * time.Second); bBack.IsZero(); {
		if p := e.probe(e.b, client); p.Aggregate() {
			bBack = time.Now()
			pub, _ := e.d.out.Get(lab.OutputTable, k)
			if p.Rate != int64(pub[output.SlotRate]) {
				t.Logf("node b reads %d, published %d (still converging)", p.Rate, pub[output.SlotRate])
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("node b never regained authority after the stall")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if aLocal != 0 {
		t.Errorf("node a lost authority in %d of %d probes during b's stall", aLocal, probes)
	}
	if !ready {
		t.Errorf("roster not ready during b's stall: %+v", e.d.pub.Stats())
	}
	if bSeen.IsZero() {
		t.Error("node b's updates were not read during its stall")
	}
	if bLocal.IsZero() || bLocal.Sub(stalled) > output.MaxLease+250*time.Millisecond {
		t.Errorf("node b fell back to local %v after the stall started", bLocal.Sub(stalled))
	}
	if p := p99(during); p > 250*time.Millisecond {
		t.Errorf("node a propagation p99 %v during b's stall", p)
	}
	if got := status(t, e, "b").Session; got != sessionB {
		t.Errorf("node b's session changed from %d to %d", sessionB, got)
	}
	t.Logf("baseline propagation b->a: %s", percentiles(base))
	t.Logf("propagation b->a during b's stall: %s", percentiles(during))
	t.Logf("during the stall: node a aggregate in %d of %d probes; node b local %v after the stall began; "+
		"b's input read %v after; roster ready throughout %v; b's session queued %d bytes (max %d), deferred %d",
		probes-aLocal, probes, bLocal.Sub(stalled).Round(time.Millisecond), bSeen.Sub(stalled).Round(time.Millisecond),
		ready, sb.OutQueued, sb.MaxOutQueued, sb.OutDeferred)
	t.Logf("node b regained authority %v after the stall ended, session %d unchanged",
		bBack.Sub(released).Round(time.Millisecond), sessionB)
}

func status(t *testing.T, e *e2e, src string) sources.SourceStatus {
	t.Helper()
	for _, s := range e.d.m.Status() {
		if s.Name == src {
			return s
		}
	}
	t.Fatalf("no source %s", src)
	return sources.SourceStatus{}
}
