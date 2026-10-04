package sources_test

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// The phase 06 freshness experiments run the frozen authority rule (see
// package output) on stock HAProxy through the lab's probe listener, with
// known output values and a simulated publisher that renews the lease.
// Authority is measured by request decisions: a probe every probeEvery,
// each recorded with when it was sent.
const (
	// freshLease is what each renewal grants: authority should end
	// about this long after the last renewal.
	freshLease = time.Second
	// authorityBound is the contract's bound on authority after the last
	// valid publication.
	authorityBound = output.MaxLease
	probeEvery     = 10 * time.Millisecond
)

// Probe clients: overLimit is published at or above lab.OutputLimit.
const (
	overClient  = "2001:db8:a::1"
	underClient = "2001:db8:b::1"
	otherClient = "2001:db8:c::1"
	freshClient = "2001:db8:d::1"
)

type freshOptions struct {
	period     time.Duration // zero: lab.DefaultPeriod
	reloadable bool
	skew       time.Duration // the aggregator's wall clock minus HAProxy's
	refresh    time.Duration // sources.Options.OutputRefresh
}

// fresh is one freshness experiment: a single lab node "a" that dials
// the aggregator through a controllable listener.
type fresh struct {
	refresh time.Duration
	t       *testing.T
	l       *lab.Lab
	n       *lab.Node
	addr    string
	ctl     *holdListener
	cfg     config.Config
	store   *output.Store
	m       *sources.Manager
	r       *recorder
	c       *lab.Client
}

func newFresh(t *testing.T, o freshOptions) *fresh {
	t.Helper()
	ln := listen(t)
	l := labtest.Start(t, lab.Options{
		Period: o.period, Nodes: []string{"a"}, Reloadable: o.reloadable,
		Aggregator: &lab.Aggregator{Name: aggPeer, Addr: ln.Addr().String()},
	})
	c, err := lab.NewClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdle)
	f := &fresh{
		t: t, l: l, n: l.Node("a"), addr: ln.Addr().String(), c: c,
		cfg: liveOutputConfig(t, l.Period, config.FileSource{Name: "a"}),
	}
	f.refresh = o.refresh
	f.start(ln, o.skew)
	return f
}

// start runs a manager (a new aggregator incarnation) on ln with a new
// store, and waits until node a's session is up and resynchronized.
func (f *fresh) start(ln net.Listener, skew time.Duration) {
	f.t.Helper()
	var wall func() time.Time
	if skew != 0 {
		wall = func() time.Time { return time.Now().Add(skew) }
	}
	store, err := output.NewStore(f.cfg.OutputTables(), output.StoreOptions{WallClock: wall})
	if err != nil {
		f.t.Fatal(err)
	}
	f.store, f.ctl = store, &holdListener{Listener: ln}
	f.m, f.r = startManager(f.t, sources.Options{
		Config: f.cfg, Listener: f.ctl, Output: store,
		OutputRefresh: f.refresh,
	})
	f.t.Cleanup(f.ctl.releaseAll)
	f.r.waitFor("node a resynchronized", 30*time.Second, func(evs []sources.Event) bool {
		return count[peersession.SyncFinished](evs, "a") >= 1
	})
}

// restart kills the current incarnation (if still running) and starts a
// new one on the same address.
func (f *fresh) restart() {
	f.t.Helper()
	if err := f.m.Close(); err != nil {
		f.t.Fatal(err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp4", f.addr)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = ln.Close() })
	f.start(ln, 0)
}

// gen returns the generation of node a's current session.
func (f *fresh) gen() int64 {
	f.t.Helper()
	s := status(f.t, f.m, "a")
	if !s.Up {
		f.t.Fatal("node a has no session")
	}
	return int64(s.Stats.Generation)
}

func (f *fresh) publish(client string, rate uint32) {
	f.t.Helper()
	if err := f.store.Set(lab.OutputTable, clientKey(f.t, client), output.AggregateValues(rate)); err != nil {
		f.t.Fatal(err)
	}
}

func clientKey(t *testing.T, client string) peermsg.Key {
	t.Helper()
	switch client {
	case overClient:
		return mustKey(t, "2001:db8:a::")
	case underClient:
		return mustKey(t, "2001:db8:b::")
	case otherClient:
		return mustKey(t, "2001:db8:c::")
	case freshClient:
		return mustKey(t, "2001:db8:d::")
	}
	t.Fatalf("unknown probe client %s", client)
	return peermsg.Key{}
}

func (f *fresh) probe(client string) lab.ProbeResponse {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := f.c.Probe(ctx, f.n.ProbeAddr, client)
	if err != nil {
		f.t.Fatal(err)
	}
	return p
}

// waitProbe polls until pred holds for client's decision.
func (f *fresh) waitProbe(client, what string, pred func(lab.ProbeResponse) bool) lab.ProbeResponse {
	f.t.Helper()
	return waitProbe(f.t, f.n, client, what, pred)
}

// authoritative is a probe predicate: aggregate authority over rate, with
// the matching decision.
func authoritative(rate int64) func(lab.ProbeResponse) bool {
	return func(p lab.ProbeResponse) bool {
		want := http.StatusOK
		if rate >= lab.OutputLimit {
			want = http.StatusTooManyRequests
		}
		return p.Aggregate() && p.Rate == rate && p.Status == want
	}
}

// checkLocal fails unless p is a local-protection decision: no aggregate
// limit applied.
func checkLocal(t *testing.T, what string, p lab.ProbeResponse) {
	t.Helper()
	if p.Aggregate() || p.Status != http.StatusOK {
		t.Fatalf("%s: want local protection, got %+v", what, p)
	}
}

// decision is one probe answer and when its request was sent.
type decision struct {
	sent time.Time
	p    lab.ProbeResponse
}

// watcher probes one client every probeEvery until end.
type watcher struct {
	mu   sync.Mutex
	ds   []decision
	err  error
	done chan struct{}
	stop chan struct{}
}

func (f *fresh) watch(client string) *watcher {
	w := &watcher{done: make(chan struct{}), stop: make(chan struct{})}
	go func() {
		defer close(w.done)
		tick := time.NewTicker(probeEvery)
		defer tick.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-tick.C:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			sent := time.Now()
			p, err := f.c.Probe(ctx, f.n.ProbeAddr, client)
			cancel()
			w.mu.Lock()
			if err != nil {
				// A reload can reset a connection in flight; retry.
				w.err = err
			} else {
				w.ds = append(w.ds, decision{sent, p})
			}
			w.mu.Unlock()
		}
	}()
	return w
}

func (w *watcher) end() []decision {
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ds
}

// checkAuthorityEnds requires every decision sent later than
// authorityBound after last (the last valid publication) to be local,
// and some decision to have been sent after that, and returns how long
// after last the last aggregate decision was sent (negative if none).
func checkAuthorityEnds(t *testing.T, what string, ds []decision, last time.Time) time.Duration {
	t.Helper()
	end := time.Duration(-1)
	after := 0
	for _, d := range ds {
		since := d.sent.Sub(last)
		if d.p.Aggregate() && since > end {
			end = since
		}
		if since > authorityBound {
			after++
			if d.p.Aggregate() || d.p.Status != http.StatusOK {
				t.Errorf("%s: decision sent %v after the last publication is still aggregate: %+v", what, since, d.p)
			}
		}
	}
	if after == 0 {
		t.Fatalf("%s: no decision sent more than %v after the last publication", what, authorityBound)
	}
	lastAgg := "none"
	if end >= 0 {
		lastAgg = end.Round(time.Millisecond).String()
	}
	t.Logf("%s: %d decisions; last aggregate one sent %s after the last publication; %d local decisions after %v",
		what, len(ds), lastAgg, after, authorityBound)
	return end
}

// holdListener wraps the aggregator's listener so that a test can hold
// the bytes the aggregator writes (they queue, in order, as in a stalled
// network path or a slow receiver) or freeze every read and write (as
// when the process is paused: the TCP connection stays open, no byte
// flows).
type holdListener struct {
	net.Listener
	mu    sync.Mutex
	cond  *sync.Cond
	held  bool
	froze bool
	conns []*holdConn
}

func (h *holdListener) Accept() (net.Conn, error) {
	c, err := h.Listener.Accept()
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cond == nil {
		h.cond = sync.NewCond(&h.mu)
	}
	hc := &holdConn{Conn: c, h: h}
	h.conns = append(h.conns, hc)
	return hc, nil
}

func (h *holdListener) set(fn func()) {
	h.mu.Lock()
	if h.cond == nil {
		h.cond = sync.NewCond(&h.mu)
	}
	fn()
	h.cond.Broadcast()
	h.mu.Unlock()
}

// hold queues every later write until release.
func (h *holdListener) hold() { h.set(func() { h.held = true }) }

// freeze blocks every later read and write until thaw.
func (h *holdListener) freeze() { h.set(func() { h.froze = true }) }

func (h *holdListener) thaw() { h.set(func() { h.froze = false }) }

// release delivers every queued write, in order, and stops queueing.
func (h *holdListener) release() {
	h.mu.Lock()
	h.held = false
	conns := append([]*holdConn(nil), h.conns...)
	h.mu.Unlock()
	for _, c := range conns {
		c.flush()
	}
}

func (h *holdListener) releaseAll() {
	h.thaw()
	h.release()
}

type holdConn struct {
	net.Conn
	h      *holdListener
	queue  [][]byte
	busy   bool // a flush is delivering the queue
	closed bool
}

func (c *holdConn) Read(b []byte) (int, error) {
	c.h.mu.Lock()
	for c.h.froze && !c.closed {
		c.h.cond.Wait()
	}
	c.h.mu.Unlock()
	return c.Conn.Read(b)
}

func (c *holdConn) Write(b []byte) (int, error) {
	c.h.mu.Lock()
	for c.h.froze && !c.closed {
		c.h.cond.Wait()
	}
	if c.closed {
		c.h.mu.Unlock()
		return 0, net.ErrClosed
	}
	if c.h.held || c.busy || len(c.queue) > 0 {
		c.queue = append(c.queue, append([]byte(nil), b...))
		c.h.mu.Unlock()
		return len(b), nil
	}
	c.h.mu.Unlock()
	return c.Conn.Write(b)
}

func (c *holdConn) flush() {
	c.h.mu.Lock()
	if c.busy {
		c.h.mu.Unlock()
		return
	}
	c.busy = true
	for len(c.queue) > 0 && !c.h.held {
		b := c.queue[0]
		c.queue = c.queue[1:]
		c.h.mu.Unlock()
		_, err := c.Conn.Write(b)
		c.h.mu.Lock()
		if err != nil {
			c.queue = nil
		}
	}
	c.busy = false
	c.h.mu.Unlock()
}

func (c *holdConn) Close() error {
	c.h.set(func() { c.closed = true })
	return c.Conn.Close()
}

// TestLiveFreshnessLease defines the authoritative and local-fallback
// decisions and shows that authority comes only from a live lease: values
// without a lease, unpublished keys, and an expired or revoked lease all
// select local protection, and requests looking the output up never
// renew the lease (HAProxy still removes the marker on time).
func TestLiveFreshnessLease(t *testing.T) {
	f := newFresh(t, freshOptions{})
	gen := f.gen()
	f.publish(overClient, 1500)
	f.publish(underClient, 50)

	// Values without any lease: visible, never authoritative.
	p := f.waitProbe(overClient, "values taught without a lease", func(p lab.ProbeResponse) bool {
		return p.Version == output.SchemaVersion && p.Rate == 1500 && p.Gen == gen
	})
	checkLocal(t, "values without metadata", p)
	if p.MetaVersion != 0 || p.RelativeAuthority != "local" {
		t.Fatalf("values without metadata: %+v", p)
	}
	t.Logf("no lease: %+v", p)

	// A live lease: the aggregate limit applies to the published keys
	// only.
	stop := renewLease(t, f.store)
	p = f.waitProbe(overClient, "authority over the limit", authoritative(1500))
	t.Logf("leased, over the limit: %+v", p)
	p = f.waitProbe(underClient, "authority under the limit", authoritative(50))
	t.Logf("leased, under the limit: %+v", p)
	p = f.probe(otherClient)
	checkLocal(t, "unpublished key under a live lease", p)
	if p.Version != 0 || p.MetaGen != gen {
		t.Fatalf("unpublished key: %+v", p)
	}
	t.Logf("leased, unpublished key: %+v", p)

	// Publication stops while requests keep looking the output up every
	// 10 ms: authority ends with the lease, and the marker's own lifetime
	// still runs out.
	w := f.watch(overClient)
	time.Sleep(300 * time.Millisecond)
	last := stop()
	marker := status(t, f.m, "a").Stats
	var exps []time.Duration
	for range 8 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		tbl, err := f.n.ShowTable(ctx, lab.MetaTable)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		e, ok := tbl.Entry("::")
		if !ok {
			break
		}
		exps = append(exps, e.Exp)
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(time.Until(last.Add(authorityBound + time.Second)))
	ds := w.end()
	end := checkAuthorityEnds(t, "publication stopped, requests continuing", ds, last)
	if end < freshLease-100*time.Millisecond {
		t.Errorf("authority ended %v after the last renewal, before its %v lease", end, freshLease)
	}
	for i := 1; i < len(exps); i++ {
		if exps[i] >= exps[i-1] {
			t.Errorf("lab_meta :: expiry rose from %v to %v under lookups: %v", exps[i-1], exps[i], exps)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tbl, err := f.n.ShowTable(ctx, lab.MetaTable)
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := tbl.Entry("::"); ok {
		t.Errorf("lab_meta :: still present %v after the last renewal: %+v", time.Since(last), e)
	}
	t.Logf("last marker written %v after the last renewal (%d markers); lab_meta :: expiry under lookups %v, then gone",
		marker.LastMarker.Sub(last).Round(time.Millisecond), marker.Markers, exps)

	// Revocation ends authority at once.
	stop = renewLease(t, f.store)
	f.waitProbe(overClient, "authority again", authoritative(1500))
	w = f.watch(overClient)
	time.Sleep(200 * time.Millisecond)
	stop()
	revoked := time.Now()
	f.store.Revoke()
	time.Sleep(500 * time.Millisecond)
	ds = w.end()
	lastAgg := time.Duration(-1)
	for _, d := range ds {
		if since := d.sent.Sub(revoked); d.p.Aggregate() && since > lastAgg {
			lastAgg = since
		}
	}
	if lastAgg > 100*time.Millisecond {
		t.Errorf("revocation: aggregate decision sent %v after Revoke", lastAgg)
	}
	p = f.probe(overClient)
	checkLocal(t, "after revocation", p)
	if p.MetaVersion != output.SchemaVersion || p.MetaGen != 0 || p.RelativeAuthority != "local" {
		t.Fatalf("after revocation: %+v", p)
	}
	t.Logf("revoked: last aggregate decision sent %v after Revoke; now %+v", lastAgg.Round(time.Millisecond), p)
}

// TestLiveFreshnessExpiredKey shows that a key HAProxy has expired
// selects local protection under a live lease, even though the
// aggregator still holds its value: with the periodic refresh disabled,
// HAProxy expires aggregate entries not re-sent within the table's
// expire. With the refresh (a third of the expire by default), an
// unchanged key stays present and authoritative.
func TestLiveFreshnessExpiredKey(t *testing.T) {
	const period = time.Second // entries expire after 3 s
	t.Run("refresh disabled", func(t *testing.T) {
		f := newFresh(t, freshOptions{period: period, refresh: -1})
		defer renewLease(t, f.store)()
		f.publish(freshClient, 1500)
		published := time.Now()
		p := f.waitProbe(freshClient, "authority", authoritative(1500))
		t.Logf("published: %+v", p)
		p = f.waitProbe(freshClient, "the entry to expire", func(p lab.ProbeResponse) bool { return p.Version == 0 })
		checkLocal(t, "expired key under a live lease", p)
		if p.MetaVersion != output.SchemaVersion || p.MetaGen != f.gen() {
			t.Fatalf("lease not live while the key expired: %+v", p)
		}
		if v, ok := f.store.Get(lab.OutputTable, clientKey(t, freshClient)); !ok || v[output.SlotRate] != 1500 {
			t.Fatalf("store lost the value: %v %v", v, ok)
		}
		t.Logf("expired %v after publication (table expire %v): %+v", time.Since(published).Round(time.Millisecond),
			lab.ExpiryFactor*f.l.Period, p)
	})
	t.Run("refreshed", func(t *testing.T) {
		f := newFresh(t, freshOptions{period: period})
		defer renewLease(t, f.store)()
		f.publish(freshClient, 1500)
		f.waitProbe(freshClient, "authority", authoritative(1500))
		w := f.watch(freshClient)
		time.Sleep(3 * lab.ExpiryFactor * period)
		ds := w.end()
		for _, d := range ds {
			if !authoritative(1500)(d.p) {
				t.Fatalf("unchanged key not authoritative while refreshed: %+v", d.p)
			}
		}
		st := status(t, f.m, "a").Stats
		if st.Refreshes < 6 {
			t.Fatalf("%d refreshes in %v, want one per second", st.Refreshes, 3*lab.ExpiryFactor*period)
		}
		t.Logf("unchanged key authoritative in all %d decisions over three table expires; %d refreshes", len(ds),
			st.Refreshes)
	})
}

// TestLiveFreshnessStop ends publication by killing the aggregator and by
// pausing it (every read and write frozen, the connection left open):
// authority ends within the bound of the last renewal either way, and a
// resumed aggregator regains it.
func TestLiveFreshnessStop(t *testing.T) {
	t.Run("killed", func(t *testing.T) {
		f := newFresh(t, freshOptions{})
		f.publish(overClient, 1500)
		stop := renewLease(t, f.store)
		f.waitProbe(overClient, "authority", authoritative(1500))
		w := f.watch(overClient)
		time.Sleep(300 * time.Millisecond)
		last := stop()
		if err := f.m.Close(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(last.Add(authorityBound + time.Second)))
		checkAuthorityEnds(t, "aggregator killed", w.end(), last)
	})
	t.Run("paused", func(t *testing.T) {
		f := newFresh(t, freshOptions{})
		f.publish(overClient, 1500)
		stop := renewLease(t, f.store)
		f.waitProbe(overClient, "authority", authoritative(1500))
		w := f.watch(overClient)
		time.Sleep(300 * time.Millisecond)
		last := stop()
		f.ctl.freeze()
		time.Sleep(time.Until(last.Add(authorityBound + time.Second)))
		checkAuthorityEnds(t, "aggregator paused", w.end(), last)
		if st := showPeer(t, f.n); st.Status != "ESTA" {
			t.Fatalf("the paused session is not established on the node: %s", st.Status)
		}
		f.ctl.thaw()
		stop = renewLease(t, f.store)
		defer stop()
		p := f.waitProbe(overClient, "authority after resuming", authoritative(1500))
		if s := status(t, f.m, "a"); s.Session != 1 {
			t.Fatalf("resumed in session %d, want the paused session 1", s.Session)
		}
		t.Logf("resumed in the same session: %+v", p)
	})
}

// TestLiveFreshnessDelayedDelivery holds everything the aggregator writes
// to the node, as a stalled path or slow receiver would, while values
// change behind the hold. Authority ends within the bound of the last
// delivered renewal; markers delivered late cannot revive it for data that
// waited in the backlog past the bound, although the relative lifetime
// they carry would; and a live publisher's markers bring authority back
// only together with the values they certify.
func TestLiveFreshnessDelayedDelivery(t *testing.T) {
	const holdFor = 2800 * time.Millisecond
	for _, keep := range []bool{false, true} {
		name := "publisher stops during the hold"
		if keep {
			name = "publisher keeps publishing"
		}
		t.Run(name, func(t *testing.T) {
			f := newFresh(t, freshOptions{})
			f.publish(overClient, 1500)
			stop := renewLease(t, f.store)
			defer stop()
			f.waitProbe(overClient, "authority", authoritative(1500))
			w := f.watch(overClient)
			time.Sleep(200 * time.Millisecond)
			held := time.Now()
			f.ctl.hold()
			// Changes behind the hold: one key falls under the limit, a
			// new one rises above it.
			f.publish(overClient, 50)
			f.publish(otherClient, 5000)
			last := time.Now()
			if !keep {
				time.Sleep(300 * time.Millisecond)
				last = stop()
			}
			time.Sleep(time.Until(held.Add(holdFor)))
			released := time.Now()
			f.ctl.release()
			p := f.waitProbe(overClient, "the held values delivered", func(p lab.ProbeResponse) bool { return p.Rate == 50 })
			other := f.probe(otherClient)
			if keep {
				p = f.waitProbe(overClient, "authority with the delivered values", authoritative(50))
				other = f.waitProbe(otherClient, "authority with the delivered values", authoritative(5000))
			}
			time.Sleep(time.Second)
			ds := w.end()
			// The last renewal HAProxy got before the hold was written
			// before held, so authority ends within the bound of it. (A
			// probe sent just before the release may be answered after
			// the released bytes.)
			for _, d := range ds {
				if d.sent.After(held.Add(authorityBound)) && d.sent.Before(released.Add(-50*time.Millisecond)) &&
					d.p.Aggregate() {
					t.Errorf("aggregate decision sent %v into the hold: %+v", d.sent.Sub(held), d.p)
				}
				if d.p.Aggregate() && d.p.Rate == 1500 && d.sent.After(released) {
					t.Errorf("aggregate decision on the pre-hold value after the release: %+v", d.p)
				}
			}
			revived := 0
			for _, d := range ds {
				if d.sent.After(released) && d.p.RelativeAuthority == "aggregate" && !d.p.Aggregate() {
					revived++
				}
			}
			if !keep {
				checkAuthorityEnds(t, "publisher stopped during the hold", ds, held)
				checkLocal(t, "held value after the release", p)
				checkLocal(t, "held new key after the release", other)
				if other.Rate != 5000 || p.LeaseLeft <= output.LeaseWindowMillis {
					t.Fatalf("after the release: %+v, %+v", p, other)
				}
				if revived == 0 {
					t.Error("no decision showed the relative lifetime alone reviving authority after the release")
				}
				t.Logf("released %v after the hold and %v after the last renewal: %+v; %d decisions where the relative lifetime alone would have granted authority",
					holdFor, released.Sub(last).Round(time.Millisecond), p, revived)
			} else {
				t.Logf("released after %v: authority back with the delivered values: %+v, %+v", holdFor, p, other)
			}
		})
	}
}

// TestLiveFreshnessGeneration restarts the aggregator as a new
// incarnation that publishes only some of the old keys: entries an
// earlier session left in HAProxy select local protection under the new
// session's lease.
func TestLiveFreshnessGeneration(t *testing.T) {
	f := newFresh(t, freshOptions{})
	first := f.gen()
	f.publish(overClient, 1500)
	f.publish(underClient, 1500)
	stop := renewLease(t, f.store)
	f.waitProbe(overClient, "first session authority", authoritative(1500))
	f.waitProbe(underClient, "first session authority", authoritative(1500))
	stop()
	f.restart()
	second := f.gen()
	if second == first {
		t.Fatalf("two sessions share generation %d", first)
	}
	f.publish(overClient, 1500)
	defer renewLease(t, f.store)()
	p := f.waitProbe(overClient, "second session authority", func(p lab.ProbeResponse) bool {
		return authoritative(1500)(p) && p.Gen == second
	})
	t.Logf("republished key: %+v", p)
	p = f.probe(underClient)
	checkLocal(t, "key left by the previous session", p)
	if p.Version != output.SchemaVersion || p.Rate != 1500 || p.Gen != first || p.MetaGen != second {
		t.Fatalf("key left by the previous session: %+v", p)
	}
	t.Logf("key left by the previous session: %+v", p)
}

// TestLiveFreshnessReload reloads HAProxy during an aggregator outage,
// while the node still holds a live marker: the new process learns the
// marker and output from the old one and keeps the authority the marker
// granted only until its deadline. A resync with an aggregator that has
// no lease, and another reload, do not revive it.
func TestLiveFreshnessReload(t *testing.T) {
	const lease = output.MaxLeaseLength
	f := newFresh(t, freshOptions{reloadable: true})
	f.publish(overClient, 1500)
	stop := renewLease(t, f.store)
	f.waitProbe(overClient, "authority", authoritative(1500))
	oldPID := f.n.PID()
	gen := f.gen()
	w := f.watch(overClient)
	time.Sleep(300 * time.Millisecond)
	last := stop()
	if err := f.m.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.n.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	reloaded := time.Now()
	newPID := f.n.PID()
	// The old process teaches the new one table by table, with timed
	// updates: the marker arrives with whatever it has left, not a
	// restarted lifetime.
	var marker lab.Entry
	learned := false
	for !learned && time.Now().Before(last.Add(lease)) {
		tbl, err := f.n.ShowTable(ctx, lab.MetaTable)
		if err != nil {
			t.Fatal(err)
		}
		marker, learned = tbl.Entry("::")
		if !learned {
			time.Sleep(10 * time.Millisecond)
		}
	}
	seen := time.Now()
	out, err := f.n.Runtime(ctx, "show table "+lab.OutputTable)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reloaded %v after the last renewal: pid %d -> %d; new process marker %+v (learned %v, seen %v after the last renewal)\n%s",
		reloaded.Sub(last).Round(time.Millisecond), oldPID, newPID, marker, learned,
		seen.Sub(last).Round(time.Millisecond), out)
	if !learned {
		t.Fatal("the new process never held the marker while it was live; the reload was too slow to test inheritance")
	}
	if left := lease - seen.Sub(last); marker.Exp > left+50*time.Millisecond {
		t.Errorf("the reload restarted the marker's lifetime: %v left, at most %v expected", marker.Exp, left)
	}
	if marker.Data["gpt2"] != gen {
		t.Errorf("new process marker %+v, want generation %d", marker, gen)
	}
	time.Sleep(time.Until(last.Add(authorityBound + time.Second)))
	ds := w.end()
	checkAuthorityEnds(t, "reload during the outage", ds, last)
	inherited, newLocal := 0, 0
	for _, d := range ds {
		if d.p.PID != int64(newPID) {
			continue
		}
		if d.p.Aggregate() {
			inherited++
		} else {
			newLocal++
		}
	}
	if inherited == 0 || newLocal == 0 {
		t.Fatalf("new process: %d aggregate and %d local decisions, want both", inherited, newLocal)
	}
	t.Logf("new process: %d aggregate decisions from the inherited marker, then %d local", inherited, newLocal)

	// The aggregator returns without a lease: the node resynchronizes,
	// replaying its copy of the stale marker, and gets the output taught
	// again, but authority stays local, across another reload too.
	f.restart()
	f.publish(overClient, 1500)
	w = f.watch(overClient)
	f.waitProbe(overClient, "values taught after the outage", func(p lab.ProbeResponse) bool {
		return p.Gen == f.gen()
	})
	if err := f.n.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	f.r.waitFor("the reloaded node's session", 30*time.Second, func(evs []sources.Event) bool {
		return count[peersession.SyncFinished](evs, "a") >= 2
	})
	time.Sleep(time.Second)
	for _, d := range w.end() {
		if d.p.Aggregate() {
			t.Fatalf("aggregate decision without a lease after the outage: %+v", d.p)
		}
	}
	st := status(t, f.m, "a").Stats
	t.Logf("no lease after the outage: always local; node replayed %d output entries, %d markers sent",
		st.EchoedUpdates, st.Markers)
	defer renewLease(t, f.store)()
	p := f.waitProbe(overClient, "authority after the outage", authoritative(1500))
	t.Logf("lease restored: %+v", p)
}

// TestLiveFreshnessSkew runs the aggregator with its wall clock offset
// from HAProxy's. Within the skew bound (2 s minus the lease, here 1 s),
// authority holds while published and ends within the bound. Just past it
// (by less than a renewal interval plus delivery) authority flaps while
// published; further past it is never granted while published. Either
// way it still ends within the bound once publication stops.
func TestLiveFreshnessSkew(t *testing.T) {
	for _, tc := range []struct {
		name    string
		skew    time.Duration
		inBound bool
		flaps   bool // just past the bound: authority comes and goes
	}{
		{"ahead within the bound", 900 * time.Millisecond, true, false},
		{"behind within the bound", -500 * time.Millisecond, true, false},
		{"ahead just beyond the bound", 1100 * time.Millisecond, false, true},
		{"ahead beyond the bound", 1500 * time.Millisecond, false, false},
		{"behind beyond the lease", -1500 * time.Millisecond, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFresh(t, freshOptions{skew: tc.skew})
			f.publish(overClient, 1500)
			stop := renewLease(t, f.store)
			defer stop()
			if tc.inBound {
				f.waitProbe(overClient, "authority", authoritative(1500))
			} else {
				f.waitProbe(overClient, "the lease marker", func(p lab.ProbeResponse) bool {
					return p.MetaGen == f.gen() && p.Rate == 1500
				})
			}
			w := f.watch(overClient)
			time.Sleep(1500 * time.Millisecond)
			published := time.Now()
			last := stop()
			if err := f.m.Close(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Until(last.Add(authorityBound + time.Second)))
			ds := w.end()
			agg, during := 0, 0
			var left int64
			for _, d := range ds {
				if d.sent.Before(published) {
					during++
					left = d.p.LeaseLeft
					if d.p.Aggregate() {
						agg++
					}
				}
			}
			if tc.inBound && agg != during {
				t.Errorf("only %d of %d decisions while published were aggregate", agg, during)
			}
			if !tc.inBound && !tc.flaps && agg != 0 {
				t.Errorf("%d of %d decisions while published were aggregate", agg, during)
			}
			t.Logf("skew %v: %d of %d decisions aggregate while published; the last saw %d ms left (mod 2^32)",
				tc.skew, agg, during, left)
			checkAuthorityEnds(t, "skewed aggregator killed", ds, last)
		})
	}
}

// TestLiveFreshnessRetire retires one of two published keys under a live
// lease: the session switches generation and re-sends the other, so the
// retired key's copy in HAProxy is never authoritative again, and, no
// longer refreshed, HAProxy expires it after the table's expire. The key
// still published keeps its authority.
func TestLiveFreshnessRetire(t *testing.T) {
	const period = time.Second // entries expire after 3 s, refresh every 1 s
	f := newFresh(t, freshOptions{period: period})
	defer renewLease(t, f.store)()
	f.publish(overClient, 1500)
	f.publish(otherClient, 1500)
	f.waitProbe(overClient, "authority", authoritative(1500))
	before := f.waitProbe(otherClient, "authority", authoritative(1500))
	w := f.watch(otherClient)
	kept := f.watch(overClient)
	retired := time.Now()
	if err := f.m.Retire(lab.OutputTable, clientKey(t, otherClient)); err != nil {
		t.Fatal(err)
	}
	gone := f.waitProbe(otherClient, "HAProxy to expire the retired key", func(p lab.ProbeResponse) bool {
		return p.Version == 0
	})
	goneAt := time.Now()
	time.Sleep(time.Second)
	ds, keptDs := w.end(), kept.end()
	expire := lab.ExpiryFactor * period
	lastAgg, oldCopy := time.Duration(-1), 0
	for _, d := range ds {
		since := d.sent.Sub(retired)
		if d.p.Aggregate() && since > lastAgg {
			lastAgg = since
		}
		if d.p.Version != 0 {
			oldCopy++
			if d.p.Gen != before.Gen {
				t.Errorf("the retired key was re-sent: %+v", d.p)
			}
		}
	}
	if lastAgg > 100*time.Millisecond {
		t.Errorf("the retired key was aggregate %v after Retire", lastAgg)
	}
	if took := goneAt.Sub(retired); took > expire+time.Second {
		t.Errorf("HAProxy kept the retired key %v, beyond its %v expire", took, expire)
	}
	keptLocal := 0
	for _, d := range keptDs {
		if !d.p.Aggregate() {
			keptLocal++
		}
	}
	if last := keptDs[len(keptDs)-1].p; !authoritative(1500)(last) || last.Gen == before.Gen || keptLocal > 5 {
		t.Fatalf("the published key after the rotation: %d of %d decisions local, last %+v", keptLocal,
			len(keptDs), last)
	}
	st := status(t, f.m, "a").Stats
	t.Logf("retired key: %d decisions still saw its old-generation copy, none aggregate after %v; gone from HAProxy %v after Retire (expire %v): %+v; kept key: %d of %d decisions local around the rotation; %d rotations",
		oldCopy, lastAgg.Round(time.Millisecond), goneAt.Sub(retired).Round(time.Millisecond), expire, gone, keptLocal,
		len(keptDs), st.Rotations)
}
