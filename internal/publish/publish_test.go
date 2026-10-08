package publish_test

import (
	"math"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/aggregate"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/publish"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

const (
	inTable  = "t_in"
	outTable = "t_out"
	period   = peermsg.Millis(10000)
	healthTO = 5 * time.Second
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// harness drives a two-source snapshot store under a fake clock through a
// Publisher, whose passes the test runs directly.
type harness struct {
	t     *testing.T
	clock *fakeClock
	snap  *snapshot.Store
	out   *output.Store
	pub   *publish.Publisher
	ids   map[string]peermsg.UpdateID
}

func newHarness(t *testing.T, opts publish.Options) *harness {
	t.Helper()
	c := &fakeClock{t: time.Now()}
	snap, err := snapshot.New(snapshot.Options{
		Sources: []string{"a", "b"}, Tables: []snapshot.Table{{Name: inTable, Period: period}},
		HealthTimeout: healthTO, MaxSourceEntries: 64, Now: c.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := output.NewStore([]output.Table{
		{Name: outTable, Kind: output.KindAggregate, Expiry: 3 * period},
		{Name: "t_meta", Kind: output.KindMetadata, Expiry: 2000},
	}, output.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	opts.Snapshot, opts.Output, opts.Now = snap, out, c.Now
	opts.Routes = []publish.Route{{Input: inTable, Output: outTable}}
	pub, err := publish.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, clock: c, snap: snap, out: out, pub: pub, ids: map[string]peermsg.UpdateID{}}
}

func (h *harness) apply(src string, session uint64, body peersession.Event) {
	h.t.Helper()
	if err := h.pub.Apply(sources.Event{Source: src, Session: session, Body: body}); err != nil {
		h.t.Fatalf("%s#%d %T: %v", src, session, body, err)
	}
}

func (h *harness) def() peermsg.Definition {
	return peermsg.Definition{
		Name: inTable, KeyType: peermsg.KeyTypeIPv6, Expiry: 3 * period,
		Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: period}},
	}
}

// sync brings source src's session 1 to Ready.
func (h *harness) sync(src string) {
	h.t.Helper()
	now := h.clock.Now()
	h.apply(src, 1, peersession.SessionUp{Direction: peersession.Inbound, At: now})
	h.apply(src, 1, peersession.TableDefined{ID: 1, Definition: h.def(), Received: now})
	h.apply(src, 1, peersession.SyncFinished{Received: now})
}

// counter delivers source src's counter for k: curr events in a period
// that started age ms ago, none in the period before.
func (h *harness) counter(src string, k peermsg.Key, curr uint32, age peermsg.Millis) {
	h.t.Helper()
	h.ids[src]++
	h.apply(src, 1, peersession.EntryUpdated{ID: 1, Table: inTable, Expiry: 3 * period, Update: peermsg.Update{
		ID: h.ids[src], Key: k, Received: h.clock.Now(),
		Values: []peermsg.Value{
			{Type: peermsg.DataHTTPReqCnt, Uint: curr},
			{Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Age: age, Curr: curr}},
		},
	}})
}

func (h *harness) heartbeat() {
	for _, s := range []string{"a", "b"} {
		h.snap.Observe(s, 1, h.clock.Now())
	}
}

// published returns k's published rate, or false if k is not published.
func (h *harness) published(k peermsg.Key) (uint32, bool) {
	v, ok := h.out.Get(outTable, k)
	if ok && (v[output.SlotVersion] != output.SchemaVersion || v[output.SlotGeneration] != 0) {
		h.t.Fatalf("published %v", v)
	}
	return v[output.SlotRate], ok
}

func key(t *testing.T, s string) peermsg.Key {
	t.Helper()
	k, err := peermsg.KeyFromAddr(netip.MustParseAddr(s))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// TestReadinessGatesPublication checks that nothing is written or leased
// until every source is Ready, that the published value is the sum of the
// sources' estimates, and that losing a source revokes the lease once and
// stops publication until the roster is ready again.
func TestReadinessGatesPublication(t *testing.T) {
	h := newHarness(t, publish.Options{})
	k := key(t, "2001:db8:1::")
	h.sync("a")
	h.counter("a", k, 6, 0)
	h.pub.Pass()
	if _, ok := h.published(k); ok || h.out.Lease().Gen != 0 {
		t.Fatalf("published with b missing: lease %+v", h.out.Lease())
	}
	if st := h.pub.Stats(); st.Ready || len(st.NotReady) != 1 || st.Leased {
		t.Fatalf("stats %+v", st)
	}
	h.sync("b")
	h.counter("b", k, 7, 0)
	h.pub.Pass()
	if v, ok := h.published(k); !ok || v != 13 {
		t.Fatalf("published %d %v, want 13", v, ok)
	}
	l := h.out.Lease()
	if !l.Valid || l.Gen != 1 || time.Until(l.Until) <= 0 || time.Until(l.Until) > publish.DefaultLease {
		t.Fatalf("lease %+v", l)
	}
	h.apply("b", 1, peersession.SessionDown{At: h.clock.Now()})
	h.counter("a", k, 50, 0)
	h.pub.Pass()
	h.pub.Pass()
	if l := h.out.Lease(); l.Valid || l.Gen != 2 {
		t.Fatalf("after b left: lease %+v, want one revocation", l)
	}
	if v, _ := h.published(k); v != 13 {
		t.Fatalf("published %d while b was missing", v)
	}
	if st := h.pub.Stats(); st.Revocations != 1 || st.Leased || st.Ready {
		t.Fatalf("stats %+v", st)
	}
	// b's next session: every key is reevaluated and leased again.
	h.apply("b", 2, peersession.SessionUp{Direction: peersession.Inbound, At: h.clock.Now()})
	h.apply("b", 2, peersession.TableDefined{ID: 1, Definition: h.def(), Received: h.clock.Now()})
	h.pub.Pass()
	if v, _ := h.published(k); v != 13 || h.out.Lease().Valid {
		t.Fatalf("published %d, lease %+v while b syncs", v, h.out.Lease())
	}
	// b's teach re-sends its entry, which the finished resync confirms.
	h.apply("b", 2, peersession.EntryUpdated{ID: 1, Table: inTable, Expiry: 3 * period, Update: peermsg.Update{
		ID: 1, Key: k, Received: h.clock.Now(), Timed: true, Remaining: 3*period - 1000,
		Values: []peermsg.Value{
			{Type: peermsg.DataHTTPReqCnt, Uint: 7},
			{Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Curr: 7}},
		},
	}})
	h.apply("b", 2, peersession.SyncFinished{Received: h.clock.Now()})
	h.pub.Pass()
	if v, _ := h.published(k); v != 57 || !h.out.Lease().Valid {
		t.Fatalf("published %d, lease %+v; want 57 leased", v, h.out.Lease())
	}
	if st := h.pub.Stats(); st.Uncertain != 0 || st.Published != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// TestLostHistoryRevokes checks that a source whose next session's
// snapshot lacks an entry it held (as after a cold restart) keeps the
// lease revoked, publishing nothing new, until the lost entry's rate
// would have decayed to 0, and that every key is then reevaluated and
// leased again without the lost contribution.
func TestLostHistoryRevokes(t *testing.T) {
	h := newHarness(t, publish.Options{})
	k := key(t, "2001:db8:1::")
	h.sync("a")
	h.sync("b")
	h.counter("a", k, 6, 0)
	h.counter("b", k, 7, 0)
	h.pub.Pass()
	if v, _ := h.published(k); v != 13 || !h.out.Lease().Valid {
		t.Fatalf("published %d, lease %+v", v, h.out.Lease())
	}
	h.apply("b", 1, peersession.SessionDown{At: h.clock.Now()})
	h.clock.Advance(time.Second)
	now := h.clock.Now()
	h.apply("b", 2, peersession.SessionUp{Direction: peersession.Inbound, At: now})
	h.apply("b", 2, peersession.TableDefined{ID: 1, Definition: h.def(), Received: now})
	h.apply("b", 2, peersession.SyncFinished{Received: now})
	rep, _ := h.snap.Source("b")
	if rep.State != snapshot.Degraded || rep.Loss.Absent != 1 {
		t.Fatalf("b after an empty snapshot: %+v", rep)
	}
	until := rep.Loss.RateUntil
	beat := func() {
		h.snap.Observe("a", 1, h.clock.Now())
		h.snap.Observe("b", 2, h.clock.Now())
	}
	for h.clock.Now().Add(4 * time.Second).Before(until) {
		h.clock.Advance(4 * time.Second)
		beat()
		h.pub.Pass()
		if h.out.Lease().Valid {
			t.Fatalf("leased %v before the lost history decayed", until.Sub(h.clock.Now()))
		}
	}
	h.clock.Advance(until.Sub(h.clock.Now()))
	beat()
	h.pub.Pass()
	// a's estimate alone, evaluated now; b's lost 7 is not restored.
	r, err := aggregate.Rate(h.snap, inTable, k)
	if err != nil || !r.Complete {
		t.Fatalf("rate %+v, %v", r, err)
	}
	if v, _ := h.published(k); uint64(v) != r.Sum || !h.out.Lease().Valid {
		t.Fatalf("published %d, lease %+v; want %d leased", v, h.out.Lease(), r.Sum)
	}
}

// TestHealthRevokes checks that a source going silent past the health
// bound, with no event at all, revokes the lease at the next pass.
func TestHealthRevokes(t *testing.T) {
	h := newHarness(t, publish.Options{})
	h.sync("a")
	h.sync("b")
	h.pub.Pass()
	if !h.out.Lease().Valid {
		t.Fatal("not leased")
	}
	h.clock.Advance(healthTO - time.Millisecond)
	h.heartbeat()
	h.clock.Advance(healthTO - time.Millisecond)
	h.pub.Pass()
	if l := h.out.Lease(); !l.Valid {
		t.Fatalf("heartbeats kept the roster: lease %+v", l)
	}
	h.clock.Advance(time.Millisecond)
	h.pub.Pass()
	if l := h.out.Lease(); l.Valid {
		t.Fatalf("silent sources: lease %+v", l)
	}
}

// TestDirtyAndDue checks that a pass evaluates only keys whose input
// changed or whose rate is due to decay, that rates decay without input
// and are reevaluated at their due times, and that zero-rate keys are
// retired in a batch.
func TestDirtyAndDue(t *testing.T) {
	h := newHarness(t, publish.Options{RetireInterval: time.Hour})
	k1, k2 := key(t, "2001:db8:1::"), key(t, "2001:db8:2::")
	h.sync("a")
	h.sync("b")
	h.counter("a", k1, 100, 0)
	h.counter("b", k2, 1, 0)
	h.pub.Pass()
	if v, _ := h.published(k1); v != 100 {
		t.Fatalf("k1 %d", v)
	}
	if v, _ := h.published(k2); v != 1 {
		t.Fatalf("k2 %d", v)
	}
	ev := h.pub.Stats().Evaluations
	h.counter("a", k1, 120, 0)
	h.pub.Pass()
	if got := h.pub.Stats().Evaluations - ev; got != 1 {
		t.Fatalf("%d evaluations after one dirty key", got)
	}
	if v, _ := h.published(k1); v != 120 {
		t.Fatalf("k1 %d", v)
	}
	// Silence: after the period rotates, k1 decays proportionally.
	for range 15 {
		h.clock.Advance(time.Second)
		h.heartbeat()
		h.pub.Pass()
	}
	v, _ := h.published(k1)
	if want := uint32(120 * 5 / 10); v != want {
		t.Fatalf("k1 after 15 s: %d, want %d", v, want)
	}
	for range 10 {
		h.clock.Advance(time.Second)
		h.heartbeat()
		h.pub.Pass()
	}
	if v, ok := h.published(k1); !ok || v != 0 {
		t.Fatalf("k1 after 25 s: %d %v, want a published 0", v, ok)
	}
	// k2 (one event) reads 1 until two periods passed, then 0.
	if v, _ := h.published(k2); v != 0 {
		t.Fatalf("k2 after 25 s: %d", v)
	}
	if st := h.pub.Stats(); st.Retired != 0 || st.Published != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// TestRetireIdle checks that keys whose rate decayed to 0 leave the
// output in one batch, and come back when their input does.
func TestRetireIdle(t *testing.T) {
	h := newHarness(t, publish.Options{RetireInterval: time.Millisecond})
	k1, k2 := key(t, "2001:db8:1::"), key(t, "2001:db8:2::")
	h.sync("a")
	h.sync("b")
	h.counter("a", k1, 5, 0)
	h.counter("a", k2, 0, 0)
	h.pub.Pass()
	time.Sleep(2 * time.Millisecond)
	h.pub.Pass()
	if _, ok := h.published(k2); ok {
		t.Fatal("zero-rate k2 not retired")
	}
	if v, ok := h.published(k1); !ok || v != 5 {
		t.Fatalf("k1 %d %v", v, ok)
	}
	for range 25 {
		h.clock.Advance(time.Second)
		h.heartbeat()
		time.Sleep(2 * time.Millisecond)
		h.pub.Pass()
	}
	if _, ok := h.published(k1); ok {
		t.Fatal("decayed k1 not retired")
	}
	if st := h.pub.Stats(); st.Retired != 2 || st.Published != 0 {
		t.Fatalf("stats %+v", st)
	}
	h.counter("b", k1, 3, 0)
	h.pub.Pass()
	if v, ok := h.published(k1); !ok || v != 3 {
		t.Fatalf("k1 back: %d %v", v, ok)
	}
}

// TestOverflowSaturated checks that a complete rate beyond the 32-bit
// slot is published as math.MaxUint32, never wrapped or withdrawn, is
// counted once, and is published exactly again once it fits.
func TestOverflowSaturated(t *testing.T) {
	h := newHarness(t, publish.Options{})
	k := key(t, "2001:db8:1::")
	h.sync("a")
	h.sync("b")
	h.counter("a", k, 10, 0)
	h.pub.Pass()
	if v, _ := h.published(k); v != 10 {
		t.Fatalf("published %d", v)
	}
	h.counter("a", k, math.MaxUint32, 0)
	h.counter("b", k, 1, 0)
	h.pub.Pass()
	if v, ok := h.published(k); !ok || v != math.MaxUint32 {
		t.Fatalf("overflowing rate: %d %v, want saturated", v, ok)
	}
	if st := h.pub.Stats(); st.Overflows != 1 || st.Saturated != 1 || st.Retired != 0 || !st.Leased {
		t.Fatalf("stats %+v", st)
	}
	h.counter("b", k, 2, 0)
	h.pub.Pass()
	if st := h.pub.Stats(); st.Overflows != 1 || st.Saturated != 1 {
		t.Fatalf("still saturated: stats %+v", st)
	}
	h.counter("b", k, 0, 0)
	h.pub.Pass()
	if v, ok := h.published(k); !ok || v != math.MaxUint32 {
		t.Fatalf("fitting rate: %d %v", v, ok)
	}
	if st := h.pub.Stats(); st.Saturated != 0 || st.Overflows != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// TestUncertainVisible checks that an uncertain rate (an out-of-range
// counter age) is published and counted.
func TestUncertainVisible(t *testing.T) {
	h := newHarness(t, publish.Options{})
	k := key(t, "2001:db8:1::")
	h.sync("a")
	h.sync("b")
	h.counter("a", k, 4, 0)
	h.counter("b", k, 9, 4_000_000_000)
	h.pub.Pass()
	if v, ok := h.published(k); !ok || v != 4 {
		t.Fatalf("published %d %v", v, ok)
	}
	if st := h.pub.Stats(); st.Uncertain != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// TestRenewal checks that a ready roster renews the lease every Renew and
// not more often, and that each lease is derived from the time its pass
// started.
func TestRenewal(t *testing.T) {
	h := newHarness(t, publish.Options{Lease: 200 * time.Millisecond, Renew: 50 * time.Millisecond})
	h.sync("a")
	h.sync("b")
	before := time.Now()
	h.pub.Pass()
	l := h.out.Lease()
	if l.Until.Before(before.Add(200*time.Millisecond)) || l.Until.After(time.Now().Add(200*time.Millisecond)) {
		t.Fatalf("lease until %v, pass started after %v", l.Until, before)
	}
	h.pub.Pass()
	if h.out.Lease().Gen != 1 {
		t.Fatal("renewed early")
	}
	time.Sleep(60 * time.Millisecond)
	h.heartbeat()
	h.pub.Pass()
	if h.out.Lease().Gen != 2 {
		t.Fatal("not renewed")
	}
}

// TestRunPublishes runs the loop: input published promptly, renewals
// while ready, and a revocation when Run stops.
func TestRunPublishes(t *testing.T) {
	h := newHarness(t, publish.Options{Lease: 200 * time.Millisecond})
	k := key(t, "2001:db8:1::")
	h.sync("a")
	h.sync("b")
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		h.pub.Run(done)
	}()
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	h.counter("a", k, 42, 0)
	for {
		if v, _ := h.published(k); v == 42 {
			break
		}
		if time.Since(start) > time.Second {
			t.Fatal("input not published")
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("published %v after the update", time.Since(start))
	time.Sleep(200 * time.Millisecond)
	if g := h.out.Lease().Gen; g < 3 {
		t.Fatalf("lease generation %d after 200 ms with a 50 ms renewal", g)
	}
	close(done)
	<-stopped
	if h.out.Lease().Valid {
		t.Fatal("lease survives Run")
	}
}

func TestInvalidOptions(t *testing.T) {
	h := newHarness(t, publish.Options{})
	for name, o := range map[string]publish.Options{
		"no stores":     {},
		"long lease":    {Snapshot: h.snap, Output: h.out, Lease: 2 * time.Second},
		"slow renewal":  {Snapshot: h.snap, Output: h.out, Lease: time.Second, Renew: 300 * time.Millisecond},
		"unknown input": {Snapshot: h.snap, Output: h.out, Routes: []publish.Route{{Input: "x", Output: outTable}}},
		"metadata out":  {Snapshot: h.snap, Output: h.out, Routes: []publish.Route{{Input: inTable, Output: "t_meta"}}},
		"twice": {Snapshot: h.snap, Output: h.out, Routes: []publish.Route{
			{Input: inTable, Output: outTable}, {Input: inTable, Output: outTable},
		}},
	} {
		if _, err := publish.New(o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestRetireIdleBatch checks that one retirement pass removes all its
// zero-rate keys from the output in one store change.
func TestRetireIdleBatch(t *testing.T) {
	h := newHarness(t, publish.Options{RetireInterval: time.Millisecond})
	h.sync("a")
	h.sync("b")
	var ks []peermsg.Key
	for i := 1; i <= 3; i++ {
		k := key(t, "2001:db8:"+string(rune('0'+i))+"::")
		ks = append(ks, k)
		h.counter("a", k, 0, 0)
	}
	// The first pass publishes the three zero rates and, its retirement
	// being due, retires them together.
	h.pub.Pass()
	if got := h.out.Retired(); got != 3 {
		t.Fatalf("retired %d keys, want 3", got)
	}
	for _, k := range ks {
		if _, ok := h.published(k); ok {
			t.Fatalf("%v still published", k)
		}
	}
	if st := h.pub.Stats(); st.Retired != 3 || st.Published != 0 {
		t.Fatalf("stats %+v", st)
	}
}
