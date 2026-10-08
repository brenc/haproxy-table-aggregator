package aggregate_test

import (
	"errors"
	"maps"
	"math"
	"math/rand/v2"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/aggregate"
	"github.com/brenc/haproxy-table-aggregator/internal/aggregate/aggregatetest"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

const (
	inTable  = "t_in"
	inTable2 = "t_in2"
	period   = peermsg.Millis(10000)
	expiry   = peermsg.Millis(30000)
	healthTO = 5 * time.Second
)

func definition(name string) peermsg.Definition {
	return peermsg.Definition{
		Name: name, KeyType: peermsg.KeyTypeIPv6, Expiry: expiry,
		Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: period}},
	}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// harness drives a snapshot store with events stamped by a fake clock and
// records every event, accepted or refused, in an independent oracle.
// Every total it reads is checked against the oracle.
type harness struct {
	t      *testing.T
	clock  *fakeClock
	s      *snapshot.Store
	o      *aggregatetest.Oracle
	roster []string
}

func newHarness(t *testing.T, capacity int, srcs ...string) *harness {
	t.Helper()
	c := &fakeClock{t: time.Now()}
	s, err := snapshot.New(snapshot.Options{
		Sources:       srcs,
		Tables:        []snapshot.Table{{Name: inTable, Period: period}, {Name: inTable2, Period: period}},
		HealthTimeout: healthTO, MaxSourceEntries: capacity, Now: c.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		t: t, clock: c, s: s, roster: srcs,
		o: &aggregatetest.Oracle{
			Health: healthTO, Tables: []string{inTable, inTable2}, Capacity: capacity, Grace: snapshot.LossGrace,
			Periods: map[string]uint32{inTable: uint32(period), inTable2: uint32(period)},
		},
	}
}

func (h *harness) apply(src string, session uint64, body peersession.Event) error {
	ev := sources.Event{Source: src, Session: session, Body: body}
	err := h.s.Apply(ev)
	h.o.Record(ev, err)
	return err
}

func (h *harness) must(src string, session uint64, body peersession.Event) {
	h.t.Helper()
	if err := h.apply(src, session, body); err != nil {
		h.t.Fatalf("%s#%d %T: %v", src, session, body, err)
	}
}

func (h *harness) up(src string, session uint64) {
	h.t.Helper()
	h.must(src, session, peersession.SessionUp{Direction: peersession.Inbound, At: h.clock.Now()})
}

func (h *harness) defineAll(src string, session uint64) {
	h.t.Helper()
	h.must(src, session, peersession.TableDefined{ID: 1, Definition: definition(inTable), Received: h.clock.Now()})
	h.must(src, session, peersession.TableDefined{ID: 2, Definition: definition(inTable2), Received: h.clock.Now()})
}

func (h *harness) finish(src string, session uint64, partial bool) {
	h.t.Helper()
	h.must(src, session, peersession.SyncFinished{Partial: partial, Received: h.clock.Now()})
}

func (h *harness) down(src string, session uint64) {
	h.t.Helper()
	h.must(src, session, peersession.SessionDown{Err: errors.New("closed"), At: h.clock.Now()})
}

// sync brings a source's session 1 to a finished resync with both input
// tables defined and nothing taught.
func (h *harness) sync(src string) {
	h.t.Helper()
	h.up(src, 1)
	h.defineAll(src, 1)
	h.finish(src, 1, false)
}

// observe records a message read now (a heartbeat) for the session.
func (h *harness) observe(src string, session uint64) {
	h.s.Observe(src, session, h.clock.Now())
	h.o.Observe(src, session, h.clock.Now())
}

func key(t *testing.T, s string) peermsg.Key {
	t.Helper()
	k, err := peermsg.KeyFromAddr(netip.MustParseAddr(s))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// upd is an ordinary update of table read now. tableID is the
// session-local ID, which plays no part in identity.
func (h *harness) upd(table string, tableID peermsg.RemoteTableID, k peermsg.Key, cnt uint32) peersession.EntryUpdated {
	return peersession.EntryUpdated{ID: tableID, Table: table, Expiry: expiry, Update: peermsg.Update{
		ID: 1, Key: k, Received: h.clock.Now(),
		Values: []peermsg.Value{
			{Type: peermsg.DataHTTPReqCnt, Uint: cnt},
			{Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Age: 10, Curr: 1}},
		},
	}}
}

// set applies an ordinary update of t_in.
func (h *harness) set(src string, session uint64, k peermsg.Key, cnt uint32) {
	h.t.Helper()
	h.must(src, session, h.upd(inTable, 1, k, cnt))
}

// teach applies a timed update of t_in, as a resync replay sends it.
func (h *harness) teach(src string, session uint64, k peermsg.Key, cnt uint32, remaining peermsg.Millis) {
	h.t.Helper()
	u := h.upd(inTable, 1, k, cnt)
	u.Update.Timed, u.Update.Remaining = true, remaining
	h.must(src, session, u)
}

// total reads the total of table and key and checks it against the
// oracle: the same sum, completeness, uncertainty, and counted values.
func (h *harness) total(table string, k peermsg.Key) aggregate.Total {
	h.t.Helper()
	got, err := aggregate.Count(h.s, table, k)
	if err != nil {
		h.t.Fatal(err)
	}
	x := h.o.Want(h.roster, table, k, h.clock.Now())
	gotCounted := map[string]uint32{}
	for _, c := range got.Sources {
		if c.Counted {
			gotCounted[c.Source] = c.Count
		}
	}
	if got.Sum != x.Sum || got.Complete != x.Complete || got.Uncertain != x.Uncertain || !maps.Equal(gotCounted, x.Counted) {
		h.t.Fatalf("%s %v: total %d complete %v uncertain %v counted %v; oracle %+v",
			table, k, got.Sum, got.Complete, got.Uncertain, gotCounted, x)
	}
	if len(got.Sources) != len(h.roster) {
		h.t.Fatalf("%d sources reported, roster has %d", len(got.Sources), len(h.roster))
	}
	return got
}

// want checks t_in's total for k.
func (h *harness) want(k peermsg.Key, sum uint64, complete bool) aggregate.Total {
	h.t.Helper()
	got := h.total(inTable, k)
	if got.Sum != sum || got.Complete != complete {
		h.t.Fatalf("total %d complete %v, want %d complete %v; sources %+v", got.Sum, got.Complete, sum, complete,
			got.Sources)
	}
	return got
}

func contribution(t *testing.T, tot aggregate.Total, src string) aggregate.Contribution {
	t.Helper()
	for _, c := range tot.Sources {
		if c.Source == src {
			return c
		}
	}
	t.Fatalf("no source %s in %+v", src, tot.Sources)
	return aggregate.Contribution{}
}

// TestReplacementNotAdditive: A=10, B=20, then A=11 yields 30 then 31;
// replaying A=11 stays 31.
func TestReplacementNotAdditive(t *testing.T) {
	h := newHarness(t, 16, "a", "b")
	k := key(t, "2001:db8::")
	h.sync("a")
	h.sync("b")
	h.want(k, 0, true)
	h.set("a", 1, k, 10)
	h.want(k, 10, true)
	h.set("b", 1, k, 20)
	h.want(k, 30, true)
	h.clock.Advance(time.Second)
	h.set("a", 1, k, 11)
	h.want(k, 31, true)
	h.set("a", 1, k, 11)
	tot := h.want(k, 31, true)
	if tot.Uncertain {
		t.Fatalf("increasing values reported uncertain: %+v", tot)
	}
	// The same record replayed by a resync in a new session.
	h.up("a", 2)
	h.defineAll("a", 2)
	h.teach("a", 2, k, 11, 29000)
	h.finish("a", 2, false)
	h.want(k, 31, true)
}

// TestEachIncrementsOnce: A=100 and B=100 each increment once, producing
// 202 after convergence, in either arrival order.
func TestEachIncrementsOnce(t *testing.T) {
	for _, order := range [][]string{{"a", "b"}, {"b", "a"}} {
		h := newHarness(t, 16, "a", "b")
		k := key(t, "2001:db8::")
		h.sync("a")
		h.sync("b")
		h.set("a", 1, k, 100)
		h.set("b", 1, k, 100)
		h.want(k, 200, true)
		h.clock.Advance(10 * time.Millisecond)
		h.set(order[0], 1, k, 101)
		h.want(k, 201, true)
		h.set(order[1], 1, k, 101)
		h.want(k, 202, true)
	}
}

// TestRepeatedFullSnapshots: repeated full snapshot records (a teach
// repeated after partial replies, and again in later sessions) do not
// inflate totals; the source counts only once synchronized.
func TestRepeatedFullSnapshots(t *testing.T) {
	h := newHarness(t, 16, "a", "b")
	k, k2 := key(t, "2001:db8::"), key(t, "2001:db8:1::")
	h.sync("b")
	h.set("b", 1, k, 20)
	h.up("a", 1)
	h.defineAll("a", 1)
	for range 3 {
		h.teach("a", 1, k, 10, 25000)
		h.teach("a", 1, k2, 4, 25000)
		tot := h.want(k, 20, false)
		if c := contribution(t, tot, "a"); !c.Present || c.Counted || c.State != snapshot.Syncing {
			t.Fatalf("syncing source: %+v", c)
		}
		h.finish("a", 1, true)
		h.clock.Advance(time.Second)
	}
	h.teach("a", 1, k, 10, 25000)
	h.teach("a", 1, k2, 4, 25000)
	h.finish("a", 1, false)
	h.want(k, 30, true)
	h.total(inTable, k2)
	for session := uint64(2); session <= 4; session++ {
		h.down("a", session-1)
		h.want(k, 20, false)
		h.up("a", session)
		h.defineAll("a", session)
		h.teach("a", session, k, 10, 25000)
		h.teach("a", session, k2, 4, 25000)
		h.finish("a", session, false)
		if tot := h.want(k, 30, true); tot.Uncertain {
			t.Fatalf("replayed snapshot reported uncertain: %+v", tot)
		}
		if got := h.total(inTable, k2); got.Sum != 4 {
			t.Fatalf("k2 total %d after session %d", got.Sum, session)
		}
	}
}

// TestIsolation: distinct keys, sources, and tables stay separate, and
// session-local table IDs, which collide across sources, play no part.
func TestIsolation(t *testing.T) {
	h := newHarness(t, 16, "a", "b")
	k1, k2 := key(t, "2001:db8::"), key(t, "2001:db8:1::")
	h.sync("a")
	// Source b numbers its tables the other way round: its ID 1 is t_in2.
	h.up("b", 1)
	h.must("b", 1, peersession.TableDefined{ID: 1, Definition: definition(inTable2), Received: h.clock.Now()})
	h.must("b", 1, peersession.TableDefined{ID: 2, Definition: definition(inTable), Received: h.clock.Now()})
	h.finish("b", 1, false)

	h.must("a", 1, h.upd(inTable, 1, k1, 1))
	h.must("a", 1, h.upd(inTable2, 2, k1, 2))
	h.must("a", 1, h.upd(inTable, 1, k2, 4))
	h.must("b", 1, h.upd(inTable, 2, k1, 8))
	h.must("b", 1, h.upd(inTable2, 1, k1, 16))
	h.must("b", 1, h.upd(inTable2, 1, k2, 32))

	for _, c := range []struct {
		table string
		k     peermsg.Key
		sum   uint64
	}{
		{inTable, k1, 1 + 8},
		{inTable2, k1, 2 + 16},
		{inTable, k2, 4},
		{inTable2, k2, 32},
		{inTable, key(t, "2001:db8:2::"), 0},
	} {
		if got := h.total(c.table, c.k); got.Sum != c.sum || !got.Complete {
			t.Errorf("%s %v: %d complete %v, want %d", c.table, c.k, got.Sum, got.Complete, c.sum)
		}
	}
	tot := h.total(inTable, k2)
	if a, b := contribution(t, tot, "a"), contribution(t, tot, "b"); !a.Present || a.Count != 4 || b.Present {
		t.Fatalf("k2 contributions: a %+v, b %+v", a, b)
	}
	if _, err := aggregate.Count(h.s, "t_out", k1); !errors.Is(err, aggregate.ErrUnknownTable) {
		t.Fatalf("output table: %v, want ErrUnknownTable", err)
	}
}

// TestExpiredAndReplacedOnce: an expired or replaced contribution leaves
// the total exactly once, whatever reads or expiry passes follow.
func TestExpiredAndReplacedOnce(t *testing.T) {
	h := newHarness(t, 16, "a", "b")
	k := key(t, "2001:db8::")
	h.sync("a")
	h.sync("b")
	h.set("a", 1, k, 10)
	h.clock.Advance(10 * time.Second)
	h.observe("a", 1)
	h.set("b", 1, k, 20)
	h.want(k, 30, true)

	// Replacement: b's 20 leaves once, its 25 counts once.
	h.set("b", 1, k, 25)
	h.want(k, 35, true)
	h.want(k, 35, true)

	// a's entry expires at 30 s after its update, with no input.
	h.clock.Advance(20*time.Second - time.Millisecond)
	h.observe("a", 1)
	h.observe("b", 1)
	h.want(k, 35, true)
	h.clock.Advance(time.Millisecond)
	h.observe("a", 1)
	h.observe("b", 1)
	tot := h.want(k, 25, true)
	if c := contribution(t, tot, "a"); c.Present || c.Counted {
		t.Fatalf("expired contribution still present: %+v", c)
	}
	for range 3 {
		h.s.Expire()
		h.want(k, 25, true)
	}
	if r, _ := h.s.Source("a"); r.Expired != 1 || r.Entries != 0 {
		t.Fatalf("source a: expired %d entries %d, want 1 and 0", r.Expired, r.Entries)
	}

	// A timed update with no lifetime left removes b's value once.
	h.teach("b", 1, k, 25, 0)
	h.want(k, 0, true)
	h.want(k, 0, true)

	// A new entry after expiry counts from its own value, with no
	// history: the old entry ended with its lifetime.
	h.set("a", 1, k, 1)
	if tot := h.want(k, 1, true); tot.Uncertain {
		t.Fatalf("recreated entry after expiry reported uncertain: %+v", tot)
	}
}

// TestDecreaseAndBoundary: a count at the 32-bit boundary sums exactly; a
// wrap, a reset, or a recreation seen as a decrease lowers the total to
// the current values (never a huge positive delta) and marks it uncertain
// until the entry expires.
func TestDecreaseAndBoundary(t *testing.T) {
	h := newHarness(t, 16, "a", "b")
	k := key(t, "2001:db8::")
	h.sync("a")
	h.sync("b")
	h.set("b", 1, k, 20)

	h.set("a", 1, k, math.MaxUint32-1)
	h.want(k, math.MaxUint32+19, true)
	h.set("a", 1, k, math.MaxUint32)
	tot := h.want(k, math.MaxUint32+20, true)
	if tot.Uncertain {
		t.Fatalf("boundary value reported uncertain: %+v", tot)
	}
	if _, err := tot.Uint32(); !errors.Is(err, aggregate.ErrOverflow) {
		t.Fatalf("narrowing %d: %v, want ErrOverflow", tot.Sum, err)
	}

	// One more request at the source wraps HAProxy's counter to 0.
	h.clock.Advance(time.Second)
	h.set("a", 1, k, 0)
	tot = h.want(k, 20, true)
	c := contribution(t, tot, "a")
	want := snapshot.Decrease{From: math.MaxUint32, To: 0, FromSession: 1, ToSession: 1, At: h.clock.Now()}
	if !tot.Uncertain || !c.Discontinuous || c.Decreases != 1 || c.LastDecrease != want {
		t.Fatalf("wrap: total %+v, contribution %+v", tot, c)
	}
	if v, err := tot.Uint32(); err != nil || v != 20 {
		t.Fatalf("narrowing 20: %d, %v", v, err)
	}
	// Counting resumes from the wrapped value; the entry stays uncertain.
	h.set("a", 1, k, 5)
	if tot = h.want(k, 25, true); !tot.Uncertain {
		t.Fatalf("continuity restored by an increase: %+v", tot)
	}

	// A reset in the same session (runtime API "set table") on b.
	h.set("b", 1, k, 3)
	tot = h.want(k, 8, true)
	if c = contribution(t, tot, "b"); c.Decreases != 1 || c.LastDecrease.From != 20 || c.LastDecrease.To != 3 {
		t.Fatalf("reset: %+v", c)
	}

	// A recreation reported by a later session: lower value, new
	// session. The source lost the old entry's history, so it stays
	// degraded while that history could still change its rate (phase
	// 11), and its counts are uncertain until the old entry's deadline.
	h.down("a", 1)
	h.want(k, 3, false)
	h.up("a", 2)
	h.defineAll("a", 2)
	h.teach("a", 2, k, 2, 30000)
	h.finish("a", 2, false)
	tot = h.want(k, 3, false)
	c = contribution(t, tot, "a")
	if c.Decreases != 2 || c.LastDecrease.From != 5 || c.LastDecrease.To != 2 || c.LastDecrease.FromSession != 1 ||
		c.LastDecrease.ToSession != 2 || c.State != snapshot.Degraded || c.Counted || !c.HistoryLost {
		t.Fatalf("recreation in a later session: %+v", c)
	}
	if r, _ := h.s.Source("a"); r.Decreases != 2 || r.Loss.Recreated != 1 || r.Loss.Absent != 0 {
		t.Fatalf("source a decreases %d, loss %+v", r.Decreases, r.Loss)
	}
	h.clock.Advance(20100 * time.Millisecond) // past 2P+1 ms after session 2 came up
	h.observe("a", 2)
	h.observe("b", 1)
	tot = h.want(k, 5, true)
	if c = contribution(t, tot, "a"); !tot.Uncertain || !c.HistoryLost || !c.Counted {
		t.Fatalf("after the rate window: total %+v, contribution %+v", tot, c)
	}

	// Uncertainty ends with the entries.
	h.clock.Advance(10 * time.Second)
	h.observe("a", 2)
	h.observe("b", 1)
	h.set("a", 2, k, 1)
	h.set("b", 1, k, 1)
	if tot := h.want(k, 2, true); tot.Uncertain {
		t.Fatalf("new entries after expiry reported uncertain: %+v", tot)
	}
}

// TestNarrowing: the proposed 32-bit output encoding detects overflow
// exactly at the boundary.
func TestNarrowing(t *testing.T) {
	for _, c := range []struct {
		sum  uint64
		fits bool
	}{{0, true}, {math.MaxUint32, true}, {math.MaxUint32 + 1, false}, {math.MaxUint64, false}} {
		v, err := aggregate.Total{Sum: c.sum}.Uint32()
		if c.fits && (err != nil || uint64(v) != c.sum) {
			t.Errorf("%d: %d, %v", c.sum, v, err)
		}
		if !c.fits && !errors.Is(err, aggregate.ErrOverflow) {
			t.Errorf("%d: %d, %v, want ErrOverflow", c.sum, v, err)
		}
	}
}

// TestCompleteness: only Ready sources are counted, and a total is
// complete only when the whole configured roster is Ready; every roster
// source stays visible whatever its state.
func TestCompleteness(t *testing.T) {
	h := newHarness(t, 1, "a", "b", "c")
	k, k2 := key(t, "2001:db8::"), key(t, "2001:db8:1::")
	tot := h.want(k, 0, false)
	for _, c := range tot.Sources {
		if c.State != snapshot.Disconnected || c.Present {
			t.Fatalf("initial contribution %+v", c)
		}
	}
	h.sync("a")
	h.sync("b")
	h.set("a", 1, k, 10)
	h.set("b", 1, k, 20)
	h.want(k, 30, false) // c missing
	h.sync("c")
	h.want(k, 30, true) // c Ready and empty: complete

	// c syncing: its taught value is visible but not counted.
	h.down("c", 1)
	h.up("c", 2)
	h.defineAll("c", 2)
	h.teach("c", 2, k, 40, 30000)
	tot = h.want(k, 30, false)
	if c := contribution(t, tot, "c"); !c.Present || c.Counted || c.State != snapshot.Syncing {
		t.Fatalf("syncing c: %+v", c)
	}
	h.finish("c", 2, false)
	h.want(k, 70, true)

	// b degraded by a refused event (its capacity is 1): not counted.
	if err := h.apply("b", 1, h.upd(inTable, 1, k2, 1)); !errors.Is(err, snapshot.ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	tot = h.want(k, 50, false)
	if c := contribution(t, tot, "b"); !c.Present || c.Counted || c.State != snapshot.Degraded {
		t.Fatalf("degraded b: %+v", c)
	}
	h.down("b", 1)
	h.up("b", 2)
	h.defineAll("b", 2)
	// b's session 1 entry is held over until b's next finished resync
	// (b is still degraded by its session 1 fault): visible, never
	// counted.
	tot = h.want(k, 50, false)
	if c := contribution(t, tot, "b"); !c.HeldOver || c.Counted || c.State == snapshot.Ready {
		t.Fatalf("held-over b: total %+v contribution %+v", tot, c)
	}
	h.teach("b", 2, k, 20, 29000)
	h.finish("b", 2, false)
	tot = h.want(k, 70, true)
	if c := contribution(t, tot, "b"); c.HeldOver || !c.Counted || tot.Uncertain {
		t.Fatalf("re-taught b: total %+v contribution %+v", tot, c)
	}

	// Silence degrades every source; heartbeats keep it Ready.
	h.clock.Advance(healthTO)
	h.observe("a", 1)
	h.observe("c", 2)
	h.want(k, 50, false) // b unhealthy
	h.observe("b", 2)
	h.want(k, 70, true)
}

// TestRandomTraces compares the total and the rate with the oracle after
// random steps across both tables: updates with random (also decreasing
// and boundary) counts and random rate counters, timed replays, partial and finished resyncs,
// reconnects, heartbeats, clock advances across health and expiry
// bounds, and the events the store refuses or reports as faults:
// duplicate SessionUp, events of an earlier session, a definition with
// the wrong schema, a TableRejected report, a non-input table, an update
// of an undefined table, and a new key beyond the entry capacity. It
// requires every one of those paths, and degraded sources with retained
// entries, to occur.
func TestRandomTraces(t *testing.T) {
	srcs := []string{"a", "b", "c"}
	keys := []string{"2001:db8::", "2001:db8:1::", "2001:db8:2::"}
	tables := []string{inTable, inTable2}
	badDef := definition(inTable2)
	badDef.Fields = []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: 2 * period}}
	outDef := definition("t_out")
	refusals := map[error]int{}
	causes := []error{
		snapshot.ErrSession, snapshot.ErrSchema, snapshot.ErrNotInput, snapshot.ErrUndefined, snapshot.ErrCapacity,
	}
	var dupUps, staleEvents, rejected, degradedRetained, uncertainComplete, nonzero2, nonzeroRates int
	var absent, recreated, lossDegraded uint64
	for seed := range uint64(40) {
		rng := rand.New(rand.NewPCG(seed, 8))
		// Capacity 4 of the 6 key/table slots, so new keys are refused.
		h := newHarness(t, 4, srcs...)
		var ks []peermsg.Key
		for _, s := range keys {
			ks = append(ks, key(t, s))
		}
		session := map[string]uint64{}
		up := map[string]bool{}
		try := func(src string, sess uint64, body peersession.Event) {
			err := h.apply(src, sess, body)
			if err == nil {
				return
			}
			if !errors.Is(err, snapshot.ErrRefused) {
				t.Fatalf("seed %d: %v", seed, err)
			}
			for _, c := range causes {
				if errors.Is(err, c) {
					refusals[c]++
				}
			}
		}
		for step := range 500 {
			src := srcs[rng.IntN(len(srcs))]
			k := ks[rng.IntN(len(ks))]
			table := tables[rng.IntN(len(tables))]
			id := peermsg.RemoteTableID(1 + rng.IntN(2))
			now := h.clock.Now()
			switch r := rng.IntN(100); {
			case r < 4 && up[src]:
				h.down(src, session[src])
				up[src] = false
			case r < 10 && !up[src]:
				session[src]++
				up[src] = true
				h.up(src, session[src])
				h.defineAll(src, session[src])
			case r < 12 && session[src] > 0:
				dupUps++
				try(src, session[src], peersession.SessionUp{Direction: peersession.Inbound, At: now})
			case r < 14 && session[src] > 1:
				staleEvents++
				try(src, session[src]-1, h.upd(table, id, k, randCount(rng)))
			case !up[src]:
			case r < 16:
				try(src, session[src], peersession.TableDefined{ID: 2, Definition: badDef, Received: now})
			case r < 17:
				rejected++
				try(src, session[src], peersession.TableRejected{
					ID: 1, Table: table, Err: peersession.ErrSchema, Received: now,
				})
			case r < 18:
				try(src, session[src], peersession.TableDefined{ID: 3, Definition: outDef, Received: now})
			case r < 19:
				u := h.upd("t_out", 3, k, 1)
				try(src, session[src], u)
			case r < 22:
				h.defineAll(src, session[src])
			case r < 32:
				h.finish(src, session[src], rng.IntN(3) == 0)
			case r < 42:
				u := h.upd(table, id, k, randCount(rng))
				u.Update.Timed, u.Update.Remaining = true, peermsg.Millis(rng.IntN(int(expiry)+5000))
				u.Update.Values[1].Freq = randFreq(rng)
				try(src, session[src], u)
			case r < 70:
				u := h.upd(table, id, k, randCount(rng))
				u.Update.Values[1].Freq = randFreq(rng)
				try(src, session[src], u)
			case r < 80:
				h.observe(src, session[src])
			default:
				h.clock.Advance(time.Duration(rng.IntN(4000)) * time.Millisecond)
			}
			// Reads and Expire purge expired entries; skipping them on
			// most steps lets updates meet expired, unpurged entries.
			if step%7 == 0 {
				h.s.Expire()
			}
			if rng.IntN(3) != 0 {
				continue
			}
			for _, k := range ks {
				for _, table := range tables {
					tot := h.total(table, k)
					if r := h.rate(table, k); r.Sum > 0 && r.Complete {
						nonzeroRates++
					}
					for _, c := range tot.Sources {
						if c.Present && c.State == snapshot.Degraded {
							degradedRetained++
						}
						if strings.HasPrefix(c.Reason, "history lost") {
							lossDegraded++
						}
					}
					if tot.Complete && tot.Uncertain {
						uncertainComplete++
					}
					if table == inTable2 && tot.Sum > 0 {
						nonzero2++
					}
				}
			}
		}
		for _, src := range h.s.Roster().Sources {
			absent += src.Loss.Absent
			recreated += src.Loss.Recreated
		}
	}
	t.Logf("lost history: %d absent, %d recreated; reads of a source degraded by it %d", absent, recreated,
		lossDegraded)
	if absent == 0 || recreated == 0 || lossDegraded == 0 {
		t.Error("trace never detected both kinds of lost history and degraded a source for it")
	}
	t.Logf("refusals %v; duplicate ups %d, earlier-session events %d, rejections %d; reads with a degraded "+
		"source's retained entry %d, complete but uncertain %d, non-zero %s totals %d, non-zero complete rates %d", refusals,
		dupUps, staleEvents, rejected, degradedRetained, uncertainComplete, inTable2, nonzero2, nonzeroRates)
	for _, c := range causes {
		if refusals[c] == 0 {
			t.Errorf("no refusal wrapping %v", c)
		}
	}
	for what, n := range map[string]int{
		"duplicate SessionUp": dupUps, "earlier-session event": staleEvents, "TableRejected": rejected,
		"degraded source with a retained entry": degradedRetained, "complete uncertain total": uncertainComplete,
		"non-zero " + inTable2 + " total": nonzero2, "non-zero complete rate": nonzeroRates,
	} {
		if n == 0 {
			t.Errorf("trace never produced a %s", what)
		}
	}
}

func randCount(rng *rand.Rand) uint32 {
	switch rng.IntN(4) {
	case 0:
		return math.MaxUint32 - uint32(rng.IntN(3))
	case 1:
		return uint32(rng.IntN(3))
	default:
		return rng.Uint32()
	}
}
