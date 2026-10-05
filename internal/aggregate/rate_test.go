package aggregate_test

import (
	"errors"
	"maps"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/aggregate"
	"github.com/brenc/haproxy-table-aggregator/internal/aggregate/aggregatetest"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/rate"
	"github.com/brenc/haproxy-table-aggregator/internal/rate/ratetest"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// ratePeriods are the two periods every rate case runs with.
var ratePeriods = []peermsg.Millis{10000, 60000}

// rateHarness drives a one-table store whose period is p and expiry 3p
// (so a silent entry outlives its decay, as in the lab), with simulated
// HAProxy sources whose millisecond clocks each have their own offset
// from the local fake clock.
type rateHarness struct {
	t      *testing.T
	p      peermsg.Millis
	clock  *fakeClock
	start  time.Time
	s      *snapshot.Store
	o      *aggregatetest.Oracle
	roster []string
	src    map[string]*ratetest.Source
	offset map[string]uint32
}

func newRateHarness(t *testing.T, p peermsg.Millis, offsets map[string]uint32, roster ...string) *rateHarness {
	t.Helper()
	c := &fakeClock{t: time.Now()}
	s, err := snapshot.New(snapshot.Options{
		Sources: roster, Tables: []snapshot.Table{{Name: inTable, Period: p}},
		HealthTimeout: healthTO, MaxSourceEntries: 16, Now: c.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := &rateHarness{
		t: t, p: p, clock: c, start: c.Now(), s: s, roster: roster,
		o:   &aggregatetest.Oracle{Health: healthTO, Tables: []string{inTable}},
		src: map[string]*ratetest.Source{}, offset: offsets,
	}
	for _, name := range roster {
		h.src[name] = &ratetest.Source{Period: uint32(p)}
	}
	return h
}

func (h *rateHarness) def() peermsg.Definition {
	return peermsg.Definition{
		Name: inTable, KeyType: peermsg.KeyTypeIPv6, Expiry: 3 * h.p,
		Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: h.p}},
	}
}

func (h *rateHarness) apply(src string, session uint64, body peersession.Event) error {
	ev := sources.Event{Source: src, Session: session, Body: body}
	err := h.s.Apply(ev)
	h.o.Record(ev, err)
	return err
}

func (h *rateHarness) must(src string, session uint64, body peersession.Event) {
	h.t.Helper()
	if err := h.apply(src, session, body); err != nil {
		h.t.Fatalf("%s#%d %T: %v", src, session, body, err)
	}
}

// sync brings every source's session 1 to Ready with nothing taught.
func (h *rateHarness) sync() {
	h.t.Helper()
	for _, name := range h.roster {
		now := h.clock.Now()
		h.must(name, 1, peersession.SessionUp{Direction: peersession.Inbound, At: now})
		h.must(name, 1, peersession.TableDefined{ID: 1, Definition: h.def(), Received: now})
		h.must(name, 1, peersession.SyncFinished{Received: now})
	}
}

// heartbeat records a message read now on every source's session 1.
func (h *rateHarness) heartbeat() {
	for _, name := range h.roster {
		h.s.Observe(name, 1, h.clock.Now())
		h.o.Observe(name, 1, h.clock.Now())
	}
}

// srcNow is source name's millisecond clock at the local fake time.
func (h *rateHarness) srcNow(name string) uint32 {
	return h.offset[name] + uint32(h.clock.Now().Sub(h.start)/time.Millisecond)
}

// update builds the update source name sends now for key k.
func (h *rateHarness) update(name string, k peermsg.Key, cnt uint32) peersession.EntryUpdated {
	return peersession.EntryUpdated{ID: 1, Table: inTable, Expiry: 3 * h.p, Update: peermsg.Update{
		ID: 1, Key: k, Received: h.clock.Now(),
		Values: []peermsg.Value{
			{Type: peermsg.DataHTTPReqCnt, Uint: cnt},
			{Type: peermsg.DataHTTPReqRate, Freq: h.src[name].Wire(h.srcNow(name))},
		},
	}}
}

// hit counts n events on source name now and delivers its update at once.
func (h *rateHarness) hit(name string, k peermsg.Key, n int) {
	h.t.Helper()
	h.src[name].Hit(h.srcNow(name), n)
	h.must(name, 1, h.update(name, k, 1))
}

// rate reads the rate of k and checks it against the oracle and against
// the simulated sources' own readings now: every counted estimate is
// the source's reading, and the sum is their sum.
func (h *rateHarness) rate(k peermsg.Key) aggregate.RateTotal {
	h.t.Helper()
	got, err := aggregate.Rate(h.s, inTable, k)
	if err != nil {
		h.t.Fatal(err)
	}
	x := h.o.WantRate(h.roster, inTable, k, uint32(h.p), h.clock.Now())
	counted := map[string]uint64{}
	for _, c := range got.Sources {
		if c.Counted {
			counted[c.Source] = c.Estimate
		}
	}
	if got.Sum != x.Sum || got.Complete != x.Complete || got.Uncertain != x.Uncertain ||
		!maps.Equal(counted, x.Counted) {
		h.t.Fatalf("rate %d complete %v uncertain %v counted %v; oracle %+v", got.Sum, got.Complete,
			got.Uncertain, counted, x)
	}
	if !got.At.Equal(h.clock.Now()) || got.Period != h.p || got.SumAt(got.At) != got.Sum {
		h.t.Fatalf("rate at %v period %v SumAt %d, sum %d", got.At, got.Period, got.SumAt(got.At), got.Sum)
	}
	return got
}

// sourcesSum is the sum of the simulated sources' own readings now.
func (h *rateHarness) sourcesSum() uint64 {
	var s uint64
	for _, name := range h.roster {
		s += h.src[name].Read(h.srcNow(name))
	}
	return s
}

func forEachRatePeriod(t *testing.T, fn func(t *testing.T, p peermsg.Millis)) {
	t.Helper()
	for _, p := range ratePeriods {
		t.Run(p.String(), func(t *testing.T) { fn(t, p) })
	}
}

// TestRateOffsetWindows: two sources with equal periods but different
// window phases and histories (and unrelated, one wrapping, source
// clocks) sum to the sum of their own readings at every common time,
// while adding their raw counter fields gives a different, invalid
// answer.
func TestRateOffsetWindows(t *testing.T) {
	forEachRatePeriod(t, func(t *testing.T, p peermsg.Millis) {
		h := newRateHarness(t, p, map[string]uint32{"a": 5000000, "b": math.MaxUint32 - uint32(p)}, "a", "b")
		h.sync()
		k := key(t, "2001:db8:9::")
		step := time.Duration(p) * time.Millisecond / 100
		var rawDiffers, sampled int
		for i := range 600 {
			// a: a steady event every 5 steps from the start. b: a
			// burst starting 0.37P later, then silence, then steady
			// traffic in a different phase from 2.5P.
			switch {
			case i%5 == 0 && i < 300:
				h.hit("a", k, 1)
			case i == 37:
				h.hit("b", k, 30)
			case i >= 250 && i < 400 && i%3 == 1:
				h.hit("b", k, 2)
			}
			if i%7 == 0 {
				h.heartbeat()
			}
			r := h.rate(k)
			if want := h.sourcesSum(); r.Sum != want || !r.Complete {
				t.Fatalf("step %d: rate %d complete %v, sources read %d", i, r.Sum, r.Complete, want)
			}
			sampled++
			ca, cb := contributionRate(t, r, "a"), contributionRate(t, r, "b")
			if ca.Present && cb.Present {
				raw := rate.Estimate(ca.Counter.Value.Curr+cb.Counter.Value.Curr,
					ca.Counter.Value.Prev+cb.Counter.Value.Prev, p, ca.Elapsed)
				if raw != r.Sum {
					rawDiffers++
				}
			}
			h.clock.Advance(step)
		}
		if rawDiffers == 0 {
			t.Fatal("adding raw counter fields never differed from the specified sum")
		}
		t.Logf("%d common times, raw-field addition wrong at %d", sampled, rawDiffers)
	})
}

// TestRateEqualWindows: sources whose windows start together and see the
// same traffic each contribute the same estimate; the sum is twice one
// source's reading, with each estimate truncated before summing.
func TestRateEqualWindows(t *testing.T) {
	forEachRatePeriod(t, func(t *testing.T, p peermsg.Millis) {
		h := newRateHarness(t, p, map[string]uint32{"a": 1000, "b": 1000}, "a", "b")
		h.sync()
		k := key(t, "2001:db8:a::")
		h.hit("a", k, 3)
		h.hit("b", k, 3)
		h.clock.Advance(time.Duration(p) * time.Millisecond)
		h.heartbeat()
		// Both rotate into the next period with three events there too:
		// prev 3 and curr 3 at the same phase.
		h.hit("a", k, 3)
		h.hit("b", k, 3)
		// At half the period each reads 3 + floor(3*0.5) = 4 (4.5
		// truncated); the sum is 8, not floor(9).
		h.clock.Advance(time.Duration(p) * time.Millisecond / 2)
		h.heartbeat()
		r := h.rate(k)
		if ca, cb := contributionRate(t, r, "a"), contributionRate(t, r, "b"); ca.Estimate != 4 || cb.Estimate != 4 ||
			r.Sum != 8 {
			t.Fatalf("equal windows: a %d b %d sum %d, want 4 4 8", ca.Estimate, cb.Estimate, r.Sum)
		}
	})
}

// TestRateDecay: with no new input the rate decays to 0 at the times
// Next and Cadence.Due name, stops needing evaluation, and the expired
// entry neither returns nor has its lifetime extended by a replay.
func TestRateDecay(t *testing.T) {
	forEachRatePeriod(t, func(t *testing.T, p peermsg.Millis) {
		h := newRateHarness(t, p, map[string]uint32{"a": 42, "b": 900000}, "a", "b")
		h.sync()
		k := key(t, "2001:db8:b::")
		h.hit("a", k, 40)
		h.clock.Advance(time.Duration(p) * time.Millisecond / 4)
		h.heartbeat()
		h.hit("b", k, 1)
		cad := aggregate.Cadence{}
		r := h.rate(k)
		if r.Sum != 41 {
			t.Fatalf("initial rate %d, want 41", r.Sum)
		}
		evaluations := 0
		for {
			due, ok := cad.Due(r)
			if !ok {
				break
			}
			if due.Sub(r.At) < aggregate.DefaultMinInterval {
				t.Fatalf("due %v after the last evaluation, below the minimum interval", due.Sub(r.At))
			}
			// Nothing changes before the due time, and the published sum
			// overstates the true one by at most the budget.
			before := due.Add(-time.Millisecond)
			if before.After(r.At) && r.SumAt(before) < r.Sum-min(r.Sum, r.DecayBound(due.Sub(r.At))) {
				t.Fatalf("sum fell from %d to %d before the due time, beyond the budget", r.Sum, r.SumAt(before))
			}
			if !r.Next.Before(r.At.Add(aggregate.DefaultMinInterval)) && r.SumAt(before) != r.Sum {
				t.Fatalf("sum changed before Next")
			}
			h.clock.t = due
			h.heartbeat() // the sources stay healthy and silent
			r = h.rate(k)
			evaluations++
		}
		if r.Sum != 0 {
			t.Fatalf("no further evaluation due at rate %d", r.Sum)
		}
		// a's 40 events decay by 1 per P/40 after its first period, so
		// the minimum interval bounds the evaluations.
		maxEvals := int(2*time.Duration(p)*time.Millisecond/aggregate.DefaultMinInterval) + 2
		if evaluations == 0 || evaluations > maxEvals {
			t.Fatalf("%d evaluations to decay, want 1 to %d", evaluations, maxEvals)
		}
		// Both entries outlive the decay (expiry 3P) without updates.
		for _, name := range h.roster {
			if c := contributionRate(t, r, name); !c.Present || c.Estimate != 0 {
				t.Fatalf("source %s after decay: %+v", name, c)
			}
		}
		e, _ := h.s.Lookup("b", inTable, k)
		h.clock.t = e.Deadline
		h.heartbeat()
		r = h.rate(k)
		if c := contributionRate(t, r, "b"); c.Present || r.Sum != 0 {
			t.Fatalf("expired entry still present: %+v", c)
		}
		// A replay of b's counter with no lifetime left must not
		// resurrect the key, and an old snapshot must not extend it.
		late := h.update("b", k, 1)
		late.Update.Timed, late.Update.Remaining = true, 0
		late.Update.Values[1].Freq = peermsg.FreqCounter{Age: 0, Curr: 9}
		h.must("b", 1, late)
		r = h.rate(k)
		if c := contributionRate(t, r, "b"); c.Present || r.Sum != 0 {
			t.Fatalf("replay resurrected the key: %+v", c)
		}
	})
}

// TestRateNextIncludesExpiry: an entry whose remaining lifetime ends
// while its estimate is above 0 makes its expiry the next change, and
// the rate drops by its whole estimate then.
func TestRateNextIncludesExpiry(t *testing.T) {
	forEachRatePeriod(t, func(t *testing.T, p peermsg.Millis) {
		h := newRateHarness(t, p, map[string]uint32{"a": 3}, "a")
		h.sync()
		k := key(t, "2001:db8:12::")
		h.src["a"].Hit(h.srcNow("a"), 9)
		u := h.update("a", k, 9)
		u.Update.Timed, u.Update.Remaining = true, p/2
		h.must("a", 1, u)
		r := h.rate(k)
		deadline := h.clock.Now().Add(p.Duration() / 2)
		if r.Sum != 9 || !r.Next.Equal(deadline) {
			t.Fatalf("rate %d next %v, want 9 until the deadline %v", r.Sum, r.Next, deadline)
		}
		if got := r.SumAt(deadline); got != 0 {
			t.Fatalf("SumAt the deadline %d, want 0", got)
		}
		h.clock.t = deadline
		h.heartbeat()
		if r := h.rate(k); r.Sum != 0 || !r.Next.IsZero() {
			t.Fatalf("after expiry: rate %d next %v", r.Sum, r.Next)
		}
	})
}

// TestRateCompleteness: only Ready sources count, an incomplete rate is
// not authoritative, and a held-over or out-of-range contribution makes
// the rate uncertain.
func TestRateCompleteness(t *testing.T) {
	forEachRatePeriod(t, func(t *testing.T, p peermsg.Millis) {
		h := newRateHarness(t, p, map[string]uint32{"a": 7, "b": 8}, "a", "b")
		h.sync()
		k := key(t, "2001:db8:c::")
		h.hit("a", k, 5)
		h.hit("b", k, 6)
		r := h.rate(k)
		if v, err := r.Authoritative(); err != nil || v != 11 {
			t.Fatalf("complete rate: %d, %v", v, err)
		}
		// b's session drops: its entry is retained but not counted.
		h.must("b", 1, peersession.SessionDown{Err: errors.New("closed"), At: h.clock.Now()})
		r = h.rate(k)
		if c := contributionRate(t, r, "b"); !c.Present || c.Counted || r.Complete || r.Sum != 5 {
			t.Fatalf("disconnected source: rate %+v", r)
		}
		if _, err := r.Authoritative(); !errors.Is(err, aggregate.ErrIncomplete) {
			t.Fatalf("incomplete rate authoritative: %v", err)
		}
		// Session 2 syncs without re-teaching the key: held over.
		now := h.clock.Now()
		h.must("b", 2, peersession.SessionUp{Direction: peersession.Inbound, At: now})
		h.must("b", 2, peersession.TableDefined{ID: 1, Definition: h.def(), Received: now})
		r = h.rate(k)
		if c := contributionRate(t, r, "b"); c.State != snapshot.Syncing || c.Counted {
			t.Fatalf("syncing source counted: %+v", c)
		}
		h.must("b", 2, peersession.SyncFinished{Received: now})
		r = h.rate(k)
		if c := contributionRate(t, r, "b"); !c.Counted || !c.HeldOver || !r.Uncertain || !r.Complete {
			t.Fatalf("held-over entry: %+v, rate %+v", c, r)
		}
		if _, err := r.Authoritative(); err != nil {
			t.Fatalf("uncertain is not an error: %v", err)
		}
	})
}

// TestRateAgeOutOfRange: a non-empty counter whose wire age exceeds
// rate.MaxAge(P) evaluates to its exact decayed reading, 0, and marks the
// rate uncertain; one at the limit is ordinary, and so is an empty one
// with any age.
func TestRateAgeOutOfRange(t *testing.T) {
	const p = peermsg.Millis(10000)
	h := newRateHarness(t, p, map[string]uint32{"a": 1}, "a")
	h.sync()
	k, k2, k3 := key(t, "2001:db8:d::"), key(t, "2001:db8:e::"), key(t, "2001:db8:13::")
	u := h.update("a", k, 1)
	u.Update.Values[1].Freq = peermsg.FreqCounter{Age: peermsg.Millis(rate.MaxAge(p) + 1), Curr: 3, Prev: 3}
	h.must("a", 1, u)
	r := h.rate(k)
	if c := contributionRate(t, r, "a"); !c.AgeOutOfRange || !r.Uncertain || r.Sum != 0 {
		t.Fatalf("out-of-range age: %+v, rate %+v", c, r)
	}
	u = h.update("a", k3, 1)
	u.Update.Values[1].Freq = peermsg.FreqCounter{Age: peermsg.Millis(rate.MaxAge(p)), Curr: 3, Prev: 3}
	h.must("a", 1, u)
	if r := h.rate(k3); r.Uncertain || r.Sum != 0 {
		t.Fatalf("age at the limit: %+v", r)
	}
	u = h.update("a", k2, 1)
	u.Update.Values[1].Freq = peermsg.FreqCounter{Age: math.MaxUint32}
	h.must("a", 1, u)
	if r := h.rate(k2); r.Uncertain || r.Sum != 0 {
		t.Fatalf("never-counted counter: %+v", r)
	}
}

// TestRateOutputFit: a rate fits the 32-bit output slot up to
// math.MaxUint32; beyond it the narrowing and authority both fail with
// ErrOverflow, never wrapping or saturating.
func TestRateOutputFit(t *testing.T) {
	forEachRatePeriod(t, func(t *testing.T, p peermsg.Millis) {
		h := newRateHarness(t, p, map[string]uint32{"a": 1, "b": 2}, "a", "b")
		h.sync()
		set := func(name string, k peermsg.Key, fc peermsg.FreqCounter) {
			u := h.update(name, k, 1)
			u.Update.Values[1].Freq = fc
			h.must(name, 1, u)
		}
		k, k2 := key(t, "2001:db8:f::"), key(t, "2001:db8:10::")
		set("a", k, peermsg.FreqCounter{Age: 0, Curr: math.MaxUint32 - 5})
		set("b", k, peermsg.FreqCounter{Age: 0, Curr: 5})
		r := h.rate(k)
		if v, err := r.Authoritative(); err != nil || v != math.MaxUint32 {
			t.Fatalf("boundary rate: %d, %v", v, err)
		}
		set("b", k, peermsg.FreqCounter{Age: 0, Curr: 6})
		r = h.rate(k)
		if _, err := r.Uint32(); !errors.Is(err, aggregate.ErrOverflow) || r.Sum != math.MaxUint32+1 {
			t.Fatalf("rate %d narrowed: %v", r.Sum, err)
		}
		if _, err := r.Authoritative(); !errors.Is(err, aggregate.ErrOverflow) {
			t.Fatalf("overflowing rate authoritative: %v", err)
		}
		// One source alone beyond 32 bits: HAProxy's own reading would
		// truncate; the exact value is kept and refused.
		set("a", k2, peermsg.FreqCounter{Age: 0, Curr: math.MaxUint32, Prev: math.MaxUint32})
		r = h.rate(k2)
		if r.Sum != 2*math.MaxUint32 {
			t.Fatalf("wide single-source rate %d", r.Sum)
		}
		if _, err := r.Authoritative(); !errors.Is(err, aggregate.ErrOverflow) {
			t.Fatalf("wide rate authoritative: %v", err)
		}
	})
}

// TestRatePeriodMismatch: a source announcing another period is refused
// by schema validation and its counter is never converted or counted.
func TestRatePeriodMismatch(t *testing.T) {
	h := newRateHarness(t, 10000, map[string]uint32{"a": 1, "b": 1}, "a", "b")
	h.sync()
	k := key(t, "2001:db8:11::")
	h.hit("a", k, 4)
	bad := h.def()
	bad.Fields[1].Period = 60000
	now := h.clock.Now()
	h.must("b", 1, peersession.SessionDown{Err: errors.New("closed"), At: now})
	h.must("b", 2, peersession.SessionUp{Direction: peersession.Inbound, At: now})
	if err := h.apply("b", 2, peersession.TableDefined{ID: 1, Definition: bad, Received: now}); !errors.Is(err,
		snapshot.ErrSchema) {
		t.Fatalf("definition with period 60s: %v, want ErrSchema", err)
	}
	u := h.update("b", k, 1)
	if err := h.apply("b", 2, u); err == nil {
		t.Fatal("update of the refused table accepted")
	}
	r := h.rate(k)
	if c := contributionRate(t, r, "b"); c.Present || c.State != snapshot.Degraded || r.Complete || r.Sum != 4 {
		t.Fatalf("mismatched period: rate %+v", r)
	}
}

// TestRateUnknownTable: only configured input tables are evaluated.
func TestRateUnknownTable(t *testing.T) {
	h := newRateHarness(t, 10000, map[string]uint32{"a": 1}, "a")
	if _, err := aggregate.Rate(h.s, "t_out", key(t, "2001:db8::")); !errors.Is(err, aggregate.ErrUnknownTable) {
		t.Fatalf("unknown table: %v", err)
	}
}

// TestCadence: Due is Next, but never sooner than the minimum interval,
// and nothing is due for a rate that no longer changes.
func TestCadence(t *testing.T) {
	at := time.Unix(100, 0)
	r := aggregate.RateTotal{At: at, Next: at.Add(10 * time.Millisecond)}
	if due, ok := (aggregate.Cadence{}).Due(r); !ok || !due.Equal(at.Add(aggregate.DefaultMinInterval)) {
		t.Fatalf("due %v %v", due, ok)
	}
	if due, ok := (aggregate.Cadence{MinInterval: time.Millisecond}).Due(r); !ok || !due.Equal(r.Next) {
		t.Fatalf("due %v %v", due, ok)
	}
	if _, ok := (aggregate.Cadence{}).Due(aggregate.RateTotal{At: at}); ok {
		t.Fatal("due without a change")
	}
}

// rate reads the rate of table and k from the phase 08 harness's store
// and checks it against the oracle.
func (h *harness) rate(table string, k peermsg.Key) aggregate.RateTotal {
	h.t.Helper()
	got, err := aggregate.Rate(h.s, table, k)
	if err != nil {
		h.t.Fatal(err)
	}
	x := h.o.WantRate(h.roster, table, k, uint32(period), h.clock.Now())
	counted := map[string]uint64{}
	for _, c := range got.Sources {
		if c.Counted {
			counted[c.Source] = c.Estimate
		}
	}
	if got.Sum != x.Sum || got.Complete != x.Complete || got.Uncertain != x.Uncertain ||
		!maps.Equal(counted, x.Counted) {
		h.t.Fatalf("%s %v: rate %d complete %v uncertain %v counted %v; oracle %+v", table, k, got.Sum,
			got.Complete, got.Uncertain, counted, x)
	}
	return got
}

// randFreq returns a counter of random age and counts, including empty
// counters, ages around 2^31 and at and beyond the native range limit
// (rate.MaxAge), and the 32-bit limits.
func randFreq(rng *rand.Rand) peermsg.FreqCounter {
	var fc peermsg.FreqCounter
	switch rng.IntN(7) {
	case 0:
		fc.Age = peermsg.Millis(rng.Uint32())
	case 1:
		fc.Age = peermsg.Millis(rate.MaxAge(period)) + peermsg.Millis(rng.IntN(3)) - 1
	case 2:
		fc.Age = 1<<31 + peermsg.Millis(rng.IntN(3)) - 1
	default:
		fc.Age = peermsg.Millis(rng.IntN(int(3*period) + 1000))
	}
	switch rng.IntN(5) {
	case 0:
	case 1:
		fc.Curr, fc.Prev = uint32(rng.IntN(2)), uint32(rng.IntN(2))
	case 2:
		fc.Curr, fc.Prev = randCount(rng), randCount(rng)
	default:
		fc.Curr, fc.Prev = uint32(rng.IntN(500)), uint32(rng.IntN(500))
	}
	return fc
}

func contributionRate(t *testing.T, r aggregate.RateTotal, src string) aggregate.RateContribution {
	t.Helper()
	for _, c := range r.Sources {
		if c.Source == src {
			return c
		}
	}
	t.Fatalf("no source %s in %+v", src, r.Sources)
	return aggregate.RateContribution{}
}
