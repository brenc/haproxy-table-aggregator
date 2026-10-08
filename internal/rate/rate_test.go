package rate_test

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/rate"
	"github.com/brenc/haproxy-table-aggregator/internal/rate/ratetest"
)

// periods are the two rate periods every semantic case runs with.
var periods = []peermsg.Millis{10000, 60000}

func forEachPeriod(t *testing.T, fn func(t *testing.T, p peermsg.Millis)) {
	t.Helper()
	for _, p := range periods {
		t.Run(p.String(), func(t *testing.T) { fn(t, p) })
	}
}

// TestEstimateMatchesReference compares Estimate with the reference
// reading over every age up to three periods for small periods and
// counts, and over random large inputs.
func TestEstimateMatchesReference(t *testing.T) {
	counts := []uint32{0, 1, 2, 3, 5, 9}
	for _, p := range []uint32{1, 2, 3, 7, 10, 16} {
		for _, c := range counts {
			for _, pr := range counts {
				for e := range uint64(3*p + 2) {
					got := rate.Estimate(c, pr, peermsg.Millis(p), e)
					if want := ratetest.Read(c, pr, p, e); got != want {
						t.Fatalf("P=%d curr=%d prev=%d age=%d: %d, reference %d", p, c, pr, e, got, want)
					}
				}
			}
		}
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 200000 {
		p := 1 + rng.Uint32N(uint32(rate.MaxPeriod))
		c, pr := rng.Uint32(), rng.Uint32()
		e := rng.Uint64N(2*uint64(p) + 10)
		if got, want := rate.Estimate(c, pr, peermsg.Millis(p), e), ratetest.Read(c, pr, p, e); got != want {
			t.Fatalf("P=%d curr=%d prev=%d age=%d: %d, reference %d", p, c, pr, e, got, want)
		}
	}
}

// TestRoundingAndLowRate pins the integer rounding and the native
// low-rate correction at both periods.
func TestRoundingAndLowRate(t *testing.T) {
	forEachPeriod(t, func(t *testing.T, p peermsg.Millis) {
		P := uint64(p)
		cases := []struct {
			name       string
			curr, prev uint32
			age        uint64
			want       uint64
		}{
			{"empty at start", 0, 0, 0, 0},
			{"empty late", 0, 0, 5 * P, 0},
			// Truncation: 3 + 7*0.7 = 7.9 reads 7.
			{"truncates 7.9", 3, 7, 3 * P / 10, 7},
			// 2*0.5 = 1 exactly, then just below 1 reads 0: no
			// correction for two or more events.
			{"two previous at half", 0, 2, P / 2, 1},
			{"two previous past half", 0, 2, P/2 + 1, 0},
			// Correction: one previous event reads 1 for the whole
			// current period, however little of it overlaps.
			{"one previous at start", 0, 1, 0, 1},
			{"one previous at end", 0, 1, P, 1},
			// Correction after rotation: one current event reads 1
			// until 2P, then 0.
			{"one current after P", 1, 0, P + 1, 1},
			{"one current at 2P", 1, 0, 2 * P, 1},
			{"one current after 2P", 1, 0, 2*P + 1, 0},
			// A current event disables the correction for the
			// previous one: 1 + floor(1*(P-e)/P) is 1 below P.
			{"current plus one previous", 1, 1, 1, 1},
			{"current plus one previous at 0", 1, 1, 0, 2},
			// Two current events decay proportionally after P.
			{"two current just after P", 2, 0, P + 1, 1},
			{"two current at 1.5P", 2, 0, 3 * P / 2, 1},
			{"two current past 1.5P", 2, 0, 3*P/2 + 1, 0},
			// The previous period is gone after rotation.
			{"previous dropped after P", 4, 100, P + 1, 3},
			// Exactly at P the current count stands alone.
			{"at P", 4, 100, P, 4},
			// Exact beyond 32 bits: no truncation to the native uint.
			{"wide", math.MaxUint32, math.MaxUint32, 0, 2 * math.MaxUint32},
		}
		for _, c := range cases {
			if got := rate.Estimate(c.curr, c.prev, p, c.age); got != c.want {
				t.Errorf("%s: Estimate(curr=%d prev=%d age=%d) = %d, want %d", c.name, c.curr, c.prev, c.age, got,
					c.want)
			}
			if got := ratetest.Read(c.curr, c.prev, uint32(p), c.age); got != c.want {
				t.Errorf("%s: reference = %d, want %d", c.name, got, c.want)
			}
		}
	})
}

// TestNextIsFirstDrop checks Next against a linear scan and that the
// reading never rises with age.
func TestNextIsFirstDrop(t *testing.T) {
	base := time.Unix(1000, 0)
	counts := []uint32{0, 1, 2, 3, 7, 20}
	for _, p := range []peermsg.Millis{1, 2, 5, 10, 13} {
		for _, c := range counts {
			for _, pr := range counts {
				for age := range 2*p + 3 {
					ctr := rate.Counter{
						Value: peermsg.FreqCounter{Age: age, Curr: c, Prev: pr}, Period: p,
						Received: base,
					}
					for d := range int(3*p + 4) {
						at := base.Add(time.Duration(d) * time.Millisecond)
						v := ctr.At(at)
						var want time.Time
						for s := d + 1; s <= 4*int(p)+8; s++ {
							w := ctr.At(base.Add(time.Duration(s) * time.Millisecond))
							if w > v {
								t.Fatalf("P=%d %+v: reading rose from %d to %d", p, ctr.Value, v, w)
							}
							if w < v && want.IsZero() {
								want = base.Add(time.Duration(s) * time.Millisecond)
							}
						}
						got, ok := ctr.Next(at)
						if ok != !want.IsZero() || !got.Equal(want) {
							t.Fatalf("P=%d %+v at +%dms: Next %v %v, want %v", p, ctr.Value, d, got.Sub(base), ok,
								want.Sub(base))
						}
					}
				}
			}
		}
	}
}

// scenario is a sequence of events on one simulated source, in units of
// a period, plus times at which to compare readings. Every scenario runs
// unchanged at each period.
type scenario struct {
	name string
	// events maps a time (fraction of the period after the start) to a
	// number of events.
	events []hit
	// checks are the times (fractions of the period) to compare at.
	checks []float64
	// zeroFrom is the time from which the reading must be 0 and Next
	// must report no further change, or a negative value for none.
	zeroFrom float64
}

type hit struct {
	at float64
	n  int
}

func steady(from, to, step float64) []hit {
	var hs []hit
	for at := from; at < to; at += step {
		hs = append(hs, hit{at, 1})
	}
	return hs
}

func grid(to, step float64) []float64 {
	var out []float64
	for at := 0.0; at <= to; at += step {
		out = append(out, at)
	}
	return out
}

var scenarios = []scenario{
	{name: "empty", checks: grid(4, 0.25), zeroFrom: 0},
	{name: "one event", events: []hit{{0.1, 1}}, checks: grid(3, 0.05), zeroFrom: 2.11},
	{name: "burst", events: []hit{{0.3, 50}}, checks: grid(3, 0.01), zeroFrom: 2.31},
	{name: "steady", events: steady(0, 3, 0.05), checks: grid(6, 0.01), zeroFrom: 5.01},
	{
		name: "multi-period idle", events: append(steady(0, 0.5, 0.1), hit{4.2, 3}),
		checks: grid(8, 0.02), zeroFrom: 6.21,
	},
}

// runScenario drives sc on a source whose clock reads clock0 at the
// scenario's start: it delivers the wire counter after every event batch
// as if received immediately, and compares the evaluated reading at each
// check time with the source's own reading then.
func runScenario(t *testing.T, sc scenario, p peermsg.Millis, clock0 uint32) {
	t.Helper()
	src := &ratetest.Source{Period: uint32(p)}
	local0 := time.Unix(5000, 0)
	ms := func(f float64) uint32 { return uint32(math.Round(f * float64(p))) }
	localAt := func(f float64) time.Time { return local0.Add(time.Duration(ms(f)) * time.Millisecond) }
	// The receiver starts with whatever the source last sent: a teach of
	// the never-counted counter, if no event came first.
	recv := rate.Counter{Value: src.Wire(clock0), Period: p, Received: local0}
	evs := sc.events
	for _, at := range sc.checks {
		for len(evs) > 0 && evs[0].at <= at {
			now := clock0 + ms(evs[0].at)
			src.Hit(now, evs[0].n)
			recv = rate.Counter{Value: src.Wire(now), Period: p, Received: localAt(evs[0].at)}
			evs = evs[1:]
		}
		now := clock0 + ms(at)
		got, want := recv.At(localAt(at)), src.Read(now)
		if got != want {
			t.Fatalf("%s at %.2fP: evaluated %d, source reads %d (wire %+v)", sc.name, at, got, want, recv.Value)
		}
		if sc.zeroFrom >= 0 && at >= sc.zeroFrom {
			if got != 0 {
				t.Fatalf("%s at %.2fP: %d, want decayed to 0", sc.name, at, got)
			}
			if next, ok := recv.Next(localAt(at)); ok {
				t.Fatalf("%s at %.2fP: decayed reading changes again at %v", sc.name, at, next)
			}
		}
		if next, ok := recv.Next(localAt(at)); ok {
			if !next.After(localAt(at)) || recv.At(next) >= got || recv.At(next.Add(-time.Millisecond)) != got {
				t.Fatalf("%s at %.2fP: Next %v is not the first drop", sc.name, at, next.Sub(local0))
			}
		}
	}
}

// TestScenarios runs the one-event, burst, steady, empty, and
// multi-period-idle cases at both periods against the simulated source,
// on a source clock far from its wrap and on one that wraps mid-scenario.
func TestScenarios(t *testing.T) {
	forEachPeriod(t, func(t *testing.T, p peermsg.Millis) {
		for _, sc := range scenarios {
			for _, clock0 := range []uint32{123456789, math.MaxUint32 - uint32(p)/2} {
				t.Run(fmt.Sprintf("%s/clock%d", sc.name, clock0), func(t *testing.T) {
					runScenario(t, sc, p, clock0)
				})
			}
		}
	})
}

// TestRolloverAge: a wire age computed across the sender's 32-bit clock
// wrap is an ordinary small age.
func TestRolloverAge(t *testing.T) {
	forEachPeriod(t, func(t *testing.T, p peermsg.Millis) {
		src := &ratetest.Source{Period: uint32(p)}
		start := uint32(math.MaxUint32 - 1000)
		src.Hit(start, 5)
		now := start + uint32(p)/2 // past the wrap
		fc := src.Wire(now)
		if want := peermsg.Millis(uint32(p)/2) + 1; fc.Age != want && fc.Age != want-1 {
			t.Fatalf("age across the wrap %d, want about %d", fc.Age, want)
		}
		c := rate.Counter{Value: fc, Period: p, Received: time.Unix(0, 0)}
		if got, want := c.At(time.Unix(0, 0)), src.Read(now); got != want || got != 5 {
			t.Fatalf("reading %d, source %d, want 5", got, want)
		}
	})
}

// TestDelayAndClock documents the error behavior of a delayed update and
// of clock advancement: a counter received d late reads as the source
// did d earlier, never below the source's current reading, and by at
// most DecayBound(d + 1ms) above it; a time before reception reads as at
// reception; sub-millisecond advances change nothing; and a clock jump
// of any size decays to 0 without wrapping.
func TestDelayAndClock(t *testing.T) {
	forEachPeriod(t, func(t *testing.T, p peermsg.Millis) {
		P := uint32(p)
		src := &ratetest.Source{Period: P}
		clock0 := uint32(77777)
		for i := range uint32(40) {
			src.Hit(clock0+i*P/40, 1)
		}
		send := clock0 + P + P/3 // in the second period
		src.Hit(send, 7)
		fc := src.Wire(send)
		local0 := time.Unix(9000, 0)
		for _, delay := range []time.Duration{
			0, time.Millisecond, 3 * time.Millisecond, 250 * time.Millisecond,
			2 * time.Second,
		} {
			delayMS := uint32(delay / time.Millisecond)
			// Read at local time local0 + delay + k; the source then
			// reads at send + delay + k.
			c := rate.Counter{Value: fc, Period: p, Received: local0.Add(delay)}
			for k := uint32(0); k < 3*P; k += P / 50 {
				at := local0.Add(delay + time.Duration(k)*time.Millisecond)
				got := c.At(at)
				if want := src.Read(send + k); got != want {
					t.Fatalf("delay %v +%dms: %d, want the source's reading %d at the send time plus %dms", delay, k,
						got, want, k)
				}
				truth := src.Read(send + delayMS + k)
				bound := rate.DecayBound(fc.Curr, fc.Prev, p, delay+time.Millisecond)
				if got < truth || got-truth > bound {
					t.Fatalf("delay %v +%dms: %d vs source now %d, outside [0, %d] overstatement", delay, k, got, truth,
						bound)
				}
			}
		}
		c := rate.Counter{Value: fc, Period: p, Received: local0}
		if got, want := c.At(local0.Add(-time.Hour)), c.At(local0); got != want {
			t.Fatalf("before reception %d, at reception %d", got, want)
		}
		if got, want := c.At(local0.Add(999*time.Microsecond)), c.At(local0); got != want {
			t.Fatalf("sub-millisecond advance changed %d to %d", want, got)
		}
		// 2^32 ms after reception, a reading modulo 2^32 would return to
		// the received value; the local age does not wrap.
		for _, jump := range []time.Duration{
			3 * time.Duration(p) * time.Millisecond,
			math.MaxUint32 * time.Millisecond, (math.MaxUint32 + 1) * time.Millisecond, 400 * 24 * time.Hour,
		} {
			if got := c.At(local0.Add(jump)); got != 0 {
				t.Fatalf("jump %v: %d, want 0", jump, got)
			}
			if _, ok := c.Next(local0.Add(jump)); ok {
				t.Fatalf("jump %v: decayed counter changes again", jump)
			}
		}
		if got := c.Elapsed(local0.Add((math.MaxUint32 + 1) * time.Millisecond)); got != uint64(fc.Age)+math.MaxUint32+1 {
			t.Fatalf("elapsed %d wrapped", got)
		}
	})
}

// TestDecayBound checks that no interval's fall exceeds DecayBound.
func TestDecayBound(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for range 200000 {
		p := peermsg.Millis(1 + rng.Uint32N(70000))
		c, pr := uint32(rng.IntN(3000)), uint32(rng.IntN(3000))
		if rng.IntN(4) == 0 {
			c, pr = uint32(rng.IntN(3)), uint32(rng.IntN(3))
		}
		e := rng.Uint64N(3 * uint64(p))
		d := time.Duration(rng.Int64N(int64(2*p))) * time.Millisecond / 3
		from := rate.Counter{Value: peermsg.FreqCounter{Curr: c, Prev: pr}, Period: p, Received: time.Unix(0, 0)}
		at := time.Unix(0, 0).Add(time.Duration(e) * time.Millisecond).Add(time.Duration(rng.IntN(1000)) *
			time.Microsecond)
		drop := from.At(at) - from.At(at.Add(d))
		if b := rate.DecayBound(c, pr, p, d); drop > b {
			t.Fatalf("P=%d curr=%d prev=%d age=%d: fall %d over %v exceeds bound %d", p, c, pr, e, drop, d, b)
		}
	}
	if b := rate.DecayBound(0, 0, 10000, time.Hour); b != 0 {
		t.Fatalf("empty counter bound %d", b)
	}
	if b := rate.DecayBound(math.MaxUint32, math.MaxUint32, 1, math.MaxInt64); b != math.MaxUint64 {
		t.Fatalf("huge bound %d, want saturation", b)
	}
}

// TestRanges covers the supported period range and the wire-age range.
func TestRanges(t *testing.T) {
	for _, p := range []peermsg.Millis{0, rate.MaxPeriod + 1, math.MaxUint32} {
		if err := rate.CheckPeriod(p); !errors.Is(err, rate.ErrPeriod) {
			t.Errorf("CheckPeriod(%d) = %v, want ErrPeriod", p, err)
		}
	}
	for _, p := range []peermsg.Millis{1, 10000, 60000, rate.MaxPeriod} {
		if err := rate.CheckPeriod(p); err != nil {
			t.Errorf("CheckPeriod(%d) = %v", p, err)
		}
	}
	if got := rate.Estimate(5, 5, 0, 0); got != 0 {
		t.Errorf("zero period read %d", got)
	}
	if got := rate.MaxAge(10000); got != 10000+1<<31 {
		t.Errorf("MaxAge(10s) = %d", got)
	}
}

// wrappedRead is the reading as upstream's arithmetic yields it from a
// 32-bit wire age: the remainder of the period is the signed 32-bit
// value of P - age modulo 2^32, so it wraps for ages beyond P + 2^31.
// The proportional reading is otherwise the specified one.
func wrappedRead(curr, prev uint32, p peermsg.Millis, age uint32) uint64 {
	remain := int64(int32(uint32(p) - age))
	newer, older := uint64(curr), uint64(prev)
	if remain < 0 {
		remain += int64(p)
		if remain >= 0 {
			older = newer
		} else {
			older = 0
		}
		newer = 0
	}
	if newer == 0 && older <= 1 {
		return older
	}
	return (older*uint64(remain) + newer*uint64(p)) / uint64(p)
}

// TestAgeRange: upstream's reading is the specified one for every wire
// age up to P + 2^31 and wraps just beyond it, which is exactly where
// AgeInRange turns false; an out-of-range counter evaluates to 0, and an
// empty counter is in range at any age.
func TestAgeRange(t *testing.T) {
	for _, p := range []peermsg.Millis{1, 10000, 60000, 1 << 30, rate.MaxPeriod - 1, rate.MaxPeriod} {
		limit := rate.MaxAge(p)
		ages := []uint64{
			0, uint64(p), 2 * uint64(p), 2*uint64(p) + 1, 1<<31 - 1, 1 << 31, limit - 1, limit,
			limit + 1, limit + 2, math.MaxUint32,
		}
		for _, age := range ages {
			if age > math.MaxUint32 {
				continue
			}
			for _, cp := range [][2]uint32{{3, 0}, {0, 3}, {3, 7}, {1, 0}, {0, 1}} {
				ctr := rate.Counter{
					Value:  peermsg.FreqCounter{Age: peermsg.Millis(age), Curr: cp[0], Prev: cp[1]},
					Period: p,
				}
				spec := rate.Estimate(cp[0], cp[1], p, age)
				native := wrappedRead(cp[0], cp[1], p, uint32(age))
				in := age <= limit
				if ctr.AgeInRange() != in {
					t.Fatalf("P=%d age=%d %v: AgeInRange %v, want %v", p, age, cp, !in, in)
				}
				if in && native != spec {
					t.Fatalf("P=%d age=%d %v: in range but upstream reads %d, specified %d", p, age, cp, native, spec)
				}
				if !in && spec != 0 {
					t.Fatalf("P=%d age=%d %v: out-of-range counter reads %d, want 0", p, age, cp, spec)
				}
			}
			if age == limit+1 && wrappedRead(3, 0, p, uint32(age)) == 0 {
				t.Fatalf("P=%d: upstream does not misread just beyond the limit; the bound is wrong", p)
			}
		}
		empty := rate.Counter{Value: peermsg.FreqCounter{Age: math.MaxUint32}, Period: p}
		if !empty.AgeInRange() {
			t.Fatalf("P=%d: empty counter out of range", p)
		}
	}
}

// TestZeroAt checks against the reference reading that a counter reads 0
// from ZeroAt on and, unless it already did at Received, not 1 ms
// before; for empty counters, long-idle ones, and the low-rate case.
func TestZeroAt(t *testing.T) {
	base := time.Unix(1000, 0)
	for _, p := range []uint32{1, 7, 10000} {
		for _, c := range []struct{ curr, prev uint32 }{{0, 0}, {1, 0}, {0, 1}, {0, 2}, {5, 9}} {
			for _, age := range []uint32{0, 1, p - 1, p, 2 * p, 2*p + 1, 3 * p, math.MaxUint32} {
				ctr := rate.Counter{
					Value:  peermsg.FreqCounter{Age: peermsg.Millis(age), Curr: c.curr, Prev: c.prev},
					Period: peermsg.Millis(p), Received: base,
				}
				z := ctr.ZeroAt()
				if z.Before(base) {
					t.Fatalf("P=%d %+v age %d: ZeroAt %v before Received", p, c, age, z)
				}
				for _, d := range []time.Duration{0, time.Millisecond, time.Duration(3*p) * time.Millisecond} {
					if got := ratetest.ReadReceived(ctr.Value, p, base, z.Add(d)); got != 0 {
						t.Fatalf("P=%d %+v age %d: reads %d at ZeroAt+%v", p, c, age, got, d)
					}
				}
				if z.After(base) {
					if got := ratetest.ReadReceived(ctr.Value, p, base, z.Add(-time.Millisecond)); got == 0 {
						t.Fatalf("P=%d %+v age %d: already 0 1 ms before ZeroAt %v", p, c, age, z.Sub(base))
					}
				}
			}
		}
	}
}
