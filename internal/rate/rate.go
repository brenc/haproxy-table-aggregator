// Package rate evaluates one source's HAProxy http_req_rate counter, as
// the peers protocol delivers it, at a chosen time on the local monotonic
// clock: the integer reading that HAProxy's own sample fetches and "show
// table" would report for the counter, given no further events.
//
// # Behavioral specification
//
// This is a specification of observed and documented behavior, derived
// from the pinned upstream releases 3.4.6 and 3.2.25 (src/freq_ctr.c,
// include/haproxy/freq_ctr.h, the STD_T_FRQP cases of src/peers.c and
// src/stick_table.c) and checked against both stock builds in the lab. The
// code here is original; nothing is translated from upstream.
//
// A counter holds three values: the start of its current period on the
// sender's millisecond clock (a wrapping 32-bit tick, always even), the
// events counted since then (Curr), and the events of the period before
// (Prev). An event 2P or more after the start rotates the counter to a
// fresh period starting at that event (rounded down to even) with Prev 0;
// an event at least P but less than 2P after the start moves Curr into
// Prev and starts the next period P after the old start (rounded down to
// even). Reading never changes the counter.
//
// Reading at e milliseconds after the current period's start, for the
// configured period P in milliseconds, returns an unsigned integer:
//
//   - e <= P: Curr + floor(Prev*(P-e)/P). The previous period is assumed
//     uniform and the part of it still inside a trailing window of length
//     P is added.
//   - P < e <= 2P: the reading as if the counter had rotated with no
//     events: floor(Curr*(2P-e)/P).
//   - e > 2P: 0. Every counted event is older than a whole period.
//
// Low-rate correction: after the case selection above, if the current
// period's count is 0 and the previous one's is 0 or 1, the reading is
// that previous count exactly, with no proportional decay. So a single
// event reads 1 from the moment it is counted until 2P after the start of
// the period that counted it, then 0; it does not flap between 0 and 1.
// Two or more events decay proportionally as above.
//
// Rounding is truncation toward zero of the exact quotient (the upstream
// computes a 64-bit numerator and divides by P), so a reading is never
// above the events counted in the current and previous periods. The unit
// is events per period P, not per second.
//
// Empty: Curr = Prev = 0 reads 0 at every age. A counter that never
// counted an event has tick 0, so its wire age is the sender's raw clock
// value, which is arbitrary; only the counts identify it as empty.
//
// # Wire age and the local clock
//
// The sender encodes Age = its now_ms minus the tick, modulo 2^32, which
// is correct across the 32-bit wrap of now_ms (rollover of the clock). A
// HAProxy receiver rebuilds the tick as its own now_ms minus Age, rounded
// down to even. This package keeps the counter on the local monotonic
// clock instead: at local time t its age is
//
//	e(t) = Age + floor((t - Received) / 1ms)
//
// where Received is when the local session read the update. A time before
// Received is evaluated at Received. e grows without bound and is never
// reduced modulo 2^32, so a counter left idle decays to 0 and stays there.
//
// Transport delay approximation: the wire carries no send timestamp, so
// the time δ between the sender encoding the update and the local session
// reading it (network, queueing, and any backlog) is taken as 0. The
// evaluated reading at local time t is the sender's native reading at its
// own time t - δ, i.e. it lags the sender by δ plus up to 1 ms of
// truncation. Since a reading only falls without new events, the lag can
// only overstate the decay-only reading, by at most DecayBound(δ + 1ms).
// Events the sender counted after encoding are invisible until their own
// update arrives; that understatement is bounded by update propagation,
// not by this package.
//
// # Version comparison and out-of-range inputs
//
// The read path (period selection, low-rate correction, truncating
// division), the rotation rule, and the peers encoding and decoding of the
// counter are behaviorally identical in 3.4.6 and 3.2.25. The only
// differences in those files are that 3.4.6 reaches the global clock
// through a pointer and that it clamps freq_ctr_overshoot_period, which
// http_req_rate reads do not use.
//
// Upstream computes the remaining part of the period, P - e, as a signed
// 32-bit value from the wrapping 32-bit age e. That is exact while
// P - e >= -2^31, so for a period of at most math.MaxInt32 ms its reading
// is the specified one for every age up to P + 2^31 ms (about 24.8 days
// past the period) and reads 0 beyond 2P; at a larger age the remainder
// wraps positive and a long-idle counter reads as if it were current
// again. This package refuses periods above MaxPeriod, evaluates any age
// exactly (an idle counter reads 0), and reports a non-empty counter whose
// wire Age already exceeds MaxAge(P) through Counter.AgeInRange, since
// its sender's native reading was then not the decayed rate. Such a
// counter is older than 2P, so its exact reading is 0.
//
// A reading can exceed 32 bits (Curr and Prev are each up to 2^32 - 1);
// upstream truncates its 64-bit quotient to 32 bits. This package returns
// the exact 64-bit reading and leaves narrowing, with a check, to callers.
package rate

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

// ErrPeriod reports a period outside 1..MaxPeriod milliseconds.
var ErrPeriod = errors.New("rate: unsupported period")

// MaxPeriod is the largest supported period. Upstream treats the
// remaining part of a period as a signed 32-bit millisecond count.
const MaxPeriod peermsg.Millis = math.MaxInt32

// MaxAge returns the largest age, in milliseconds, at which upstream's
// reading of a counter with a period of period (at most MaxPeriod) is
// the specified one: period + 2^31. See the package documentation.
func MaxAge(period peermsg.Millis) uint64 { return uint64(period) + 1<<31 }

// CheckPeriod returns an error wrapping ErrPeriod unless period is from 1
// to MaxPeriod milliseconds.
func CheckPeriod(period peermsg.Millis) error {
	if period == 0 || period > MaxPeriod {
		return fmt.Errorf("%w: %d ms, want 1 to %d ms", ErrPeriod, period, MaxPeriod)
	}
	return nil
}

// Estimate returns the native reading of a counter holding curr events in
// its current period and prev in the previous one, elapsed milliseconds
// after its current period started, for period milliseconds (see the
// package documentation). It returns 0 for a period of 0; callers check
// the period with CheckPeriod. The result is exact and may exceed 32 bits.
func Estimate(curr, prev uint32, period peermsg.Millis, elapsed uint64) uint64 {
	p := uint64(period)
	if p == 0 {
		return 0
	}
	var keep, weighted uint64 // reading = keep + floor(weighted*remain/p)
	var remain uint64
	switch {
	case elapsed <= p:
		keep, weighted, remain = uint64(curr), uint64(prev), p-elapsed
	case elapsed <= 2*p:
		keep, weighted, remain = 0, uint64(curr), 2*p-elapsed
	default:
		return 0
	}
	if keep == 0 && weighted <= 1 {
		return weighted
	}
	hi, lo := bits.Mul64(weighted, remain)
	// remain <= p, so the quotient is at most weighted and hi < p.
	q, _ := bits.Div64(hi, lo, p)
	return keep + q
}

// nextDrop returns the smallest age above elapsed at which Estimate is
// lower than at elapsed, or false if it never is. Estimate is
// non-increasing in the age and 0 beyond 2*period, so a binary search
// over (elapsed, 2*period+1] finds it.
func nextDrop(curr, prev uint32, period peermsg.Millis, elapsed uint64) (uint64, bool) {
	v := Estimate(curr, prev, period, elapsed)
	if v == 0 {
		return 0, false
	}
	lo, hi := elapsed, 2*uint64(period)+1
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		if Estimate(curr, prev, period, mid) < v {
			hi = mid
		} else {
			lo = mid
		}
	}
	return hi, true
}

// Counter is one source's http_req_rate counter as received, placed on
// the local monotonic clock.
type Counter struct {
	// Value is the counter as decoded from the wire; Value.Age is
	// relative to Received.
	Value peermsg.FreqCounter
	// Period is the counter's configured period.
	Period peermsg.Millis
	// Received is when the local session read the update (monotonic).
	Received time.Time
}

// Elapsed returns the counter's age at local time at: the milliseconds
// since its current period started, Value.Age plus the whole milliseconds
// since Received (none if at is before Received).
func (c Counter) Elapsed(at time.Time) uint64 {
	d := at.Sub(c.Received)
	if d < 0 {
		d = 0
	}
	return uint64(c.Value.Age) + uint64(d/time.Millisecond)
}

// At returns the counter's native reading at local time at, assuming no
// further events (see Estimate).
func (c Counter) At(at time.Time) uint64 {
	return Estimate(c.Value.Curr, c.Value.Prev, c.Period, c.Elapsed(at))
}

// Next returns the earliest local time after at when the reading falls
// below its value at at, or false if it never changes again without new
// events (it is 0, or will stay at its value). The reading is constant on
// [at, next).
func (c Counter) Next(at time.Time) (time.Time, bool) {
	e := c.Elapsed(at)
	next, ok := nextDrop(c.Value.Curr, c.Value.Prev, c.Period, e)
	if !ok {
		return time.Time{}, false
	}
	// next > e >= Value.Age, so the offset from Received is positive,
	// and next <= 2*period+1 < 2^33 ms fits a Duration.
	//nolint:gosec // G115: bounded above.
	return c.Received.Add(time.Duration(next-uint64(c.Value.Age)) * time.Millisecond), true
}

// Empty reports that the counter holds no events: it reads 0 at any age.
func (c Counter) Empty() bool { return c.Value.Curr == 0 && c.Value.Prev == 0 }

// ZeroAt returns the earliest local time from which the counter reads 0
// and keeps reading 0 without new events: Received for an empty counter
// or one that already reads 0, and at the latest once its age exceeds two
// periods. Every event it counted is then outside the window its reading
// covers. The reading is non-increasing in the age, so the first zero
// is found by binary search, as Next finds the first drop.
func (c Counter) ZeroAt() time.Time {
	age := uint64(c.Value.Age)
	if Estimate(c.Value.Curr, c.Value.Prev, c.Period, age) == 0 {
		return c.Received
	}
	lo, hi := age, 2*uint64(c.Period)+1 // reads > 0 at lo, 0 at hi
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		if Estimate(c.Value.Curr, c.Value.Prev, c.Period, mid) == 0 {
			hi = mid
		} else {
			lo = mid
		}
	}
	// hi - age <= 2*MaxPeriod+1 < 2^33 ms fits a Duration.
	//nolint:gosec // G115: bounded above.
	return c.Received.Add(time.Duration(hi-age) * time.Millisecond)
}

// AgeInRange reports whether the sender's native reading of the counter
// was the specified one when it was sent: the counter is empty or its
// wire age is at most MaxAge(Period).
func (c Counter) AgeInRange() bool { return c.Empty() || uint64(c.Value.Age) <= MaxAge(c.Period) }

// DecayBound returns an upper bound on how much the reading of a counter
// with these counts can fall during any interval of length d without new
// events: floor((curr+prev)*ceil(d/1ms)/period) + 1, or 0 for an empty
// counter or a zero d. It bounds the staleness of a reading published d
// before it is replaced, and the overstatement caused by a transport delay
// of d. The result saturates at math.MaxUint64.
func DecayBound(curr, prev uint32, period peermsg.Millis, d time.Duration) uint64 {
	if (curr == 0 && prev == 0) || d <= 0 || period == 0 {
		return 0
	}
	ms := uint64(d / time.Millisecond)
	if d%time.Millisecond != 0 {
		ms++
	}
	hi, lo := bits.Mul64(uint64(curr)+uint64(prev), ms)
	if hi >= uint64(period) {
		return math.MaxUint64
	}
	q, _ := bits.Div64(hi, lo, uint64(period))
	if q == math.MaxUint64 {
		return q
	}
	return q + 1
}
