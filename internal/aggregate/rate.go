package aggregate

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/rate"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
)

// ErrIncomplete reports a rate that does not cover every configured
// source, so it cannot be published as authoritative.
var ErrIncomplete = errors.New("aggregate: incomplete rate")

// DefaultMinInterval is the default Cadence.MinInterval.
const DefaultMinInterval = 100 * time.Millisecond

// RateContribution describes one roster source's part in a RateTotal.
type RateContribution struct {
	// Source is the configured source name.
	Source string
	// State and Reason are the source's state when the rate was read.
	State  snapshot.State
	Reason string
	// Present reports that the source holds an unexpired entry for the
	// key. A Ready source without one contributes 0.
	Present bool
	// Counter is the entry's http_req_rate counter on the local clock,
	// if Present.
	Counter rate.Counter
	// Deadline is when the entry expires locally, if Present.
	Deadline time.Time
	// Session is the source session that delivered the entry, if
	// Present.
	Session uint64
	// Elapsed is the counter's age at RateTotal.At in milliseconds (see
	// rate.Counter.Elapsed), and Estimate its native reading then.
	Elapsed  uint64
	Estimate uint64
	// Counted reports that Estimate is included in RateTotal.Sum: the
	// entry is Present and the source Ready.
	Counted bool
	// HeldOver reports a Present entry delivered by an earlier session
	// than the source's current one (see Contribution.HeldOver); it is
	// never Counted.
	HeldOver bool
	// AgeOutOfRange reports a non-empty counter whose wire age exceeded
	// rate.MaxAge(Period), so its sender's native reading was not the
	// decayed rate (see rate.Counter.AgeInRange). Its Estimate is the
	// exact decayed reading, 0.
	AgeOutOfRange bool
	// Next is when this contribution next changes without new input:
	// its estimate falls, or the entry expires while its estimate is
	// above 0. Zero if it never changes again.
	Next time.Time
}

// RateTotal is the aggregate http_req_rate of one key at one moment: the
// sum of each Ready source's native integer estimate, every estimate
// evaluated at the same local time At. See the package documentation of
// package rate for the estimator, its units, and its error behavior.
type RateTotal struct {
	// Table and Key identify the logical key.
	Table string
	Key   peermsg.Key
	// Period is the table's configured http_req_rate period; Sum is in
	// estimated requests per Period.
	Period peermsg.Millis
	// At is the store clock's time the states and entries were read at
	// and every estimate was evaluated at.
	At time.Time
	// Sum is the sum of the Counted estimates.
	Sum uint64
	// Complete reports that every configured source was Ready at At.
	Complete bool
	// Uncertain reports a counted contribution that is HeldOver (which a
	// Ready source cannot hold; kept as a defensive check) or
	// AgeOutOfRange. Lost history does not make a rate uncertain: the
	// source is not Ready until it could no longer change one.
	Uncertain bool
	// Next is the earliest Next of the counted contributions: until then
	// Sum stays the same unless new input arrives or a source changes
	// state. Zero if no counted contribution changes again.
	Next time.Time
	// Sources describes every configured source, in roster order.
	Sources []RateContribution
}

// Rate reads the roster and every source's entry for table and key from
// store in one consistent view, evaluates each entry's http_req_rate at
// the store clock's reading of that view, and sums the Ready sources'
// estimates. The store's clock (snapshot.Options.Now) is the evaluator's
// injected clock. Rate needs no new input to change: calling it later
// evaluates the same retained counters at a later time, so rates decay in
// silence, and an expired entry, gone from the store, never contributes
// again.
//
// It returns an error wrapping ErrUnknownTable for a table the store does
// not keep, rate.ErrPeriod if the table's period is unsupported or an
// entry's period differs from it (never converted), or ErrOverflow if the
// sum does not fit 64 bits.
func Rate(store *snapshot.Store, table string, key peermsg.Key) (RateTotal, error) {
	spec, ok := store.Table(table)
	if !ok {
		return RateTotal{}, fmt.Errorf("%w: %q", ErrUnknownTable, table)
	}
	if err := rate.CheckPeriod(spec.Period); err != nil {
		return RateTotal{}, fmt.Errorf("table %s: %w", table, err)
	}
	roster, entries, ok := store.KeyView(table, key)
	if !ok {
		return RateTotal{}, fmt.Errorf("%w: %q", ErrUnknownTable, table)
	}
	return sumRates(table, key, spec.Period, roster, entries)
}

func sumRates(table string, key peermsg.Key, period peermsg.Millis, roster snapshot.Roster,
	entries []snapshot.Contribution,
) (RateTotal, error) {
	byName := make(map[string]snapshot.Entry, len(entries))
	for _, c := range entries {
		byName[c.Source] = c.Entry
	}
	at := roster.At
	r := RateTotal{Table: table, Key: key, Period: period, At: at, Complete: roster.Ready}
	for _, src := range roster.Sources {
		c := RateContribution{Source: src.Name, State: src.State, Reason: src.Reason}
		if e, ok := byName[src.Name]; ok {
			if e.Period != period {
				return RateTotal{}, fmt.Errorf("table %s key %v source %s: %w: entry period %v, table period %v",
					table, key, src.Name, rate.ErrPeriod, e.Period, period)
			}
			c.Present, c.Session, c.Deadline = true, e.Session, e.Deadline
			c.Counter = rate.Counter{Value: e.Rate, Period: e.Period, Received: e.Received}
			c.Elapsed, c.Estimate = c.Counter.Elapsed(at), c.Counter.At(at)
			c.HeldOver = e.Session < src.Session
			c.AgeOutOfRange = !c.Counter.AgeInRange()
			c.Counted = src.State == snapshot.Ready
			if next, ok := c.Counter.Next(at); ok {
				c.Next = next
			}
			if c.Estimate > 0 && (c.Next.IsZero() || e.Deadline.Before(c.Next)) {
				c.Next = e.Deadline
			}
		}
		if c.Counted {
			var err error
			if r.Sum, err = add(r.Sum, c.Estimate); err != nil {
				return RateTotal{}, fmt.Errorf("table %s key %v source %s: %w", table, key, src.Name, err)
			}
			r.Uncertain = r.Uncertain || c.HeldOver || c.AgeOutOfRange
			if !c.Next.IsZero() && (r.Next.IsZero() || c.Next.Before(r.Next)) {
				r.Next = c.Next
			}
		}
		r.Sources = append(r.Sources, c)
	}
	return r, nil
}

// SumAt returns the sum the counted contributions would give at local
// time at, assuming no new input and no change of source state: each
// counter evaluated at at, and an entry whose Deadline is not after at
// contributing nothing. SumAt(r.At) is r.Sum. It saturates at
// math.MaxUint64 rather than wrapping.
func (r RateTotal) SumAt(at time.Time) uint64 {
	var s uint64
	for _, c := range r.Sources {
		if !c.Counted || !c.Deadline.After(at) {
			continue
		}
		var err error
		if s, err = add(s, c.Counter.At(at)); err != nil {
			return math.MaxUint64
		}
	}
	return s
}

// DecayBound returns an upper bound on how much Sum can fall during any
// interval of length d without new input: the sum of rate.DecayBound
// over the counted contributions, saturating at math.MaxUint64. An entry
// that expires within d may also remove its whole estimate.
func (r RateTotal) DecayBound(d time.Duration) uint64 {
	var s uint64
	for _, c := range r.Sources {
		if !c.Counted {
			continue
		}
		var err error
		b := rate.DecayBound(c.Counter.Value.Curr, c.Counter.Value.Prev, c.Counter.Period, d)
		if s, err = add(s, b); err != nil {
			return math.MaxUint64
		}
	}
	return s
}

// Uint32 returns Sum narrowed to the unsigned 32-bit aggregate rate
// slot of the output schema (output.SlotRate), or an error wrapping
// ErrOverflow if it exceeds math.MaxUint32. Nothing wraps or saturates.
func (r RateTotal) Uint32() (uint32, error) {
	if r.Sum > math.MaxUint32 {
		return 0, fmt.Errorf("%w: rate %d of table %s key %v exceeds 32 bits", ErrOverflow, r.Sum, r.Table, r.Key)
	}
	return uint32(r.Sum), nil
}

// Authoritative returns the value the output rate slot may carry as an
// authoritative aggregate, or an error if the rate must not be published
// as one: ErrIncomplete unless every configured source was Ready, or
// ErrOverflow if Sum does not fit the slot. Uncertain is not an error; it
// is reported for diagnostics. A caller that gets an error must not
// certify the returned value (publish it under a valid lease). After
// ErrOverflow the sum is still complete, so a caller may certify
// math.MaxUint32 as a lower bound in its place (package publish does);
// after ErrIncomplete, falling back to local protection is the safe
// outcome.
func (r RateTotal) Authoritative() (uint32, error) {
	if !r.Complete {
		return 0, fmt.Errorf("%w: table %s key %v at %v", ErrIncomplete, r.Table, r.Key, r.At)
	}
	return r.Uint32()
}

// Cadence bounds how often a changing rate is reevaluated for
// publication. Between evaluations the published value is the sum at the
// last evaluation; without new input the true sum can only fall, so a
// published rate overstates the current one by at most the decay since
// it was evaluated, and never understates it because of decay.
type Cadence struct {
	// MinInterval is the shortest time between two evaluations of one
	// key; zero means DefaultMinInterval. The overstatement budget of a
	// published value is RateTotal.DecayBound(MinInterval) (plus, for an
	// entry that expires within MinInterval, its estimate).
	MinInterval time.Duration
}

// Due returns when r must next be reevaluated if no new input arrives
// and no source changes state: at r.Next, but no earlier than
// MinInterval after r.At. It returns false if r never changes again
// without input, so a decayed key needs no further evaluation.
func (c Cadence) Due(r RateTotal) (time.Time, bool) {
	if r.Next.IsZero() {
		return time.Time{}, false
	}
	minInterval := c.MinInterval
	if minInterval <= 0 {
		minInterval = DefaultMinInterval
	}
	if earliest := r.At.Add(minInterval); r.Next.Before(earliest) {
		return earliest, true
	}
	return r.Next, true
}
