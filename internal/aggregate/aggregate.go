// Package aggregate combines the sources' current snapshots of one key:
// Count sums their http_req_cnt values into a diagnostic total, and Rate
// sums their http_req_rate estimates into the aggregate rate.
//
// # Scope
//
// The count total is a diagnostic for snapshot correctness. It is not a
// billing count, a quota, or a durable or lifetime request total, and
// must not be published or enforced as one. It is the sum of the values
// the sources' stick-table entries hold right now, as the snapshot store
// last accepted them. It forgets everything an entry counted once the
// entry expires at its source or here, is evicted, or is reset; it is lost
// on restart; and it inherits HAProxy's 32-bit per-entry counter.
//
// # Rates
//
// Rate evaluates each Ready source's retained http_req_rate counter at
// one common local time, the store clock's reading of the view, as the
// native integer reading HAProxy would give for it (package rate), and
// sums those integers. Raw counter fields are never added across
// sources: their periods start at different times. The unit is estimated
// requests per configured period. Evaluating later with no new input
// decays the result, and RateTotal.Next says when it next changes, so a
// publisher can reevaluate without waiting for updates (Cadence bounds how
// often). Only a Complete rate may be published as authoritative, and only
// if it fits the 32-bit output slot (RateTotal.Authoritative).
//
// # Arithmetic
//
// The store keeps one absolute value per source, table, and key, and an
// accepted update replaces only its own source's value (package
// snapshot). Count recomputes the sum from those current values every
// time it is called, so a replayed or repeated snapshot record cannot
// inflate the total, a replaced value counts once at its new value, and an
// expired one stops counting exactly when the store stops returning it.
// Absolute values are never treated as increments and no delta is ever
// derived from two of them. Sums are unsigned 64-bit with a checked carry
// (each term is at most math.MaxUint32); narrowing to an output field
// (Total.Uint32) is checked too, and neither ever wraps.
//
// # Decreases and the 32-bit boundary
//
// HAProxy stores http_req_cnt as an unsigned 32-bit integer and increments
// it without saturation, so one request at math.MaxUint32 makes it 0. A
// reset through the runtime API, and an entry that expired or was evicted
// at the source and then recreated, also lower the value. The wire cannot
// tell these apart. When an update lowers a source's value the store
// keeps the new, lower value (snapshot.Entry.Decreases): the total falls
// with it and never jumps by about 2^32. The contribution is marked
// Discontinuous and the total Uncertain for the rest of the entry's
// lifetime, because the entry's count no longer represents everything it
// counted. Only a decrease is detectable: an entry that expired or was
// evicted at the source and recreated at a count at or above the stored
// one looks like continuous counting and is not marked Uncertain. A value
// at the boundary itself (math.MaxUint32) is valid and sums exactly in 64
// bits.
//
// # Completeness
//
// Only a Ready source's contribution is summed: the store applies a
// session's snapshot in place while the source syncs, so a syncing,
// degraded, or disconnected source's retained entry may be partial or
// stale. Such entries stay visible in Total.Sources but are not counted.
// A total is Complete only when the roster is ready, every configured
// source Ready at the same instant the values were read. Membership is
// the configured roster: a source that disconnects or degrades keeps the
// total incomplete until it is Ready again or is removed from the
// configuration; connected peers never redefine it.
//
// # Recovery
//
// A finished resync releases every entry of the source that its session
// did not confirm (package snapshot, Recovery), so a Ready source holds
// only entries of its current session: a HeldOver entry, from an earlier
// session, is only ever seen on a source that is not Ready, and is never
// counted. When the store detected that a source lost history it held (an
// entry absent from or reset in a later session), the source stays
// Degraded until the lost entries could no longer change a rate; after
// that, its counts may still lack what the lost entries counted until
// their deadlines, so its contributions are marked HistoryLost and every
// total Uncertain until then (the lost keys are no longer known). Lost
// history is never restored or estimated.
package aggregate

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
)

var (
	// ErrOverflow reports a sum or a narrowed value that does not fit its
	// integer type. Nothing is wrapped or saturated.
	ErrOverflow = errors.New("aggregate: integer overflow")
	// ErrUnknownTable reports a table that is not a configured input
	// table of the store.
	ErrUnknownTable = errors.New("aggregate: unknown input table")
)

// Contribution describes one roster source's part in a Total.
type Contribution struct {
	// Source is the configured source name.
	Source string
	// State and Reason are the source's state when the total was read.
	State  snapshot.State
	Reason string
	// Present reports that the source holds an unexpired entry for the
	// key. A Ready source without one contributes 0.
	Present bool
	// Count is the entry's http_req_cnt, if Present.
	Count uint32
	// Session is the source session that delivered the entry, if
	// Present.
	Session uint64
	// Counted reports that Count is included in Total.Sum: the entry is
	// Present and the source Ready.
	Counted bool
	// HeldOver reports a Present entry delivered by an earlier session
	// than the source's current one: the current session's snapshot has
	// not confirmed it (yet), so it may no longer exist at the source.
	// Only a source that is not Ready can hold one, so it is never
	// Counted (see Recovery in the package documentation).
	HeldOver bool
	// HistoryLost reports that the source lost history it held for some
	// keys (snapshot.Loss) and that a lost entry would not have expired
	// yet at the total's time: the source's count for this key, present
	// or not, may lack what it counted. It is per source, not per key,
	// and makes the total Uncertain while the source is Ready.
	HistoryLost bool
	// Decreases and LastDecrease are the entry's recorded decreases
	// (snapshot.Entry.Decreases); Discontinuous is Decreases > 0.
	Decreases     uint64
	LastDecrease  snapshot.Decrease
	Discontinuous bool
}

// Total is the diagnostic http_req_cnt total of one key. It is not a
// durable or billing count; see the package documentation.
type Total struct {
	// Table and Key identify the logical key.
	Table string
	Key   peermsg.Key
	// At is the store clock's time the values and states were read at.
	At time.Time
	// Sum is the sum of the Counted contributions.
	Sum uint64
	// Complete reports that every configured source was Ready at At, so
	// Sum covers every source. Otherwise Sum covers only the Ready
	// sources and is not a total of the roster.
	Complete bool
	// Uncertain reports that a counted contribution is Discontinuous or
	// HeldOver, or that a Ready source's contribution is HistoryLost:
	// Sum is still the sum of the current values, but those values may
	// not represent everything the entries counted.
	Uncertain bool
	// Sources describes every configured source, in roster order,
	// whether or not it contributed.
	Sources []Contribution
}

// Count reads the roster and every source's entry for table and key from
// store in one consistent view and sums the Ready sources' counts. It
// returns an error wrapping ErrUnknownTable for a table the store does not
// keep, or ErrOverflow if the sum does not fit 64 bits (impossible with
// fewer than 2^32 sources, but checked rather than assumed).
func Count(store *snapshot.Store, table string, key peermsg.Key) (Total, error) {
	roster, entries, ok := store.KeyView(table, key)
	if !ok {
		return Total{}, fmt.Errorf("%w: %q", ErrUnknownTable, table)
	}
	return sum(table, key, roster, entries)
}

func sum(table string, key peermsg.Key, roster snapshot.Roster, entries []snapshot.Contribution) (Total, error) {
	byName := make(map[string]snapshot.Entry, len(entries))
	for _, c := range entries {
		byName[c.Source] = c.Entry
	}
	t := Total{Table: table, Key: key, At: roster.At, Complete: roster.Ready}
	for _, src := range roster.Sources {
		c := Contribution{Source: src.Name, State: src.State, Reason: src.Reason}
		c.HistoryLost = src.Loss.CountUntil.After(roster.At)
		if e, ok := byName[src.Name]; ok {
			c.Present, c.Count, c.Session = true, e.Count, e.Session
			c.HeldOver = e.Session < src.Session
			c.Decreases, c.LastDecrease, c.Discontinuous = e.Decreases, e.LastDecrease, e.Decreases > 0
			c.Counted = src.State == snapshot.Ready
		}
		if c.Counted {
			var err error
			if t.Sum, err = add(t.Sum, uint64(c.Count)); err != nil {
				return Total{}, fmt.Errorf("table %s key %v source %s: %w", table, key, src.Name, err)
			}
			if c.Discontinuous || c.HeldOver {
				t.Uncertain = true
			}
		}
		if c.HistoryLost && src.State == snapshot.Ready {
			// Present or not: the lost entry may have been this key's.
			t.Uncertain = true
		}
		t.Sources = append(t.Sources, c)
	}
	return t, nil
}

// add returns a+b, or ErrOverflow if the result does not fit 64 bits.
func add(a, b uint64) (uint64, error) {
	s, carry := bits.Add64(a, b, 0)
	if carry != 0 {
		return 0, fmt.Errorf("%w: %d + %d exceeds 64 bits", ErrOverflow, a, b)
	}
	return s, nil
}

// Uint32 returns Sum narrowed to an unsigned 32-bit output field, such as
// one gpt slot of the output schema (package output), or an error wrapping
// ErrOverflow if it exceeds math.MaxUint32. It checks only the range:
// whether the total is Complete or Uncertain is the caller's decision.
func (t Total) Uint32() (uint32, error) {
	if t.Sum > math.MaxUint32 {
		return 0, fmt.Errorf("%w: total %d of table %s key %v exceeds 32 bits", ErrOverflow, t.Sum, t.Table, t.Key)
	}
	return uint32(t.Sum), nil
}
