// Package aggregatetest provides an independent trace oracle for the
// diagnostic totals and aggregate rates of package aggregate, for tests
// only.
//
// The Oracle records the events a snapshot store accepted or refused and
// the message reads observed for heartbeats, and recomputes from that
// trace, using the rules the plan states rather than any production code,
// which value each source holds for a key, which sources are ready, and
// what the total and rate therefore must be. It deliberately calls none
// of the snapshot store's merge, aggregate.Count, aggregate.Rate, or
// package rate (rates come from the reference in package ratetest): a
// test compares their results with Want and WantRate.
package aggregatetest

import (
	"slices"
	"sync"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/rate/ratetest"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// Oracle is a trace model of the diagnostic total. It is safe for
// concurrent use, so a live test can record from session goroutines.
//
// Its model: a source is ready when its current session is up, has
// completed a finished (not partial) resync, has defined every configured
// table, has no refused event since that resync, and last read a message
// less than Health ago. Each source keeps its latest accepted value per
// table and key (latest in trace order); an ordinary update lives for the
// table expiry from its reception, a timed one for its remaining lifetime
// capped at that expiry, and a zero lifetime removes the value. The total
// is the sum of the ready sources' unexpired values; it is complete when
// every roster source is ready. A counted value is uncertain if an update
// lowered it while its entry was live, if an earlier session than the
// source's current one delivered it, or while the source's lost history
// could still be missing from counts.
//
// Recovery (phase 11): a finished resync drops every value an earlier
// session delivered in each table the current session defined. A new key
// arriving while the source holds Capacity or more unexpired values first
// drops every earlier-session value in every table. A dropped value that
// would have outlived the drop by more than Grace, and a value replaced
// by a lower count from a later session, are lost history: the source is
// not ready until each such value's reference rate reading would have
// stayed 0 or the value would have expired, whichever is first, and its
// counted values are uncertain until the latest such value's deadline.
//
// Refusals: a refused event of the current session faults the source
// until a finished resync (a refused definition of a configured table
// also leaves it undefined); a refused event of any other session, and a
// refused SessionUp (one that does not start a newer session), change
// nothing. A TableRejected report is accepted, but faults the source and
// leaves the table undefined. The model assumes the trace's reception
// times never go backwards per source.
type Oracle struct {
	// Health is the snapshot store's health timeout.
	Health time.Duration
	// Tables are the configured input table names.
	Tables []string
	// Periods maps each table to its http_req_rate period in ms.
	Periods map[string]uint32
	// Capacity is the store's entry capacity per source; 0 means none.
	Capacity int
	// Grace is the store's loss grace (snapshot.LossGrace).
	Grace time.Duration

	mu    sync.Mutex
	trace []record
}

// record is one trace record: an accepted event, a refused one, or an
// observed message read (a heartbeat, which produces no event).
type record struct {
	ev       sources.Event
	refused  bool
	observed time.Time
}

// Record adds ev to the trace as accepted when err is nil, else refused:
// pass it the result of snapshot.Store.Apply.
func (o *Oracle) Record(ev sources.Event, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.trace = append(o.trace, record{ev: ev, refused: err != nil})
}

// Observe records that the source's session read a message at at, as
// passed to snapshot.Store.Observe.
func (o *Oracle) Observe(source string, session uint64, at time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.trace = append(o.trace, record{ev: sources.Event{Source: source, Session: session}, observed: at})
}

// Expectation is what the oracle expects of one key's total.
type Expectation struct {
	// Sum is the sum of the counted values.
	Sum uint64
	// Complete reports every roster source ready.
	Complete bool
	// Uncertain reports a counted value that was lowered or held over.
	Uncertain bool
	// Counted maps each ready source holding a value to that value.
	Counted map[string]uint32
}

type value struct {
	count    uint32
	rate     peermsg.FreqCounter
	received time.Time
	deadline time.Time
	session  uint64
	lowered  bool
}

type source struct {
	session uint64
	up      bool
	synced  bool
	fault   bool
	lastRx  time.Time
	defined map[string]bool
	values  map[string]map[peermsg.Key]value
	// rateUntil and countUntil bound the effect of lost history.
	rateUntil, countUntil time.Time
	// upAt is when the current session came up; expiry is each table's
	// last accepted announced expiry, in any session.
	upAt   time.Time
	expiry map[string]peermsg.Millis
}

// RateExpectation is what the oracle expects of one key's aggregate
// rate.
type RateExpectation struct {
	// Sum is the sum of the counted estimates.
	Sum uint64
	// Complete reports every roster source ready.
	Complete bool
	// Uncertain reports a counted value held over from an earlier
	// session, or a non-empty counter whose wire age exceeded the period
	// plus 2^31 ms.
	Uncertain bool
	// Counted maps each ready source holding a value to its estimate.
	Counted map[string]uint64
}

// Want returns the expectation for table and key at now, for the roster
// of configured source names.
func (o *Oracle) Want(roster []string, table string, key peermsg.Key, now time.Time) Expectation {
	x := Expectation{Complete: true, Counted: map[string]uint32{}}
	o.visit(roster, table, key, now, func(name string, s *source, v value, ok bool) {
		if !ok {
			x.Complete = false
			return
		}
		if now.Before(s.countUntil) {
			x.Uncertain = true
		}
		if v.session == 0 {
			return // ready, holding no value
		}
		x.Counted[name] = v.count
		x.Sum += uint64(v.count)
		x.Uncertain = x.Uncertain || v.lowered || v.session < s.session
	})
	return x
}

// WantRate returns the expectation for the aggregate rate of table and
// key at now, for the roster: each ready source's latest value evaluated
// at now by the reference reading of package ratetest for period (ms),
// then summed.
func (o *Oracle) WantRate(roster []string, table string, key peermsg.Key, period uint32, now time.Time,
) RateExpectation {
	x := RateExpectation{Complete: true, Counted: map[string]uint64{}}
	o.visit(roster, table, key, now, func(name string, s *source, v value, ok bool) {
		if !ok {
			x.Complete = false
			return
		}
		if v.session == 0 {
			return
		}
		est := ratetest.ReadReceived(v.rate, period, v.received, now)
		x.Counted[name] = est
		x.Sum += est
		// Upstream's signed remainder P - age is exact down to -2^31.
		outOfRange := (v.rate.Curr != 0 || v.rate.Prev != 0) && int64(v.rate.Age)-int64(period) > 1<<31
		x.Uncertain = x.Uncertain || v.session < s.session || outOfRange
	})
	return x
}

// visit replays the trace and calls fn for every roster source, in roster
// order: with ok false for a source that is not ready at now, else with
// its unexpired value of table and key, or a zero value (session 0) if it
// holds none.
func (o *Oracle) visit(roster []string, table string, key peermsg.Key, now time.Time,
	fn func(name string, s *source, v value, ok bool),
) {
	o.mu.Lock()
	trace := append([]record(nil), o.trace...)
	o.mu.Unlock()

	srcs := map[string]*source{}
	for _, name := range roster {
		srcs[name] = &source{defined: map[string]bool{}, values: map[string]map[peermsg.Key]value{}}
	}
	for _, r := range trace {
		if s := srcs[r.ev.Source]; s != nil {
			s.replay(r, o)
		}
	}
	for _, name := range roster {
		s := srcs[name]
		ready := s.up && s.synced && !s.fault && now.Sub(s.lastRx) < o.Health && !now.Before(s.rateUntil)
		for _, t := range o.Tables {
			ready = ready && s.defined[t]
		}
		if !ready {
			fn(name, s, value{}, false)
			continue
		}
		if v, ok := s.values[table][key]; ok && now.Before(v.deadline) {
			fn(name, s, v, true)
		} else {
			fn(name, s, value{}, true)
		}
	}
}

func (s *source) seen(at time.Time) {
	if at.After(s.lastRx) {
		s.lastRx = at
	}
}

// replay applies one trace record.
func (s *source) replay(r record, o *Oracle) {
	current := s.up && s.session == r.ev.Session
	if !r.observed.IsZero() {
		if current {
			s.seen(r.observed)
		}
		return
	}
	if r.refused {
		s.refuse(r.ev.Body, current, o.Tables)
		return
	}
	switch b := r.ev.Body.(type) {
	case peersession.SessionUp:
		s.session, s.up, s.synced, s.lastRx, s.upAt = r.ev.Session, true, false, b.At, b.At
		s.defined = map[string]bool{}
	case peersession.SessionDown:
		s.up, s.synced = false, false
	case peersession.TableDefined:
		s.seen(b.Received)
		s.defined[b.Definition.Name] = true
		if s.expiry == nil {
			s.expiry = map[string]peermsg.Millis{}
		}
		s.expiry[b.Definition.Name] = b.Definition.Expiry
		if s.synced {
			// Announced after "finished": reconciled at once.
			s.drop(o, b.Received, func(t string) bool { return t == b.Definition.Name })
		}
	case peersession.TableRejected:
		// A report, not a refusal: the session rejected the input table's
		// definition and ends; the source is faulty until a finished
		// resync, and the table undefined until defined again.
		s.seen(b.Received)
		s.defined[b.Table] = false
		s.fault = true
	case peersession.SyncFinished:
		s.seen(b.Received)
		s.synced = !b.Partial
		if !b.Partial {
			s.fault = false
			s.drop(o, b.Received, func(t string) bool { return s.defined[t] })
		}
	case peersession.EntryUpdated:
		s.seen(b.Update.Received)
		s.update(o, r.ev.Session, b)
	}
}

// refuse applies a refused event. A refusal faults only the current
// session; a SessionUp is refused for not starting a newer session and
// leaves the source as it was. A refused message still counts as read,
// and a refused definition of a configured table leaves it undefined.
func (s *source) refuse(body peersession.Event, current bool, tables []string) {
	if _, ok := body.(peersession.SessionUp); ok || !current {
		return
	}
	s.fault = true
	switch b := body.(type) {
	case peersession.TableDefined:
		s.seen(b.Received)
		if slices.Contains(tables, b.Definition.Name) {
			s.defined[b.Definition.Name] = false
		}
	case peersession.EntryUpdated:
		s.seen(b.Update.Received)
	}
}

// drop removes every value of an earlier session than the current one at
// at, from the tables in selects, recording unexpired ones as lost.
func (s *source) drop(o *Oracle, at time.Time, in func(table string) bool) {
	for table, vals := range s.values {
		if !in(table) {
			continue
		}
		for k, v := range vals {
			if v.session < s.session {
				delete(vals, k)
				if at.Before(v.deadline) {
					s.lose(o, table, v, at)
				}
			}
		}
	}
}

// lose records v of table as lost history detected at at.
func (s *source) lose(o *Oracle, table string, v value, at time.Time) {
	if !v.deadline.After(at.Add(o.Grace)) {
		return
	}
	if v.deadline.After(s.countUntil) {
		s.countUntil = v.deadline
	}
	// The first age from which the reference reading stays 0: one past
	// the last non-zero age up to two periods (it is 0 beyond).
	p := o.Periods[table]
	age := uint64(v.rate.Age)
	zeroAge := age
	for a := 2*uint64(p) + 1; a > age; a-- {
		if ratetest.Read(v.rate.Curr, v.rate.Prev, p, a-1) > 0 {
			zeroAge = a
			break
		}
	}
	//nolint:gosec // G115: zeroAge - age <= 2p+1 < 2^33 ms.
	zero := v.received.Add(time.Duration(zeroAge-age) * time.Millisecond)
	if v.deadline.Before(zero) {
		zero = v.deadline
	}
	// Events the source may have counted unreported, up to the moment
	// the current session came up, each weigh for at most two periods,
	// in an entry that lives at most the table expiry after it.
	life := v.deadline.Sub(v.received)
	if e := s.expiry[table]; e > 0 {
		life = time.Duration(e) * time.Millisecond
	}
	gapEnd := s.upAt.Add(min(life, time.Duration(2*int64(p)+1)*time.Millisecond))
	if gapEnd.After(zero) {
		zero = gapEnd
	}
	if c := s.upAt.Add(life); c.After(s.countUntil) {
		s.countUntil = c
	}
	if zero.After(at) && zero.After(s.rateUntil) {
		s.rateUntil = zero
	}
}

// live counts the values unexpired at at, across tables.
func (s *source) live(at time.Time) int {
	n := 0
	for _, vals := range s.values {
		for _, v := range vals {
			if at.Before(v.deadline) {
				n++
			}
		}
	}
	return n
}

func (s *source) update(o *Oracle, session uint64, b peersession.EntryUpdated) {
	u := b.Update
	life := b.Expiry
	if u.Timed && u.Remaining < life {
		life = u.Remaining
	}
	if s.values[b.Table] == nil {
		s.values[b.Table] = map[peermsg.Key]value{}
	}
	if life == 0 {
		delete(s.values[b.Table], u.Key)
		return
	}
	var count uint32
	var rate peermsg.FreqCounter
	for _, v := range u.Values {
		switch v.Type {
		case peermsg.DataHTTPReqCnt:
			count = v.Uint
		case peermsg.DataHTTPReqRate:
			rate = v.Freq
		default:
		}
	}
	v := value{count: count, rate: rate, received: u.Received, deadline: u.Received.Add(time.Duration(life) * time.Millisecond), session: session}
	old, had := s.values[b.Table][u.Key]
	had = had && u.Received.Before(old.deadline)
	if had {
		v.lowered = old.lowered || count < old.count
		if count < old.count && old.session < session {
			s.lose(o, b.Table, old, u.Received)
		}
	} else if o.Capacity > 0 && s.live(u.Received) >= o.Capacity {
		s.drop(o, u.Received, func(string) bool { return true })
	}
	s.values[b.Table][u.Key] = v
}
