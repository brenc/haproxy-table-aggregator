// Package aggregatetest provides an independent trace oracle for the
// diagnostic totals of package aggregate, for tests only.
//
// The Oracle records the events a snapshot store accepted or refused and
// the message reads observed for heartbeats, and recomputes from that
// trace, using the rules the plan states rather than any production code,
// which value each source holds for a key, which sources are ready, and
// what the total therefore must be. It deliberately calls neither the
// snapshot store's merge nor aggregate.Count: a test compares their
// result with Want.
package aggregatetest

import (
	"slices"
	"sync"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
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
// lowered it while its entry was live, or if an earlier session than the
// source's current one delivered it.
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
}

// Want returns the expectation for table and key at now, for the roster
// of configured source names.
func (o *Oracle) Want(roster []string, table string, key peermsg.Key, now time.Time) Expectation {
	o.mu.Lock()
	trace := append([]record(nil), o.trace...)
	o.mu.Unlock()

	srcs := map[string]*source{}
	for _, name := range roster {
		srcs[name] = &source{defined: map[string]bool{}, values: map[string]map[peermsg.Key]value{}}
	}
	for _, r := range trace {
		if s := srcs[r.ev.Source]; s != nil {
			s.replay(r, o.Tables)
		}
	}
	x := Expectation{Complete: true, Counted: map[string]uint32{}}
	for _, name := range roster {
		s := srcs[name]
		ready := s.up && s.synced && !s.fault && now.Sub(s.lastRx) < o.Health
		for _, t := range o.Tables {
			ready = ready && s.defined[t]
		}
		if !ready {
			x.Complete = false
			continue
		}
		if v, ok := s.values[table][key]; ok && now.Before(v.deadline) {
			x.Counted[name] = v.count
			x.Sum += uint64(v.count)
			x.Uncertain = x.Uncertain || v.lowered || v.session < s.session
		}
	}
	return x
}

func (s *source) seen(at time.Time) {
	if at.After(s.lastRx) {
		s.lastRx = at
	}
}

// replay applies one trace record. tables are the configured input
// table names.
func (s *source) replay(r record, tables []string) {
	current := s.up && s.session == r.ev.Session
	if !r.observed.IsZero() {
		if current {
			s.seen(r.observed)
		}
		return
	}
	if r.refused {
		s.refuse(r.ev.Body, current, tables)
		return
	}
	switch b := r.ev.Body.(type) {
	case peersession.SessionUp:
		s.session, s.up, s.synced, s.lastRx = r.ev.Session, true, false, b.At
		s.defined = map[string]bool{}
	case peersession.SessionDown:
		s.up, s.synced = false, false
	case peersession.TableDefined:
		s.seen(b.Received)
		s.defined[b.Definition.Name] = true
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
		}
	case peersession.EntryUpdated:
		s.seen(b.Update.Received)
		s.update(r.ev.Session, b)
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

func (s *source) update(session uint64, b peersession.EntryUpdated) {
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
	for _, v := range u.Values {
		if v.Type == peermsg.DataHTTPReqCnt {
			count = v.Uint
		}
	}
	v := value{count: count, deadline: u.Received.Add(time.Duration(life) * time.Millisecond), session: session}
	if old, ok := s.values[b.Table][u.Key]; ok && u.Received.Before(old.deadline) {
		v.lowered = old.lowered || count < old.count
	}
	s.values[b.Table][u.Key] = v
}
