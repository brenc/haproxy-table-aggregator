package snapshot

import (
	"fmt"
	"sync"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// Store holds the per-source snapshots and states. It is safe for
// concurrent use.
type Store struct {
	now      func() time.Time
	health   time.Duration
	capacity int
	order    []string
	tables   []Table

	mu      sync.Mutex
	sources map[string]*source
}

// source is one roster member's state.
type source struct {
	name     string
	session  uint64
	up       bool
	synced   bool
	partials int
	lastRx   time.Time
	fault    error
	tables   map[string]*table
	entries  int

	accepted, refused, stale, expired, decreases uint64
}

// table is one logical input table of one source.
type table struct {
	spec     Table
	def      peermsg.Definition
	defined  bool
	rejected error
	entries  map[peermsg.Key]Entry
}

// New returns an empty store for the roster and tables in opts: every
// source Disconnected, nothing stored.
func New(opts Options) (*Store, error) {
	if len(opts.Sources) == 0 || len(opts.Tables) == 0 {
		return nil, fmt.Errorf("%w: need at least one source and one table", ErrInvalid)
	}
	if opts.HealthTimeout <= 0 || opts.MaxSourceEntries <= 0 {
		return nil, fmt.Errorf("%w: health timeout %v and entry capacity %d must be positive", ErrInvalid,
			opts.HealthTimeout, opts.MaxSourceEntries)
	}
	s := &Store{
		now: opts.Now, health: opts.HealthTimeout, capacity: opts.MaxSourceEntries,
		order: append([]string(nil), opts.Sources...), tables: append([]Table(nil), opts.Tables...),
		sources: map[string]*source{},
	}
	if s.now == nil {
		s.now = time.Now
	}
	seen := map[string]bool{}
	for _, t := range opts.Tables {
		if seen[t.Name] || t.Period == 0 {
			return nil, fmt.Errorf("%w: table %q duplicate or without a period", ErrInvalid, t.Name)
		}
		seen[t.Name] = true
	}
	for _, name := range opts.Sources {
		if s.sources[name] != nil || name == "" {
			return nil, fmt.Errorf("%w: source %q duplicate or empty", ErrInvalid, name)
		}
		src := &source{name: name, tables: map[string]*table{}}
		for _, t := range opts.Tables {
			src.tables[t.Name] = &table{spec: t, entries: map[peermsg.Key]Entry{}}
		}
		s.sources[name] = src
	}
	return s, nil
}

// Apply applies one session event. It has the signature of
// sources.Options.Apply, which calls it in wire order per source. A
// refused event returns an error wrapping ErrRefused and the specific
// cause, changes no stored value, and (for an event of the source's
// current session) records a fault that degrades the source. Lifecycle
// events are never refused unless out of order.
func (s *Store) Apply(ev sources.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.sources[ev.Source]
	if src == nil {
		return refuse(ErrUnknownSource, "source %q", ev.Source)
	}
	if up, ok := ev.Body.(peersession.SessionUp); ok {
		if ev.Session <= src.session {
			return refuse(ErrSession, "source %s: session %d up after session %d", src.name, ev.Session, src.session)
		}
		src.session, src.up, src.synced, src.partials = ev.Session, true, false, 0
		src.lastRx = up.At
		for _, t := range src.tables {
			t.defined = false
		}
		return nil
	}
	if !src.up || ev.Session != src.session {
		src.refused++
		return refuse(ErrSession, "source %s: %T of session %d, current session %d (up %v)",
			src.name, ev.Body, ev.Session, src.session, src.up)
	}
	err := s.apply(src, ev.Body)
	if err != nil {
		src.refused++
		src.fault = err
	}
	return err
}

func (s *Store) apply(src *source, body peersession.Event) error {
	switch b := body.(type) {
	case peersession.SessionDown:
		src.up, src.synced = false, false
		return nil
	case peersession.TableRejected:
		// Accepted as a report, so that it is queued too; the session
		// ends right after it.
		src.touch(b.Received)
		if t := src.tables[b.Table]; t != nil {
			t.defined, t.rejected = false, b.Err
		}
		src.fault = fmt.Errorf("%w: source %s: input table %s rejected by the session: %w",
			ErrSchema, src.name, b.Table, b.Err)
		return nil
	case peersession.TableDefined:
		src.touch(b.Received)
		t := src.tables[b.Definition.Name]
		if t == nil {
			return refuse(ErrNotInput, "source %s: definition of %s", src.name, b.Definition.Name)
		}
		if err := checkDefinition(b.Definition, t.spec); err != nil {
			t.defined, t.rejected = false, err
			return refuse(ErrSchema, "source %s: %v", src.name, err)
		}
		t.def, t.defined, t.rejected = b.Definition, true, nil
		return nil
	case peersession.EntryUpdated:
		src.touch(b.Update.Received)
		return s.update(src, b)
	case peersession.SyncFinished:
		src.touch(b.Received)
		if b.Partial {
			src.partials++
			src.synced = false
			return nil
		}
		// Every refusal ends its session, so a fault recorded now
		// belongs to an earlier session: this complete sync supersedes
		// it.
		src.synced, src.fault = true, nil
		return nil
	default:
		return refuse(ErrSchema, "source %s: unknown event %T", src.name, body)
	}
}

func (s *Store) update(src *source, ev peersession.EntryUpdated) error {
	t := src.tables[ev.Table]
	if t == nil {
		return refuse(ErrNotInput, "source %s: update of %s", src.name, ev.Table)
	}
	if !t.defined {
		return refuse(ErrUndefined, "source %s: update of %s", src.name, ev.Table)
	}
	u := ev.Update
	if ev.Expiry != t.def.Expiry {
		return refuse(ErrSchema, "source %s table %s: update with expiry %v, definition %v",
			src.name, ev.Table, ev.Expiry, t.def.Expiry)
	}
	e, err := entryFrom(u, t.def)
	if err != nil {
		return refuse(ErrSchema, "source %s table %s key %v: %v", src.name, ev.Table, u.Key, err)
	}
	e.Session = src.session
	now := s.now()
	old, had := t.entries[u.Key]
	if had && !old.Deadline.After(now) {
		// Expired but not yet purged: it is already invisible, and its
		// value and history must not carry into a new entry.
		delete(t.entries, u.Key)
		src.entries--
		src.expired++
		had = false
	}
	if had && u.Received.Before(old.Received) {
		src.stale++
		return nil
	}
	lowered := had && e.Count < old.Count
	if had {
		e.Decreases, e.LastDecrease = old.Decreases, old.LastDecrease
		if lowered {
			e.Decreases++
			e.LastDecrease = Decrease{
				From: old.Count, To: e.Count, FromSession: old.Session, ToSession: e.Session, At: e.Received,
			}
		}
	}
	if !e.Deadline.After(now) {
		if had {
			delete(t.entries, u.Key)
			src.entries--
		}
		src.expired++
		return nil
	}
	if !had && src.entries >= s.capacity {
		s.expireSource(src, now)
		if src.entries >= s.capacity {
			return refuse(ErrCapacity, "source %s: %d entries", src.name, src.entries)
		}
	}
	if !had {
		src.entries++
	}
	t.entries[u.Key] = e
	src.accepted++
	if lowered {
		src.decreases++
	}
	return nil
}

// checkDefinition requires the configured schema, which every contributor
// to a logical table must share: the IPv6 key of 16 bytes and exactly
// http_req_cnt and http_req_rate with the configured period, with no
// array fields. Expiry may differ between sources; it is per contributor.
func checkDefinition(def peermsg.Definition, spec Table) error {
	if err := def.Validate(); err != nil {
		return err
	}
	want := []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: spec.Period}}
	if def.KeyType != peermsg.KeyTypeIPv6 || def.KeyType.KeyLen() != peermsg.IPv6KeyLen ||
		len(def.Fields) != len(want) || def.Fields[0] != want[0] || def.Fields[1] != want[1] {
		return fmt.Errorf("table %s stores %v with key type %d, want %v with IPv6 keys",
			def.Name, def.Fields, def.KeyType, want)
	}
	return nil
}

// entryFrom validates u's values against def and converts them.
func entryFrom(u peermsg.Update, def peermsg.Definition) (Entry, error) {
	if len(u.Values) != len(def.Fields) {
		return Entry{}, fmt.Errorf("%d values for %d fields", len(u.Values), len(def.Fields))
	}
	e := Entry{UpdateID: u.ID, Timed: u.Timed, Received: u.Received}
	for i, f := range def.Fields {
		v := u.Values[i]
		if v.Type != f.Type || v.Array != nil {
			return Entry{}, fmt.Errorf("value %d is %v, field is %v", i, v.Type, f)
		}
		switch f.Type {
		case peermsg.DataHTTPReqCnt:
			if v.Freq != (peermsg.FreqCounter{}) {
				return Entry{}, fmt.Errorf("%v carries a counter", f)
			}
			e.Count = v.Uint
		case peermsg.DataHTTPReqRate:
			if v.Uint != 0 {
				return Entry{}, fmt.Errorf("%v carries an integer", f)
			}
			e.Rate, e.Period = v.Freq, f.Period
		default:
			return Entry{}, fmt.Errorf("unsupported field %v", f)
		}
	}
	e.Deadline = u.Received.Add(u.Lifetime(def.Expiry).Duration())
	return e, nil
}

func (src *source) touch(at time.Time) {
	if at.After(src.lastRx) {
		src.lastRx = at
	}
}

// Observe records that the source's session read a message at lastRx,
// typically peersession.Stats.LastRx from sources.Manager.Status: the
// only evidence of heartbeats, which produce no events. It is ignored
// unless session is the source's current session.
func (s *Store) Observe(sourceName string, session uint64, lastRx time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if src := s.sources[sourceName]; src != nil && src.up && src.session == session {
		src.touch(lastRx)
	}
}

// ObserveStatus calls Observe for every source status that is up.
func (s *Store) ObserveStatus(statuses []sources.SourceStatus) {
	for _, st := range statuses {
		if st.Up {
			s.Observe(st.Name, st.Session, st.Stats.LastRx)
		}
	}
}

// Expire removes every entry whose deadline has passed and returns how
// many it removed. Reads already ignore such entries; Expire releases
// them and their capacity.
func (s *Store) Expire() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now, n := s.now(), 0
	for _, src := range s.sources {
		n += s.expireSource(src, now)
	}
	return n
}

func (s *Store) expireSource(src *source, now time.Time) int {
	n := 0
	for _, t := range src.tables {
		for k, e := range t.entries {
			if !e.Deadline.After(now) {
				delete(t.entries, k)
				n++
			}
		}
	}
	src.entries -= n
	src.expired += uint64(n)
	return n
}

// NextDeadline returns the earliest entry deadline, if any entry is
// stored, so that a caller can schedule Expire.
func (s *Store) NextDeadline() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var next time.Time
	found := false
	for _, src := range s.sources {
		for _, t := range src.tables {
			for _, e := range t.entries {
				if !found || e.Deadline.Before(next) {
					next, found = e.Deadline, true
				}
			}
		}
	}
	return next, found
}

// Lookup returns the source's unexpired entry for the table and key.
func (s *Store) Lookup(sourceName, tableName string, key peermsg.Key) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.sources[sourceName]
	if src == nil || src.tables[tableName] == nil {
		return Entry{}, false
	}
	e, ok := src.tables[tableName].entries[key]
	if !ok || !e.Deadline.After(s.now()) {
		return Entry{}, false
	}
	return e, true
}

// Contributions returns every source's unexpired entry for the table and
// key, in roster order: one per source at most, never combined. It does
// not consider source state; use KeyView to read entries with the roster
// state that certifies them.
func (s *Store) Contributions(tableName string, key peermsg.Key) []Contribution {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var out []Contribution
	for _, name := range s.order {
		t := s.sources[name].tables[tableName]
		if t == nil {
			continue
		}
		if e, ok := t.entries[key]; ok && e.Deadline.After(now) {
			out = append(out, Contribution{Source: name, Entry: e})
		}
	}
	return out
}

// Source reports one source now, after removing its expired entries.
func (s *Store) Source(name string) (SourceReport, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.sources[name]
	if src == nil {
		return SourceReport{}, false
	}
	now := s.now()
	s.expireSource(src, now)
	return s.report(src, now), true
}

// Roster reports every source now, after removing expired entries. Its
// Ready is the roster readiness that aggregate authority requires: every
// configured source Ready.
func (s *Store) Roster() Roster {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.roster(s.now())
}

// KeyView reports the roster and every source's unexpired entry for the
// table and key, in roster order, at one moment under one lock, so that
// a reader can tell which contributions the roster's state certifies.
// Contributions alone, like Lookup, does not consider source state. ok
// is false if tableName is not a configured input table.
func (s *Store) KeyView(tableName string, key peermsg.Key) (r Roster, contributions []Contribution, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isTable(tableName) {
		return Roster{}, nil, false
	}
	now := s.now()
	r = s.roster(now)
	for _, name := range s.order {
		if e, found := s.sources[name].tables[tableName].entries[key]; found && e.Deadline.After(now) {
			contributions = append(contributions, Contribution{Source: name, Entry: e})
		}
	}
	return r, contributions, true
}

// Keys returns every key for which any source, whatever its state, holds
// an unexpired entry in the table, each once, in no particular order. A
// publisher evaluates these keys; whether a source's entry counts is
// decided when the key is read (KeyView).
func (s *Store) Keys(tableName string) []peermsg.Key {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isTable(tableName) {
		return nil
	}
	now := s.now()
	seen := map[peermsg.Key]bool{}
	var out []peermsg.Key
	for _, name := range s.order {
		for k, e := range s.sources[name].tables[tableName].entries {
			if !seen[k] && e.Deadline.After(now) {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	return out
}

// Table returns the configured input table named name. The configuration
// is fixed at New, so no lock is needed.
func (s *Store) Table(name string) (Table, bool) {
	for _, t := range s.tables {
		if t.Name == name {
			return t, true
		}
	}
	return Table{}, false
}

func (s *Store) isTable(name string) bool {
	_, ok := s.Table(name)
	return ok
}

// roster reports every source at now, after removing expired entries.
// Callers hold s.mu.
func (s *Store) roster(now time.Time) Roster {
	r := Roster{At: now, Ready: true}
	for _, name := range s.order {
		src := s.sources[name]
		s.expireSource(src, now)
		rep := s.report(src, now)
		if rep.State != Ready {
			r.Ready = false
		}
		r.Sources = append(r.Sources, rep)
	}
	return r
}

// report describes src at now. Callers hold s.mu.
func (s *Store) report(src *source, now time.Time) SourceReport {
	rep := SourceReport{
		Name: src.name, Session: src.session, Up: src.up, Synced: src.synced, PartialReplies: src.partials,
		Fault: src.fault, Entries: src.entries,
		Accepted: src.accepted, Refused: src.refused, Stale: src.stale, Expired: src.expired,
		Decreases: src.decreases,
	}
	if src.up {
		rep.LastRx = src.lastRx
		rep.Healthy = now.Sub(src.lastRx) < s.health
	}
	var missing []string
	for _, spec := range s.tables {
		t := src.tables[spec.Name]
		tr := TableReport{Name: spec.Name, Defined: src.up && t.defined, Entries: len(t.entries), Rejected: t.rejected}
		if tr.Defined {
			tr.Expiry = t.def.Expiry
		} else {
			missing = append(missing, spec.Name)
		}
		rep.Tables = append(rep.Tables, tr)
	}
	switch {
	case src.fault != nil:
		rep.State, rep.Reason = Degraded, "fault: "+src.fault.Error()
	case !src.up:
		rep.State, rep.Reason = Disconnected, "no session"
	case !rep.Healthy:
		rep.State = Degraded
		rep.Reason = fmt.Sprintf("unhealthy: last message read %v ago, bound %v",
			now.Sub(src.lastRx).Round(time.Millisecond), s.health)
	case !src.synced && src.partials > 0:
		rep.State = Syncing
		rep.Reason = fmt.Sprintf("source answered %d resync request(s) partial; retrying", src.partials)
	case !src.synced:
		rep.State, rep.Reason = Syncing, "awaiting the resync reply"
	case len(missing) > 0:
		rep.State, rep.Reason = Degraded, fmt.Sprintf("synchronized without announcing input tables %v", missing)
	default:
		rep.State = Ready
	}
	return rep
}

func refuse(cause error, format string, args ...any) error {
	return fmt.Errorf("%w: %w: %s", ErrRefused, cause, fmt.Sprintf(format, args...))
}
