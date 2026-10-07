package output

import (
	"cmp"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

// ErrInvalid is wrapped by every error Store methods return for invalid
// arguments.
var ErrInvalid = errors.New("output: invalid")

// Table configures one output table of a Store.
type Table struct {
	// Name is the HAProxy stick-table name. It must differ from every
	// input table's name.
	Name string
	// Kind fixes the slot layout.
	Kind Kind
	// Expiry is the HAProxy table's configured expire, which the source
	// announces and the aggregator announces back. A metadata table's
	// must be at most MaxLease (see NewStore).
	Expiry peermsg.Millis
}

// Entry is one published output entry.
type Entry struct {
	// Table is the output table name.
	Table string
	// Key is the entry key.
	Key peermsg.Key
	// Values is the gpt array.
	Values Values
	// Seq is the store change sequence at which the entry last changed.
	Seq uint64
}

// Lease is a Store's readiness: the publisher's statement that every value
// set up to sequence Seq is authoritative until Until.
type Lease struct {
	// Gen counts lease changes (SetLease and Revoke) from 1; 0 means no
	// lease was ever set, and nothing is to be sent.
	Gen uint64
	// Seq is the store change sequence the lease certifies: the values
	// current when it was set.
	Seq uint64
	// Valid is false after Revoke.
	Valid bool
	// Until is when authority ends, a reading of time.Now (with its
	// monotonic clock) when Valid.
	Until time.Time
	// WallUntil is Until on the store's wall clock, read once when the
	// lease was set: the deadline every marker of this lease carries.
	WallUntil time.Time
}

// StoreOptions configures NewStore.
type StoreOptions struct {
	// WallClock returns the wall-clock time lease deadlines are encoded
	// against; nil means time.Now. HAProxy compares a deadline with its
	// own wall clock, so this clock must agree with every source's within
	// the skew bound. Tests use it to simulate skew.
	WallClock func() time.Time
}

// Store is the current output state that every peers session teaches to
// its source: the latest values per aggregate table and key, each stamped
// with the store-wide change sequence at which it last changed, and the
// lease that makes them authoritative. Sessions read a full copy for a
// teach, then only the entries changed since the sequence they last sent,
// and write the lease marker into the metadata tables after the values it
// certifies. It is safe for concurrent use.
//
// Sessions re-send every entry periodically, so HAProxy keeps each one for
// as long as it stays in the store. The peers protocol has no deletion:
// Retire removes a key from the store, and from then on no session sends
// it, so HAProxy expires its copy after the aggregate table's expire,
// while a generation change makes that copy non-authoritative at once. A
// Store holds at most one entry per table and key, so its size, and every
// refresh, are bounded by the keys published and not yet retired; when to
// retire is the publisher's policy (package publish retires zero-rate
// keys in batches, and overflowing ones at once), and the bounds are
// phase 14's.
type Store struct {
	defs  []peermsg.Definition
	kinds map[string]Kind
	index map[string]int
	wall  func() time.Time

	// retired counts the keys Retire and RetireBatch removed.
	retired uint64

	mu      sync.Mutex
	seq     uint64
	lease   Lease
	entries []map[peermsg.Key]*stored // by table index
	changed chan struct{}
}

type stored struct {
	values Values
	seq    uint64
}

// NewStore returns an empty store for tables, which must have distinct,
// valid names, known kinds, and non-zero expiries; a metadata table's
// expiry must be at most MaxLease, which bounds how long HAProxy keeps a
// marker on its own clock.
func NewStore(tables []Table, opts StoreOptions) (*Store, error) {
	s := &Store{
		kinds:   map[string]Kind{},
		index:   map[string]int{},
		wall:    opts.WallClock,
		changed: make(chan struct{}),
	}
	if s.wall == nil {
		s.wall = time.Now
	}
	for _, t := range tables {
		if t.Kind != KindAggregate && t.Kind != KindMetadata {
			return nil, fmt.Errorf("%w: table %s: %v", ErrInvalid, t.Name, t.Kind)
		}
		if t.Kind == KindMetadata && t.Expiry.Duration() > MaxLease {
			return nil, fmt.Errorf("%w: metadata table %s expires after %v, more than the %v lease bound",
				ErrInvalid, t.Name, t.Expiry, MaxLease)
		}
		def := Definition(t.Name, t.Expiry)
		if err := def.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
		}
		if _, dup := s.index[t.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate table %s", ErrInvalid, t.Name)
		}
		s.index[t.Name] = len(s.defs)
		s.kinds[t.Name] = t.Kind
		s.defs = append(s.defs, def)
		s.entries = append(s.entries, map[peermsg.Key]*stored{})
	}
	return s, nil
}

// Tables returns the output table definitions in configuration order,
// which is the order sessions announce them in.
func (s *Store) Tables() []peermsg.Definition {
	out := make([]peermsg.Definition, len(s.defs))
	for i, d := range s.defs {
		d.Fields = slices.Clone(d.Fields)
		out[i] = d
	}
	return out
}

// Kind returns the kind of the output table named table.
func (s *Store) Kind(table string) (Kind, bool) {
	k, ok := s.kinds[table]
	return k, ok
}

// Set publishes values for key in the aggregate table named table.
// Setting the values an entry already has changes nothing and wakes no
// session. It fails, wrapping ErrInvalid, for an unknown or metadata
// table (whose marker only sessions write, from the lease) or values the
// schema does not allow (a wrong version, or a non-zero generation or
// reserved slot: each session writes its own generation).
//
// A new value is not authoritative until a later SetLease certifies it;
// until then HAProxy may already apply it under the current lease, which
// certified an older value no staler than the lease allows.
func (s *Store) Set(table string, key peermsg.Key, values Values) error {
	i, ok := s.index[table]
	if !ok {
		return fmt.Errorf("%w: unknown output table %q", ErrInvalid, table)
	}
	if s.kinds[table] != KindAggregate {
		return fmt.Errorf("%w: %v table %s is written from the lease, not set", ErrInvalid, s.kinds[table], table)
	}
	if err := checkAggregate(values); err != nil {
		return fmt.Errorf("%w: table %s key %v: %w", ErrInvalid, table, key, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.entries[i][key]; e != nil && e.values == values {
		return nil
	}
	s.seq++
	s.entries[i][key] = &stored{values: values, seq: s.seq}
	s.notify()
	return nil
}

// Retire removes key from the aggregate table named table, if present, so
// that no session teaches or refreshes it again. Every session then
// switches to a new generation and re-sends the remaining output before
// its next marker, so HAProxy's copy of the key keeps the old generation:
// no later marker certifies it, and HAProxy expires it after the table's
// expire. Until that re-send and marker reach the source, the copy stays
// as authoritative as the marker already there, which is the same bound a
// value change has. Each Retire that removes a key costs every session a
// full re-send (retirements seen at one wake-up share it), and every
// remaining key reads as local from its re-send until the new marker
// arrives, so a publisher should retire keys together (RetireBatch). It
// fails, wrapping ErrInvalid, for an unknown or metadata table.
func (s *Store) Retire(table string, key peermsg.Key) error {
	return s.RetireBatch(map[string][]peermsg.Key{table: {key}})
}

// RetireBatch retires every listed key of every listed aggregate table at
// once, as Retire does for one: the removals are one change, so each
// session rotates its generation and re-sends the remaining output once
// for the whole batch. It fails, wrapping ErrInvalid, before removing
// anything if any table is unknown or a metadata table.
func (s *Store) RetireBatch(keys map[string][]peermsg.Key) error {
	for table := range keys {
		if _, ok := s.index[table]; !ok || s.kinds[table] != KindAggregate {
			return fmt.Errorf("%w: %q is not an aggregate output table", ErrInvalid, table)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for table, ks := range keys {
		i := s.index[table]
		for _, key := range ks {
			if _, ok := s.entries[i][key]; ok {
				delete(s.entries[i], key)
				removed++
			}
		}
	}
	if removed == 0 {
		return nil
	}
	s.retired += uint64(removed)
	s.notify()
	return nil
}

// Retired returns how many keys Retire has removed so far; a session that
// sees it change switches generation before writing another marker.
func (s *Store) Retired() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retired
}

// SetLease declares every value set so far authoritative until until,
// which must be in the future and at most MaxLeaseLength away. The publisher
// computes it from the moment its data was current, not from when it
// calls, so that a slow publisher cannot certify stale data for a full
// lease. Sessions write the lease after the values it certifies.
//
// The deadline markers carry is fixed here, on the store's wall clock,
// and never recomputed: a lease written late (after a suspend, a stalled
// session, or a wall-clock step) keeps its original deadline.
func (s *Store) SetLease(until time.Time) error {
	now := time.Now()
	left := until.Sub(now)
	if left <= 0 || left > MaxLeaseLength {
		return fmt.Errorf("%w: lease ends in %v, want (0, %v]", ErrInvalid, left, MaxLeaseLength)
	}
	wall := s.wall().Add(left)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lease = Lease{Gen: s.lease.Gen + 1, Seq: s.seq, Valid: true, Until: until, WallUntil: wall}
	s.notify()
	return nil
}

// Revoke withdraws authority: sessions write a revocation marker, which
// makes HAProxy fall back to local protection at once.
func (s *Store) Revoke() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lease = Lease{Gen: s.lease.Gen + 1, Seq: s.seq}
	s.notify()
}

// Lease returns the current lease.
func (s *Store) Lease() Lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lease
}

// Marker returns the metadata entry with which a session of generation
// gen writes lease l now, and the remaining lifetime to send it with, or
// false when there is nothing to write: no lease was ever set, or a valid
// lease has already run out by either the monotonic or the wall clock (a
// marker sent then would certify nothing). A valid lease's marker carries
// the deadline fixed by SetLease and lives as long as the lease has left
// by both clocks; a revocation lives MaxLease, outlasting any marker it
// replaces.
func (s *Store) Marker(l Lease, gen uint32) (Values, peermsg.Millis, bool) {
	if l.Gen == 0 {
		return Values{}, 0, false
	}
	if !l.Valid {
		return RevocationValues(), LeaseWindowMillis, true
	}
	left := min(time.Until(l.Until), l.WallUntil.Sub(s.wall()), MaxLease)
	if left < time.Millisecond {
		return Values{}, 0, false
	}
	return MarkerValues(DeadlineValue(l.WallUntil), gen), peermsg.Millis(left / time.Millisecond), true
}

// NewGeneration returns a random non-zero session generation.
func NewGeneration() (uint32, error) {
	for {
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 0, fmt.Errorf("output: generation: %w", err)
		}
		if g := binary.BigEndian.Uint32(b[:]); g != 0 {
			return g, nil
		}
	}
}

// notify wakes every waiter. Callers hold s.mu.
func (s *Store) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// Get returns the values published for key in table.
func (s *Store) Get(table string, key peermsg.Key) (Values, bool) {
	i, ok := s.index[table]
	if !ok {
		return Values{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.entries[i][key]; e != nil {
		return e.values, true
	}
	return Values{}, false
}

// Changed returns a channel that is closed by the next change of a value
// or of the lease. Take it before reading with Since or Lease so that no
// change between the two is missed.
func (s *Store) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed
}

// Since returns every entry that changed after sequence seq, ordered by
// table (in Tables order) and then by change sequence, and the sequence
// the result is current to. Since(0) is a full copy. Passing the returned
// sequence to the next call yields exactly the later changes.
func (s *Store) Since(seq uint64) ([]Entry, uint64) {
	type change struct {
		Entry
		table int
	}
	s.mu.Lock()
	var changes []change
	for i, m := range s.entries {
		for k, e := range m {
			if e.seq > seq {
				changes = append(changes, change{Entry{Table: s.defs[i].Name, Key: k, Values: e.values, Seq: e.seq}, i})
			}
		}
	}
	cur := s.seq
	s.mu.Unlock()
	slices.SortFunc(changes, func(a, b change) int {
		return cmp.Or(cmp.Compare(a.table, b.table), cmp.Compare(a.Seq, b.Seq))
	})
	out := make([]Entry, len(changes))
	for i, c := range changes {
		out[i] = c.Entry
	}
	return out, cur
}
