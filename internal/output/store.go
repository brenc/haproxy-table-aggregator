package output

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"

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
	// announces and the aggregator announces back.
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

// Store is the current output state that every peers session teaches to
// its source: the latest values per table and key, each stamped with the
// store-wide change sequence at which it last changed. Sessions read a
// full copy for a teach, then only the entries changed since the sequence
// they last sent. It is safe for concurrent use.
//
// Entries are never deleted: the peers protocol has no deletion, and an
// HAProxy entry expires on its own when updates stop. A Store holds at
// most one entry per table and key, so its size is bounded by the keys
// published; phase 14 adds a configured bound.
type Store struct {
	defs  []peermsg.Definition
	kinds map[string]Kind
	index map[string]int

	mu      sync.Mutex
	seq     uint64
	entries []map[peermsg.Key]*stored // by table index
	changed chan struct{}
}

type stored struct {
	values Values
	seq    uint64
}

// NewStore returns an empty store for tables, which must have distinct,
// valid names, known kinds, and non-zero expiries.
func NewStore(tables []Table) (*Store, error) {
	s := &Store{
		kinds:   map[string]Kind{},
		index:   map[string]int{},
		changed: make(chan struct{}),
	}
	for _, t := range tables {
		if t.Kind != KindAggregate && t.Kind != KindMetadata {
			return nil, fmt.Errorf("%w: table %s: %v", ErrInvalid, t.Name, t.Kind)
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

// Set publishes values for key in table. Setting the values an entry
// already has changes nothing and wakes no session. It fails, wrapping
// ErrInvalid, for an unknown table or values the table's kind does not
// allow (a wrong schema version or a non-zero reserved slot).
func (s *Store) Set(table string, key peermsg.Key, values Values) error {
	i, ok := s.index[table]
	if !ok {
		return fmt.Errorf("%w: unknown output table %q", ErrInvalid, table)
	}
	if err := s.kinds[table].check(values); err != nil {
		return fmt.Errorf("%w: table %s key %v: %w", ErrInvalid, table, key, err)
	}
	if s.kinds[table] == KindMetadata && key != MetadataKey {
		return fmt.Errorf("%w: metadata table %s has only key %v, not %v", ErrInvalid, table, MetadataKey, key)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.entries[i][key]; e != nil && e.values == values {
		return nil
	}
	s.seq++
	s.entries[i][key] = &stored{values: values, seq: s.seq}
	close(s.changed)
	s.changed = make(chan struct{})
	return nil
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

// Changed returns a channel that is closed by the next change. Take it
// before reading with Since so that no change between the two is missed.
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
