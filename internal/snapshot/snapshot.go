// Package snapshot keeps the latest accepted state of every configured
// source, separately: for each source, logical input table, and canonical
// wire key, the source's own last value and the local deadline at which
// it expires; and for each source its session, synchronization, and
// health, from which roster readiness follows. It does no aggregate
// arithmetic: values from different sources are never combined here.
//
// # Identity
//
// A contributor is a configured source name. Session numbers (from
// package sources) identify the source's successive sessions, the
// replaceable session generation: the store records the current one per
// source and which session delivered each entry, but a new session never
// becomes another contributor. Table IDs are session-local and play no
// part; entries are keyed by the configured input table's name and the
// 16-byte wire key, compared bytewise (see peermsg.Key).
//
// # Merge rules
//
// Store.Apply takes every event of every session synchronously (see
// sources.Options.Apply), so for one source it sees each session's events
// in wire order and all of a session's events before the next session's.
//
//   - An accepted update replaces only its own source's previous value
//     of that table and key. Absolute values are never added together.
//   - Within a session, wire order is recency: HAProxy encodes an entry's
//     current values when it sends them, so a teach (resync replay) and
//     live updates interleaved on one connection are each at least as new
//     as everything before them. A later session's update replaces an
//     earlier session's value; an event of any session other than the
//     source's current one is refused (ErrSession). Update IDs are never
//     compared: they wrap and restart with every teach. As a defensive
//     check, an update read before the stored value of the same source
//     and key is ignored, keeping the newer value.
//   - An ordinary update lives for the source table's announced expiry
//     from its reception; a timed update for its remaining lifetime,
//     capped at that expiry (peermsg.Update.Lifetime). The local deadline
//     is reception time plus that lifetime on the monotonic clock. An
//     update whose lifetime has already ended removes the source's value
//     instead of storing it, so replay never resurrects expired state or
//     extends a lifetime. An entry's lifetime is unrelated to its rate
//     counter's window age (Entry.Rate.Age, relative to Entry.Received).
//   - An update that lowers the stored count still replaces it, and is
//     recorded on the entry (Entry.Decreases, Entry.LastDecrease): a
//     reset, a recreation at the source, and a 32-bit wrap look alike
//     on the wire, so no delta is ever inferred from absolute values.
//   - Expired entries are invisible to every read at once and removed by
//     Expire (or any roster report); no new input is needed.
//   - Entries of a disconnected source are retained until they expire.
//     Keys missing from a later session's snapshot are not reconciled
//     here; each entry records the session that delivered it.
//
// # Source states
//
// Each source is in one State, evaluated at the store clock's now, in
// this order of precedence:
//
//   - Degraded, while a fault is recorded: the store refused an event of
//     the source's session (a schema mismatch, which also marks the
//     logical table rejected; an output or unknown table; values that do
//     not match the table; the entry capacity), or the session reported a
//     rejected input table. Every refusal ends the session unacknowledged.
//     The next finished resync clears the fault. Because the faulting
//     session ends, that resync normally comes from a later session; the
//     store does not check this.
//   - Disconnected, without a current session.
//   - Degraded, while the source is unhealthy: its session's last message
//     was read HealthTimeout or more ago. Message read times come from
//     the events themselves and from Observe (heartbeats produce no
//     events), so a quiet source that heartbeats stays healthy, and a
//     backlog ages its last read like silence does.
//   - Syncing, until the session's resync request is answered "finished";
//     a "partial" reply (the source is not itself synchronized) keeps it
//     syncing while the session asks again.
//   - Degraded, after a finished resync, while a configured input table
//     was not announced in the session: HAProxy announces every table it
//     shares when it teaches, even an empty one, so the source does not
//     share it.
//   - Ready otherwise.
//
// A finished resync commits completeness only after every event before it
// in the session was applied, because Apply runs in wire order. So an
// empty but synchronized, healthy source is Ready with no entries, while
// a source that never connected is Disconnected. The roster is ready only
// when every configured source is Ready; one source completing its sync
// changes nothing for the others.
package snapshot

import (
	"errors"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

// Errors returned by Apply and New. Apply's errors also wrap ErrRefused.
var (
	// ErrInvalid wraps every Options validation error.
	ErrInvalid = errors.New("snapshot: invalid options")
	// ErrRefused is wrapped by every event Apply refuses.
	ErrRefused = errors.New("snapshot: event refused")
	// ErrUnknownSource refuses an event of a source not in the roster.
	ErrUnknownSource = errors.New("snapshot: unknown source")
	// ErrSession refuses an event of a session that is not the source's
	// current one, or a SessionUp that does not start a newer session.
	ErrSession = errors.New("snapshot: not the current session")
	// ErrNotInput refuses a definition or update of a table that is not
	// a configured input table: an output or metadata table replayed by
	// the source, or any other table. Such state never contributes.
	ErrNotInput = errors.New("snapshot: not an input table")
	// ErrSchema refuses a definition that does not match the logical
	// table's configured schema, or an update whose values or expiry do
	// not match the table's accepted definition.
	ErrSchema = errors.New("snapshot: schema mismatch")
	// ErrUndefined refuses an update of a table the session has not
	// defined.
	ErrUndefined = errors.New("snapshot: table not defined in this session")
	// ErrCapacity refuses a new key beyond Options.MaxSourceEntries.
	ErrCapacity = errors.New("snapshot: source entry capacity reached")
)

// State is a source's state.
type State uint8

// Source states; see the package documentation for their precedence.
const (
	// Disconnected: no current session.
	Disconnected State = iota + 1
	// Syncing: a session is up and has not completed a finished resync.
	Syncing
	// Ready: synchronized, healthy, and every input table announced.
	Ready
	// Degraded: a recorded fault, an unhealthy session, or a missing
	// input table.
	Degraded
)

// String names the state.
func (s State) String() string {
	switch s {
	case Disconnected:
		return "disconnected"
	case Syncing:
		return "syncing"
	case Ready:
		return "ready"
	case Degraded:
		return "degraded"
	default:
		return "unknown"
	}
}

// Table is one configured input table.
type Table struct {
	// Name is the HAProxy table name, the logical table's identity.
	Name string
	// Period is the http_req_rate period every contributor must announce.
	Period peermsg.Millis
}

// Options configures New.
type Options struct {
	// Sources is the roster: the configured source names, all required.
	Sources []string
	// Tables are the input tables.
	Tables []Table
	// HealthTimeout is the health bound H: a source is healthy only while
	// its last message read is younger than H. It must be positive;
	// config enforces the operational range.
	HealthTimeout time.Duration
	// MaxSourceEntries bounds the entries stored per source, across its
	// tables. It must be positive.
	MaxSourceEntries int
	// Now is the monotonic clock deadlines and health are judged by. Nil
	// means time.Now. Event times come from the sessions, which read the
	// same clock; a fake clock must therefore also stamp the events.
	Now func() time.Time
}

// OptionsFrom returns the options a validated configuration implies.
func OptionsFrom(cfg config.Config) Options {
	o := Options{HealthTimeout: cfg.HealthTimeout, MaxSourceEntries: cfg.MaxSourceEntries}
	for _, s := range cfg.Sources {
		o.Sources = append(o.Sources, s.Name)
	}
	for _, t := range cfg.Tables {
		//nolint:gosec // G115: config.Validate bounds periods to 1ms..MaxInt32 ms.
		o.Tables = append(o.Tables, Table{Name: t.Name, Period: peermsg.Millis(t.Period.Milliseconds())})
	}
	return o
}

// Entry is one source's latest accepted value of one key.
type Entry struct {
	// Session is the source session that delivered the value.
	Session uint64
	// UpdateID is the update's ID in that session, for diagnostics only.
	UpdateID peermsg.UpdateID
	// Timed records whether the update carried a remaining lifetime.
	Timed bool
	// Received is when the session read the update (monotonic).
	Received time.Time
	// Deadline is when the entry expires locally: Received plus the
	// lifetime the update granted. It is the entry's lifetime, not the
	// rate counter's window.
	Deadline time.Time
	// Count is http_req_cnt as the source reported it.
	Count uint32
	// Rate is the http_req_rate counter as the source reported it; its
	// Age is relative to Received.
	Rate peermsg.FreqCounter
	// Period is the rate counter's period.
	Period peermsg.Millis
	// Decreases counts the accepted updates of this entry, in any
	// session, whose Count was lower than the value they replaced. The
	// wire cannot tell the causes apart: a reset (the runtime API's
	// "set table" or "clear table"), the entry's expiry or eviction and
	// recreation at the source, or HAProxy's unsigned 32-bit counter
	// wrapping past math.MaxUint32 to 0. The store keeps the lower value
	// as the source's current one and never infers a delta; continuity
	// of the entry's count is unknown from the first decrease until the
	// entry expires. The count survives replacement and ends with the
	// entry.
	Decreases uint64
	// LastDecrease describes the most recent decrease, if Decreases > 0.
	LastDecrease Decrease
}

// Decrease is one update that lowered an entry's Count.
type Decrease struct {
	// From and To are the replaced and the new Count.
	From, To uint32
	// FromSession and ToSession are the sessions that delivered them;
	// they differ when the decrease arrived in a later session.
	FromSession, ToSession uint64
	// At is when the lowering update was read (monotonic).
	At time.Time
}

// Contribution is one source's entry for a key.
type Contribution struct {
	Source string
	Entry  Entry
}

// TableReport describes one logical table of one source.
type TableReport struct {
	// Name is the table name.
	Name string
	// Defined reports whether the current session announced it with
	// the configured schema.
	Defined bool
	// Expiry is the source's announced table expiry, once defined.
	Expiry peermsg.Millis
	// Entries counts the source's unexpired entries in the table.
	Entries int
	// Rejected is why the table's last definition was refused, until a
	// later definition is accepted; nil otherwise.
	Rejected error
}

// SourceReport describes one source at a moment.
type SourceReport struct {
	// Name is the source name.
	Name string
	// State is the source's state at the report's time.
	State State
	// Reason explains any state but Ready.
	Reason string
	// Session is the current or last session number; 0 if none yet.
	Session uint64
	// Up reports whether that session is current.
	Up bool
	// Synced reports a finished resync in the current session.
	Synced bool
	// PartialReplies counts "partial" resync replies in the session.
	PartialReplies int
	// Healthy reports whether the last message read is younger than
	// the health timeout (false without a session).
	Healthy bool
	// LastRx is the last message read known for the current session.
	LastRx time.Time
	// Fault is the recorded fault, if any (see the package
	// documentation).
	Fault error
	// Entries counts the source's unexpired entries.
	Entries int
	// Tables reports each input table, in configuration order.
	Tables []TableReport
	// Accepted, Refused, Stale, and Expired count updates stored,
	// events refused, updates ignored as older than the stored value,
	// and entries removed by expiry (including updates that arrived
	// expired). Decreases counts accepted updates that lowered a stored
	// Count (see Entry.Decreases).
	Accepted, Refused, Stale, Expired, Decreases uint64
}

// Roster reports every source at one moment.
type Roster struct {
	// At is the store clock's time of the report.
	At time.Time
	// Ready reports that every configured source is Ready.
	Ready bool
	// Sources reports each source, in roster order.
	Sources []SourceReport
}

// NotReady returns the reports of the sources that are not Ready.
func (r Roster) NotReady() []SourceReport {
	var out []SourceReport
	for _, s := range r.Sources {
		if s.State != Ready {
			out = append(out, s)
		}
	}
	return out
}
