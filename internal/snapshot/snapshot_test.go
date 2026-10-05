package snapshot_test

import (
	"errors"
	"math"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

const (
	inTable  = "t_in"
	period   = peermsg.Millis(10000)
	expiry   = peermsg.Millis(30000)
	healthTO = 5 * time.Second
)

var inDef = peermsg.Definition{
	Name: inTable, KeyType: peermsg.KeyTypeIPv6, Expiry: expiry,
	Fields: []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: period}},
}

// fakeClock is a manual monotonic clock.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// harness drives a store with events stamped by its fake clock.
type harness struct {
	t     *testing.T
	clock *fakeClock
	s     *snapshot.Store
}

func newHarness(t *testing.T, capacity int, srcs ...string) *harness {
	t.Helper()
	c := &fakeClock{t: time.Now()}
	s, err := snapshot.New(snapshot.Options{
		Sources: srcs, Tables: []snapshot.Table{{Name: inTable, Period: period}},
		HealthTimeout: healthTO, MaxSourceEntries: capacity, Now: c.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, clock: c, s: s}
}

func (h *harness) apply(src string, session uint64, body peersession.Event) error {
	return h.s.Apply(sources.Event{Source: src, Session: session, Body: body})
}

func (h *harness) must(src string, session uint64, body peersession.Event) {
	h.t.Helper()
	if err := h.apply(src, session, body); err != nil {
		h.t.Fatalf("%s#%d %T: %v", src, session, body, err)
	}
}

func (h *harness) refused(src string, session uint64, body peersession.Event, cause error) {
	h.t.Helper()
	err := h.apply(src, session, body)
	if !errors.Is(err, snapshot.ErrRefused) || !errors.Is(err, cause) {
		h.t.Fatalf("%s#%d %T: %v, want a refusal wrapping %v", src, session, body, err, cause)
	}
}

func (h *harness) up(src string, session uint64) {
	h.t.Helper()
	h.must(src, session, peersession.SessionUp{Direction: peersession.Inbound, At: h.clock.Now()})
}

func (h *harness) define(src string, session uint64, def peermsg.Definition) {
	h.t.Helper()
	h.must(src, session, peersession.TableDefined{ID: 1, Definition: def, Received: h.clock.Now()})
}

func (h *harness) finish(src string, session uint64, partial bool) {
	h.t.Helper()
	h.must(src, session, peersession.SyncFinished{Partial: partial, Received: h.clock.Now()})
}

func (h *harness) down(src string, session uint64) {
	h.t.Helper()
	h.must(src, session, peersession.SessionDown{Err: errors.New("closed"), At: h.clock.Now()})
}

// sync brings a source's new session to a finished resync with the input
// table defined.
func (h *harness) sync(src string, session uint64) {
	h.t.Helper()
	h.up(src, session)
	h.define(src, session, inDef)
	h.finish(src, session, false)
}

func key(t *testing.T, s string) peermsg.Key {
	t.Helper()
	k, err := peermsg.KeyFromAddr(netip.MustParseAddr(s))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// upd is an ordinary update of the input table read now.
func (h *harness) upd(id peermsg.UpdateID, k peermsg.Key, cnt uint32) peersession.EntryUpdated {
	return peersession.EntryUpdated{ID: 1, Table: inTable, Expiry: expiry, Update: peermsg.Update{
		ID: id, ExplicitID: true, Key: k, Received: h.clock.Now(),
		Values: []peermsg.Value{
			{Type: peermsg.DataHTTPReqCnt, Uint: cnt},
			{Type: peermsg.DataHTTPReqRate, Freq: peermsg.FreqCounter{Age: 1234, Curr: cnt, Prev: 1}},
		},
	}}
}

// timed is a timed update (as a teach sends) with the remaining lifetime.
func (h *harness) timed(id peermsg.UpdateID, k peermsg.Key, cnt uint32, remaining peermsg.Millis) peersession.EntryUpdated {
	u := h.upd(id, k, cnt)
	u.Update.Timed, u.Update.Remaining = true, remaining
	return u
}

func (h *harness) count(src string, k peermsg.Key) (uint32, bool) {
	e, ok := h.s.Lookup(src, inTable, k)
	return e.Count, ok
}

func (h *harness) wantCount(src string, k peermsg.Key, want uint32) {
	h.t.Helper()
	if got, ok := h.count(src, k); !ok || got != want {
		h.t.Fatalf("source %s key %v: count %d (present %v), want %d", src, k, got, ok, want)
	}
}

func (h *harness) state(src string) snapshot.SourceReport {
	h.t.Helper()
	r, ok := h.s.Source(src)
	if !ok {
		h.t.Fatalf("no source %s", src)
	}
	return r
}

func (h *harness) wantState(src string, want snapshot.State) snapshot.SourceReport {
	h.t.Helper()
	r := h.state(src)
	if r.State != want {
		h.t.Fatalf("source %s is %v (%s), want %v", src, r.State, r.Reason, want)
	}
	return r
}

// TestTwoSourcesOneKey: two sources reporting one key retain two
// independent snapshots.
func TestTwoSourcesOneKey(t *testing.T) {
	h := newHarness(t, 16, "a", "b")
	k := key(t, "2001:db8::")
	h.sync("a", 1)
	h.sync("b", 1)
	h.must("a", 1, h.upd(1, k, 10))
	h.must("b", 1, h.upd(1, k, 20))
	h.wantCount("a", k, 10)
	h.wantCount("b", k, 20)
	got := h.s.Contributions(inTable, k)
	if len(got) != 2 || got[0].Source != "a" || got[0].Entry.Count != 10 || got[1].Source != "b" ||
		got[1].Entry.Count != 20 {
		t.Fatalf("contributions %+v", got)
	}
	for _, src := range []string{"a", "b"} {
		if r := h.state(src); r.Entries != 1 || r.Accepted != 1 {
			t.Fatalf("source %s: %+v", src, r)
		}
	}
}

// TestReplaceOwnSource: the last accepted update replaces only its
// source's previous value; a replayed identical update changes nothing.
func TestReplaceOwnSource(t *testing.T) {
	h := newHarness(t, 16, "a", "b")
	k, other := key(t, "2001:db8::"), key(t, "2001:db8:1::")
	h.sync("a", 1)
	h.sync("b", 1)
	h.must("a", 1, h.upd(1, k, 10))
	h.must("b", 1, h.upd(1, k, 20))
	h.must("a", 1, h.upd(2, other, 3))
	h.clock.Advance(time.Second)
	h.must("a", 1, h.upd(3, k, 11))
	h.wantCount("a", k, 11)
	h.wantCount("b", k, 20)
	h.wantCount("a", other, 3)
	h.must("a", 1, h.upd(4, k, 11))
	h.wantCount("a", k, 11)
	if c := h.s.Contributions(inTable, k); len(c) != 2 {
		t.Fatalf("contributions %+v", c)
	}
	if r := h.state("a"); r.Entries != 2 {
		t.Fatalf("source a has %d entries, want 2", r.Entries)
	}
}

// TestOverlappingSnapshotAndLive: teaches (resync replays) and live
// updates interleaved across sessions retain the newest valid state.
func TestOverlappingSnapshotAndLive(t *testing.T) {
	h := newHarness(t, 16, "a")
	k, k2 := key(t, "2001:db8::"), key(t, "2001:db8:2::")
	h.sync("a", 1)
	h.must("a", 1, h.upd(10, k, 3))
	h.must("a", 1, h.upd(11, k2, 7))
	h.down("a", 1)
	h.clock.Advance(time.Second)

	// Session 2 starts its teach, which interleaves with live traffic.
	h.up("a", 2)
	h.define("a", 2, inDef)
	h.must("a", 2, h.timed(500, k, 5, 25000))
	if e, _ := h.s.Lookup("a", inTable, k); e.Count != 5 || e.Session != 2 {
		t.Fatalf("taught value %+v", e)
	}
	h.wantState("a", snapshot.Syncing)
	h.clock.Advance(10 * time.Millisecond)
	h.must("a", 2, h.upd(1, k, 6)) // live, IDs restart: never compared
	h.finish("a", 2, true)         // partial: the source asks again
	h.wantState("a", snapshot.Syncing)
	h.clock.Advance(10 * time.Millisecond)
	h.must("a", 2, h.timed(501, k, 6, 29990)) // the retried teach
	h.finish("a", 2, false)
	h.wantCount("a", k, 6)
	h.wantState("a", snapshot.Ready)

	// A late event of the replaced session is refused and changes
	// nothing.
	late := h.upd(12, k, 4)
	h.refused("a", 1, late, snapshot.ErrSession)
	h.wantCount("a", k, 6)
	// So is an update read before the stored value: kept as the newer.
	stale := h.upd(13, k, 2)
	stale.Update.Received = h.clock.Now().Add(-time.Hour)
	h.must("a", 2, stale)
	h.wantCount("a", k, 6)
	if r := h.state("a"); r.Stale != 1 {
		t.Fatalf("stale count %d", r.Stale)
	}
	// An invalid update is refused (ending that session); the newest
	// valid value stays.
	bad := h.upd(14, k, 9)
	bad.Update.Values = bad.Update.Values[:1]
	h.refused("a", 2, bad, snapshot.ErrSchema)
	h.wantCount("a", k, 6)
	// The key session 2 has not re-sent keeps session 1's value, marked
	// as such, until it expires.
	if e, ok := h.s.Lookup("a", inTable, k2); !ok || e.Count != 7 || e.Session != 1 {
		t.Fatalf("earlier session's key: %+v %v", e, ok)
	}
}

// TestOneSourceCannotReadyRoster: one source completing its sync cannot
// make the whole roster ready.
func TestOneSourceCannotReadyRoster(t *testing.T) {
	h := newHarness(t, 16, "a", "b", "c")
	h.sync("a", 1)
	h.up("b", 1)
	h.define("b", 1, inDef)
	r := h.s.Roster()
	if r.Ready || r.Sources[0].State != snapshot.Ready || r.Sources[1].State != snapshot.Syncing ||
		r.Sources[2].State != snapshot.Disconnected {
		t.Fatalf("roster %+v", r)
	}
	if n := len(r.NotReady()); n != 2 {
		t.Fatalf("%d sources not ready", n)
	}
	// A partial reply does not complete b.
	h.finish("b", 1, true)
	if h.s.Roster().Ready {
		t.Fatal("ready after a partial reply")
	}
	h.finish("b", 1, false)
	h.sync("c", 1)
	if r := h.s.Roster(); !r.Ready {
		t.Fatalf("not ready with every source synchronized: %+v", r.NotReady())
	}
	// Losing any one source ends roster readiness.
	h.down("c", 1)
	if r := h.s.Roster(); r.Ready || r.Sources[2].State != snapshot.Disconnected {
		t.Fatalf("roster %+v", r)
	}
}

// TestEmptySyncedVersusMissing: an empty but synchronized, healthy
// source is distinct from a missing one.
func TestEmptySyncedVersusMissing(t *testing.T) {
	h := newHarness(t, 16, "a", "b")
	h.sync("a", 1)
	a := h.wantState("a", snapshot.Ready)
	if a.Entries != 0 || !a.Synced || !a.Healthy || !a.Tables[0].Defined {
		t.Fatalf("empty synchronized source %+v", a)
	}
	b := h.wantState("b", snapshot.Disconnected)
	if b.Session != 0 || b.Synced || b.Healthy {
		t.Fatalf("missing source %+v", b)
	}
	if h.s.Roster().Ready {
		t.Fatal("roster ready with a missing source")
	}
	// A source that synchronized without announcing the table does not
	// share it: it is degraded, not an empty contributor.
	h.up("b", 1)
	h.finish("b", 1, false)
	if r := h.wantState("b", snapshot.Degraded); !strings.Contains(r.Reason, inTable) {
		t.Fatalf("reason %q", r.Reason)
	}
	h.down("b", 1)
	h.sync("b", 2)
	if r := h.s.Roster(); !r.Ready {
		t.Fatalf("roster %+v", r.NotReady())
	}
}

// TestExpiryUnderFakeClock: expiry is deterministic under a fake clock
// and needs no input.
func TestExpiryUnderFakeClock(t *testing.T) {
	h := newHarness(t, 16, "a")
	normal, short, capped, gone := key(t, "2001:db8::1"), key(t, "2001:db8::2"), key(t, "2001:db8::3"),
		key(t, "2001:db8::4")
	h.sync("a", 1)
	start := h.clock.Now()
	h.must("a", 1, h.upd(1, normal, 1))               // table expiry: 30s
	h.must("a", 1, h.timed(2, short, 1, 5000))        // 5s left at the source
	h.must("a", 1, h.timed(3, capped, 1, 0x7fffffff)) // capped at 30s
	h.must("a", 1, h.timed(4, gone, 1, 0))            // already expired
	if _, ok := h.count("a", gone); ok {
		t.Fatal("an update with no lifetime left was stored")
	}
	if e, _ := h.s.Lookup("a", inTable, capped); !e.Deadline.Equal(start.Add(30 * time.Second)) {
		t.Fatalf("capped deadline %v after start", e.Deadline.Sub(start))
	}
	// The rate window's age does not shorten the entry's lifetime.
	if e, _ := h.s.Lookup("a", inTable, normal); e.Rate.Age != 1234 || !e.Deadline.Equal(start.Add(30*time.Second)) {
		t.Fatalf("entry %+v", e)
	}
	if next, ok := h.s.NextDeadline(); !ok || !next.Equal(start.Add(5*time.Second)) {
		t.Fatalf("next deadline %v %v", next.Sub(start), ok)
	}

	h.clock.Advance(5*time.Second - time.Millisecond)
	h.wantCount("a", short, 1)
	h.clock.Advance(time.Millisecond)
	if _, ok := h.count("a", short); ok {
		t.Fatal("entry visible at its deadline")
	}
	if n := h.s.Expire(); n != 1 {
		t.Fatalf("expired %d entries, want 1", n)
	}
	h.clock.Advance(25 * time.Second)
	if c := h.s.Contributions(inTable, normal); len(c) != 0 {
		t.Fatalf("contributions after expiry %+v", c)
	}
	r := h.state("a")
	if r.Entries != 0 || r.Expired != 4 {
		t.Fatalf("after expiry: %d entries, %d expired", r.Entries, r.Expired)
	}
	if _, ok := h.s.NextDeadline(); ok {
		t.Fatal("deadline left after every entry expired")
	}
	// An update with no lifetime left also removes the source's
	// stored value: replay never resurrects or extends it.
	h.must("a", 1, h.upd(5, normal, 2))
	h.must("a", 1, h.timed(6, normal, 3, 0))
	if _, ok := h.count("a", normal); ok {
		t.Fatal("expired replay left the value")
	}
}

// TestHealthUnderFakeClock: a quiet source stays healthy while its
// session reads messages (heartbeats, observed through Observe), and
// degrades once the health bound passes without one.
func TestHealthUnderFakeClock(t *testing.T) {
	h := newHarness(t, 16, "a")
	h.sync("a", 1)
	for range 4 {
		h.clock.Advance(3 * time.Second)
		h.s.Observe("a", 1, h.clock.Now()) // a heartbeat read
		h.wantState("a", snapshot.Ready)
	}
	h.s.Observe("a", 7, h.clock.Now().Add(time.Hour)) // another session's: ignored
	h.clock.Advance(healthTO - time.Millisecond)
	h.wantState("a", snapshot.Ready)
	h.clock.Advance(time.Millisecond)
	if r := h.wantState("a", snapshot.Degraded); r.Healthy || !strings.Contains(r.Reason, "unhealthy") {
		t.Fatalf("%+v", r)
	}
	if h.s.Roster().Ready {
		t.Fatal("roster ready with an unhealthy source")
	}
	h.s.ObserveStatus([]sources.SourceStatus{{
		Name: "a", Up: true, Session: 1,
		Stats: peersession.Stats{LastRx: h.clock.Now()},
	}})
	h.wantState("a", snapshot.Ready)
}

// TestSchemaMismatch: mismatches degrade or reject the affected logical
// table explicitly, and only for the affected source.
func TestSchemaMismatch(t *testing.T) {
	k := key(t, "2001:db8::")
	wrongPeriod := inDef
	wrongPeriod.Fields = []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: 5000}}
	onlyCount := inDef
	onlyCount.Fields = []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}}
	array := inDef
	array.Fields = []peermsg.Field{
		{Type: peermsg.DataHTTPReqCnt},
		{Type: peermsg.DataHTTPReqRate, Period: period},
		{Type: peermsg.DataGPT, ArrayLen: 2},
	}
	keyType := inDef
	keyType.KeyType = 2

	for name, def := range map[string]peermsg.Definition{
		"period": wrongPeriod, "fields": onlyCount, "array": array, "key type": keyType,
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, 16, "a", "b")
			h.sync("b", 1)
			h.must("b", 1, h.upd(1, k, 20))
			h.up("a", 1)
			h.refused("a", 1, peersession.TableDefined{ID: 1, Definition: def, Received: h.clock.Now()},
				snapshot.ErrSchema)
			r := h.wantState("a", snapshot.Degraded)
			if r.Tables[0].Rejected == nil || r.Tables[0].Defined || r.Fault == nil {
				t.Fatalf("table not marked rejected: %+v", r)
			}
			// Its updates are refused: the table is undefined.
			h.refused("a", 1, h.upd(1, k, 10), snapshot.ErrUndefined)
			if c := h.s.Contributions(inTable, k); len(c) != 1 || c[0].Source != "b" {
				t.Fatalf("contributions %+v", c)
			}
			h.wantState("b", snapshot.Ready)
			// Still degraded after the session ends and a new one is
			// syncing; a later complete sync with the right schema
			// clears it.
			h.down("a", 1)
			h.up("a", 2)
			h.wantState("a", snapshot.Degraded)
			h.define("a", 2, inDef)
			h.finish("a", 2, false)
			if r := h.wantState("a", snapshot.Ready); r.Tables[0].Rejected != nil || r.Fault != nil {
				t.Fatalf("%+v", r)
			}
		})
	}

	t.Run("session rejection", func(t *testing.T) {
		h := newHarness(t, 16, "a")
		h.up("a", 1)
		rej := peersession.TableRejected{ID: 3, Table: inTable, Err: peersession.ErrSchema, Received: h.clock.Now()}
		h.must("a", 1, rej)
		if r := h.wantState("a", snapshot.Degraded); !errors.Is(r.Tables[0].Rejected, peersession.ErrSchema) ||
			!errors.Is(r.Fault, snapshot.ErrSchema) {
			t.Fatalf("%+v", r)
		}
	})

	t.Run("values", func(t *testing.T) {
		h := newHarness(t, 16, "a")
		h.sync("a", 1)
		h.must("a", 1, h.upd(1, k, 1))
		swapped := h.upd(2, k, 2)
		swapped.Update.Values[0], swapped.Update.Values[1] = swapped.Update.Values[1], swapped.Update.Values[0]
		h.refused("a", 1, swapped, snapshot.ErrSchema)
		otherExpiry := h.upd(3, k, 3)
		otherExpiry.Expiry = 60000
		h.refused("a", 1, otherExpiry, snapshot.ErrSchema)
		h.wantCount("a", k, 1)
		h.wantState("a", snapshot.Degraded)
	})

	t.Run("expiry may differ between sources", func(t *testing.T) {
		h := newHarness(t, 16, "a", "b")
		long := inDef
		long.Expiry = 60000
		h.sync("a", 1)
		h.up("b", 1)
		h.define("b", 1, long)
		h.finish("b", 1, false)
		if r := h.s.Roster(); !r.Ready || r.Sources[1].Tables[0].Expiry != 60000 {
			t.Fatalf("%+v", r)
		}
	})
}

// TestOutputNotAdmitted: no output or metadata replay (or any other
// non-input table) is admitted into the snapshot store.
func TestOutputNotAdmitted(t *testing.T) {
	h := newHarness(t, 16, "a")
	k := key(t, "2001:db8::")
	h.sync("a", 1)
	outDef := peermsg.Definition{
		Name: "lab_out", KeyType: peermsg.KeyTypeIPv6, Expiry: expiry,
		Fields: []peermsg.Field{{Type: peermsg.DataGPT, ArrayLen: 4}},
	}
	h.refused("a", 1, peersession.TableDefined{ID: 2, Definition: outDef, Received: h.clock.Now()},
		snapshot.ErrNotInput)
	for _, table := range []string{"lab_out", "lab_meta", "prod_in"} {
		u := h.upd(1, k, 5)
		u.Table = table
		h.refused("a", 1, u, snapshot.ErrNotInput)
		if c := h.s.Contributions(table, k); len(c) != 0 {
			t.Fatalf("%s admitted: %+v", table, c)
		}
	}
	if r := h.state("a"); r.Entries != 0 || r.Accepted != 0 || r.State != snapshot.Degraded {
		t.Fatalf("%+v", r)
	}
}

// TestCapacity: entries are bounded per source; a refusal degrades only
// that source, and expiry frees capacity.
func TestCapacity(t *testing.T) {
	h := newHarness(t, 2, "a", "b")
	h.sync("a", 1)
	h.sync("b", 1)
	k1, k2, k3 := key(t, "2001:db8::1"), key(t, "2001:db8::2"), key(t, "2001:db8::3")
	h.must("a", 1, h.timed(1, k1, 1, 1000))
	h.must("a", 1, h.upd(2, k2, 1))
	h.must("a", 1, h.upd(3, k2, 2)) // replacing an existing key needs no room
	h.refused("a", 1, h.upd(4, k3, 1), snapshot.ErrCapacity)
	h.must("b", 1, h.upd(1, k3, 1))
	h.wantState("a", snapshot.Degraded)
	h.wantState("b", snapshot.Ready)
	h.clock.Advance(time.Second)
	h.must("a", 1, h.upd(5, k3, 1)) // k1 expired: room again
	h.wantCount("a", k3, 1)
}

// TestLifecycleOrder: events must belong to the current session.
func TestLifecycleOrder(t *testing.T) {
	h := newHarness(t, 4, "a")
	h.refused("x", 1, peersession.SessionUp{At: h.clock.Now()}, snapshot.ErrUnknownSource)
	h.refused("a", 1, h.upd(1, key(t, "::1"), 1), snapshot.ErrSession) // before any session
	h.up("a", 2)
	h.refused("a", 2, peersession.SessionUp{At: h.clock.Now()}, snapshot.ErrSession)
	h.refused("a", 1, peersession.SessionUp{At: h.clock.Now()}, snapshot.ErrSession)
	h.down("a", 2)
	h.refused("a", 2, peersession.SyncFinished{Received: h.clock.Now()}, snapshot.ErrSession)
	// Refusals outside the current session record no fault.
	if r := h.wantState("a", snapshot.Disconnected); r.Fault != nil {
		t.Fatalf("%+v", r)
	}
}

func TestNewValidates(t *testing.T) {
	good := snapshot.Options{
		Sources: []string{"a"}, Tables: []snapshot.Table{{Name: inTable, Period: period}},
		HealthTimeout: time.Second, MaxSourceEntries: 1,
	}
	for name, mutate := range map[string]func(*snapshot.Options){
		"no sources":       func(o *snapshot.Options) { o.Sources = nil },
		"no tables":        func(o *snapshot.Options) { o.Tables = nil },
		"duplicate source": func(o *snapshot.Options) { o.Sources = []string{"a", "a"} },
		"duplicate table":  func(o *snapshot.Options) { o.Tables = append(o.Tables, o.Tables[0]) },
		"no period":        func(o *snapshot.Options) { o.Tables = []snapshot.Table{{Name: inTable}} },
		"no health bound":  func(o *snapshot.Options) { o.HealthTimeout = 0 },
		"no capacity":      func(o *snapshot.Options) { o.MaxSourceEntries = 0 },
	} {
		o := good
		mutate(&o)
		if _, err := snapshot.New(o); !errors.Is(err, snapshot.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := snapshot.New(good); err != nil {
		t.Fatal(err)
	}
}

// TestDecreaseRecorded: an update that lowers a stored count replaces it
// and is recorded on the entry, across sessions, until the entry ends;
// an expired entry that was not yet purged passes no history on.
func TestDecreaseRecorded(t *testing.T) {
	h := newHarness(t, 16, "a")
	k := key(t, "2001:db8::")
	h.sync("a", 1)
	h.must("a", 1, h.upd(1, k, math.MaxUint32))
	h.must("a", 1, h.upd(2, k, 0))
	e, _ := h.s.Lookup("a", inTable, k)
	want := snapshot.Decrease{From: math.MaxUint32, To: 0, FromSession: 1, ToSession: 1, At: h.clock.Now()}
	if e.Count != 0 || e.Decreases != 1 || e.LastDecrease != want {
		t.Fatalf("after a wrap: %+v", e)
	}
	h.must("a", 1, h.upd(3, k, 7))
	h.up("a", 2)
	h.define("a", 2, inDef)
	h.must("a", 2, h.timed(1, k, 7, 20000))
	h.must("a", 2, h.timed(2, k, 4, 20000))
	e, _ = h.s.Lookup("a", inTable, k)
	if e.Count != 4 || e.Decreases != 2 || e.LastDecrease.From != 7 || e.LastDecrease.FromSession != 2 ||
		e.LastDecrease.ToSession != 2 {
		t.Fatalf("after a later decrease: %+v", e)
	}
	if r := h.state("a"); r.Decreases != 2 {
		t.Fatalf("source decreases %d, want 2", r.Decreases)
	}
	// A lower count arriving with no lifetime left removes the entry; it
	// is an expiry, not a stored decrease.
	other := key(t, "2001:db8:1::")
	h.must("a", 2, h.timed(3, other, 10, 20000))
	h.must("a", 2, h.timed(4, other, 3, 0))
	if _, ok := h.s.Lookup("a", inTable, other); ok {
		t.Fatal("expired lower update stored")
	}
	if r := h.state("a"); r.Decreases != 2 || r.Expired != 1 {
		t.Fatalf("after an expired lower update: decreases %d expired %d, want 2 and 1", r.Decreases, r.Expired)
	}
	// The entry expires; no read purges it before the next update.
	h.clock.Advance(time.Duration(20000) * time.Millisecond)
	h.must("a", 2, h.upd(3, k, 1))
	e, _ = h.s.Lookup("a", inTable, k)
	if e.Count != 1 || e.Decreases != 0 {
		t.Fatalf("new entry after expiry: %+v", e)
	}
	if r := h.state("a"); r.Expired != 2 || r.Entries != 1 {
		t.Fatalf("source after expiry: expired %d entries %d", r.Expired, r.Entries)
	}
}
