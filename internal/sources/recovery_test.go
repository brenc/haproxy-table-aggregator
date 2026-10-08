package sources_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// teachAll completes a scripted session's teach of the given counts per
// key, then a finished resync reply.
func (f *fakePeer) teachAll(counts map[string]uint32) {
	f.t.Helper()
	f.established()
	f.define(1, inDef)
	id := uint32(0)
	for k, c := range counts {
		id++
		f.update(inDef, peermsg.UpdateID(id), k, c)
		f.expectAck(peermsg.UpdateID(id))
	}
	f.control(peerwire.ControlResyncFinished)
	f.expect(peerwire.ClassControl, peerwire.ControlResyncConfirm)
}

// TestRecoveryThroughManager drives real sessions through a lost
// acknowledgement, an interrupted teach, a duplicate connection that
// replaces the current one, and repeated reconnects: the store keeps one
// contribution per source and key, never adds replayed values, releases
// what a finished snapshot no longer contains as lost history, and the
// manager's goroutines do not accumulate.
func TestRecoveryThroughManager(t *testing.T) {
	cfg := fastConfig(config.Source{Name: "a"})
	_, _, store, addr := snapshotManager(t, cfg, 16, sources.Options{})
	k1, k2 := mustKey(t, "2001:db8::1"), mustKey(t, "2001:db8::2")
	one := func(want uint32) {
		t.Helper()
		c := store.Contributions("t_in", k1)
		if len(c) != 1 || c[0].Entry.Count != want {
			t.Fatalf("contributions for k1 %+v, want one of %d", c, want)
		}
	}

	f := connectFake(t, addr, "a")
	f.teachAll(map[string]uint32{"2001:db8::1": 3, "2001:db8::2": 4})
	waitSource(t, store, "a", "ready", inState(snapshot.Ready))

	// Lost acknowledgement: the source sends k1=5 and its connection
	// dies before the ACK can reach it.
	f.update(inDef, 3, "2001:db8::1", 5)
	waitSource(t, store, "a", "k1=5 applied", func(r snapshot.SourceReport) bool { return r.Accepted == 3 })
	_ = f.conn.Close()
	waitSource(t, store, "a", "disconnected", inState(snapshot.Disconnected))

	// The next session is interrupted mid-teach (only k1 re-sent).
	f = connectFake(t, addr, "a")
	f.established()
	f.define(1, inDef)
	f.update(inDef, 1, "2001:db8::1", 5)
	f.expectAck(1)
	_ = f.conn.Close()
	r := waitSource(t, store, "a", "disconnected", inState(snapshot.Disconnected))
	if r.Released != 0 || r.Entries != 2 {
		t.Fatalf("an interrupted teach reconciled: %+v", r)
	}
	one(5)

	// A session that teaches only k1, replaced mid-teach by a duplicate
	// connection of the same source, whose complete snapshot also lacks
	// k2 (the source lost it).
	f = connectFake(t, addr, "a")
	f.established()
	f.define(1, inDef)
	f.update(inDef, 1, "2001:db8::1", 5)
	f.expectAck(1)
	dup := connectFake(t, addr, "a")
	f.expectClose()
	dup.teachAll(map[string]uint32{"2001:db8::1": 5})
	r = waitSource(t, store, "a", "reconciled", func(r snapshot.SourceReport) bool { return r.Released == 1 })
	if _, ok := store.Lookup("a", "t_in", k2); ok || r.Loss.Absent != 1 || r.State != snapshot.Degraded {
		t.Fatalf("after the duplicate's finished snapshot: %+v", r)
	}
	one(5)

	// Repeated reconnects, each a duplicate replacing the last.
	runtime.GC()
	before := runtime.NumGoroutine()
	session := r.Session
	for i := range 50 {
		next := connectFake(t, addr, "a")
		dup.expectClose()
		next.teachAll(map[string]uint32{"2001:db8::1": uint32(5 + i)})
		dup = next
		r = waitSource(t, store, "a", "next session synced", func(r snapshot.SourceReport) bool {
			return r.Session > session && r.Synced
		})
		session = r.Session
		one(uint32(5 + i))
		if r.Entries != 1 {
			t.Fatalf("reconnect %d: %d entries", i, r.Entries)
		}
	}
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	if after := runtime.NumGoroutine(); after > before+4 {
		t.Fatalf("goroutines grew from %d to %d over 50 reconnects", before, after)
	}
}
