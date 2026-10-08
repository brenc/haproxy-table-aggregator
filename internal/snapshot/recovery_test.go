package snapshot_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
)

// idle is an update of k whose rate counter is empty, so losing it
// cannot change a rate.
func (h *harness) idle(id peermsg.UpdateID, k peermsg.Key, cnt uint32) peersession.EntryUpdated {
	u := h.upd(id, k, cnt)
	u.Update.Values[1].Freq = peermsg.FreqCounter{}
	return u
}

// observe records a message read now on every listed source's session.
func (h *harness) observe(sessions map[string]uint64) {
	for src, s := range sessions {
		h.s.Observe(src, s, h.clock.Now())
	}
}

// oneEach fails unless every source holds at most one entry for k.
func (h *harness) oneEach(k peermsg.Key) {
	h.t.Helper()
	seen := map[string]bool{}
	for _, c := range h.s.Contributions(inTable, k) {
		if seen[c.Source] {
			h.t.Fatalf("source %s contributes twice for %v", c.Source, k)
		}
		seen[c.Source] = true
	}
}

// TestRestartRecovers: an empty store (a restarted aggregator) rebuilds
// both sources from their teaches, live updates interleaved with the
// teach, a repeated teach after a "partial" reply, and duplicated
// records, without adding replayed values; the roster is ready only once
// every source has finished.
func TestRestartRecovers(t *testing.T) {
	h := newHarness(t, 16, "a", "b")
	k1, k2, k3 := key(t, "2001:db8::1"), key(t, "2001:db8::2"), key(t, "2001:db8::3")
	if h.s.Roster().Ready {
		t.Fatal("empty store ready")
	}
	h.up("a", 1)
	h.up("b", 1)
	h.define("a", 1, inDef)
	h.define("b", 1, inDef)
	// a teaches k1=10 and k2=4 with their remaining lifetimes; a live
	// request on k1 arrives mid-teach (an ordinary update, k1=11).
	h.must("a", 1, h.timed(1, k1, 10, 20000))
	h.must("a", 1, h.upd(1, k1, 11))
	h.must("a", 1, h.timed(2, k2, 4, 9000))
	h.finish("a", 1, true) // a was not itself synchronized yet
	h.wantState("a", snapshot.Syncing)
	// The retried teach repeats every record, now with k1 at 11.
	h.must("a", 1, h.timed(1, k1, 11, 19990))
	h.must("a", 1, h.timed(2, k2, 4, 8990))
	h.must("a", 1, h.timed(2, k2, 4, 8990)) // a duplicated record
	// b teaches k1=20 and k3=1, with a live new key k2 mid-teach.
	h.must("b", 1, h.timed(7, k1, 20, 25000))
	h.must("b", 1, h.upd(1, k2, 1))
	h.must("b", 1, h.timed(8, k3, 1, 5000))
	h.finish("b", 1, false)
	if h.s.Roster().Ready {
		t.Fatal("roster ready before a finished")
	}
	h.finish("a", 1, false)
	if !h.s.Roster().Ready {
		t.Fatalf("roster not ready: %+v", h.s.Roster())
	}
	for _, c := range []struct {
		src  string
		k    peermsg.Key
		want uint32
	}{{"a", k1, 11}, {"a", k2, 4}, {"b", k1, 20}, {"b", k2, 1}, {"b", k3, 1}} {
		h.wantCount(c.src, c.k, c.want)
	}
	for _, k := range []peermsg.Key{k1, k2, k3} {
		h.oneEach(k)
	}
	// Remaining lifetimes are kept: k2 on a lives 8.99 s, not a full
	// table expiry from the replay.
	if e, _ := h.s.Lookup("a", inTable, k2); e.Deadline != h.clock.Now().Add(8990*time.Millisecond) {
		t.Fatalf("k2 deadline %v after the teach", e.Deadline.Sub(h.clock.Now()))
	}
	for _, src := range []string{"a", "b"} {
		if r := h.state(src); r.Loss.Absent+r.Loss.Recreated != 0 || r.Released != 0 {
			t.Fatalf("restart reported lost history for %s: %+v", src, r)
		}
	}
}

// TestReconcileOnFinished: a finished resync releases the entries an
// earlier session delivered and the current one did not confirm; a
// partial reply and an interrupted teach release nothing; an entry
// confirmed by the teach keeps its new value; an unconfirmed entry
// expiring within LossGrace is not lost history.
func TestReconcileOnFinished(t *testing.T) {
	h := newHarness(t, 16, "a")
	k1, k2, k3, k4 := key(t, "2001:db8::1"), key(t, "2001:db8::2"), key(t, "2001:db8::3"), key(t, "2001:db8::4")
	h.sync("a", 1)
	h.must("a", 1, h.upd(1, k1, 5))
	h.must("a", 1, h.upd(2, k2, 6))
	h.must("a", 1, h.idle(3, k3, 7)) // its rate is empty
	h.clock.Advance(time.Second)
	h.must("a", 1, h.upd(4, k4, 8))
	h.down("a", 1)

	// Session 2 is interrupted mid-teach: nothing is released.
	h.clock.Advance(time.Second)
	h.up("a", 2)
	h.define("a", 2, inDef)
	h.must("a", 2, h.timed(1, k1, 5, 27000))
	h.finish("a", 2, true)
	h.down("a", 2)
	if r := h.state("a"); r.Entries != 4 || r.Released != 0 {
		t.Fatalf("after an interrupted teach: %+v", r)
	}

	// Session 3 teaches k1 and k4 only, then finishes: k2 and k3 no
	// longer exist at the source. k2 is lost history with a rate; k3's
	// rate was empty, so it delays nothing.
	h.clock.Advance(time.Second)
	h.up("a", 3)
	h.define("a", 3, inDef)
	h.must("a", 3, h.timed(1, k1, 5, 26000))
	h.must("a", 3, h.timed(2, k4, 9, 28000))
	for _, k := range []peermsg.Key{k2, k3} {
		if e, ok := h.s.Lookup("a", inTable, k); !ok || e.Session != 1 {
			t.Fatalf("held-over %v before the finished reply: %+v %v", k, e, ok)
		}
	}
	h.finish("a", 3, false)
	for _, k := range []peermsg.Key{k2, k3} {
		if _, ok := h.s.Lookup("a", inTable, k); ok {
			t.Fatalf("unconfirmed key %v still held", k)
		}
	}
	h.wantCount("a", k1, 5)
	h.wantCount("a", k4, 9)
	r := h.wantState("a", snapshot.Degraded)
	if r.Entries != 2 || r.Released != 2 || r.Loss.Absent != 2 || r.Loss.Session != 3 {
		t.Fatalf("after reconciliation: %+v", r)
	}
	// k2's last report (age 1234 ms, curr 6) reads 0 from e = 18334 ms
	// (6*(20000-e) < 10000), 17.1 s after it was read, 14.1 s from now.
	// But no session reported k2 between session 1's end and session 3's
	// start (now), so the source may have counted events into it up to
	// now: those weigh for two periods (20.001 s), and the entry lives up
	// to the table expiry (30 s) after now.
	wantRate := h.clock.Now().Add(2*time.Duration(period)*time.Millisecond + time.Millisecond)
	if r.Loss.RateUntil != wantRate || r.Loss.CountUntil != h.clock.Now().Add(time.Duration(expiry)*time.Millisecond) {
		t.Fatalf("loss %+v; want rate until %v", r.Loss, wantRate.Sub(h.clock.Now()))
	}
	sess := map[string]uint64{"a": 3}
	for h.clock.Now().Add(4 * time.Second).Before(wantRate) {
		h.clock.Advance(4 * time.Second)
		h.observe(sess)
		h.wantState("a", snapshot.Degraded)
	}
	h.clock.Advance(wantRate.Sub(h.clock.Now()))
	h.observe(sess)
	h.wantState("a", snapshot.Ready)

	// Session 4 does not confirm k1, which expires within LossGrace
	// of the finished reply: released, but not lost history.
	h.down("a", 3)
	e1, _ := h.s.Lookup("a", inTable, k1)
	h.clock.Advance(e1.Deadline.Sub(h.clock.Now()) - snapshot.LossGrace)
	h.up("a", 4)
	h.define("a", 4, inDef)
	h.must("a", 4, h.timed(1, k4, 9, 1000))
	h.finish("a", 4, false)
	r = h.wantState("a", snapshot.Ready)
	if r.Released != 3 || r.Loss.Absent != 2 {
		t.Fatalf("near-expiry release: %+v", r)
	}
}

// TestUnannouncedTableKept: a finished resync that did not announce a
// table releases nothing from it; the source is degraded instead.
func TestUnannouncedTableKept(t *testing.T) {
	h := newHarness(t, 16, "a")
	k := key(t, "2001:db8::1")
	h.sync("a", 1)
	h.must("a", 1, h.upd(1, k, 5))
	h.down("a", 1)
	h.up("a", 2)
	h.finish("a", 2, false)
	if r := h.wantState("a", snapshot.Degraded); r.Released != 0 || r.Loss.Absent != 0 {
		t.Fatalf("unannounced table reconciled: %+v", r)
	}
	if e, ok := h.s.Lookup("a", inTable, k); !ok || e.Session != 1 {
		t.Fatalf("entry of the unannounced table: %+v %v", e, ok)
	}
}

// TestLostAckReplay: an update whose acknowledgement was lost (the
// session ended after the store applied it) is taught again by the next
// session and replaces itself: no inflation and no lost history.
func TestLostAckReplay(t *testing.T) {
	h := newHarness(t, 16, "a")
	k := key(t, "2001:db8::1")
	h.sync("a", 1)
	h.must("a", 1, h.upd(41, k, 12))
	h.down("a", 1) // the ACK for update 41 never left
	h.resyncA2(h.timed(1, k, 12, 29000))
	h.wantCount("a", k, 12)
	h.oneEach(k)
	if r := h.wantState("a", snapshot.Ready); r.Accepted != 2 || r.Entries != 1 || r.Loss.Absent != 0 {
		t.Fatalf("after the replay: %+v", r)
	}
}

// resyncA2 starts source a's session 2, defines the input table,
// applies the teach, and finishes it.
func (h *harness) resyncA2(teach ...peersession.EntryUpdated) {
	h.t.Helper()
	h.up("a", 2)
	h.define("a", 2, inDef)
	for _, u := range teach {
		h.must("a", 2, u)
	}
	h.finish("a", 2, false)
}

// TestReplayNearExpiry: a replay never extends a lifetime or resurrects
// an expired key: a timed update keeps the source's remaining lifetime,
// one with none left removes the value, and a key that expired here is
// absent from the next teach and stays gone. Same-key live updates near
// expiry renew the entry exactly as the source did.
func TestReplayNearExpiry(t *testing.T) {
	h := newHarness(t, 16, "a")
	k1, k2 := key(t, "2001:db8::1"), key(t, "2001:db8::2")
	h.sync("a", 1)
	h.must("a", 1, h.upd(1, k1, 3))
	h.must("a", 1, h.upd(2, k2, 4))
	h.clock.Advance(time.Duration(expiry)*time.Millisecond - 50*time.Millisecond)
	h.observe(map[string]uint64{"a": 1})
	// A live request on k2 renews it at the source 50 ms before expiry.
	h.must("a", 1, h.upd(3, k2, 5))
	h.clock.Advance(50 * time.Millisecond)
	if _, ok := h.s.Lookup("a", inTable, k1); ok {
		t.Fatal("k1 outlived its deadline")
	}
	h.wantCount("a", k2, 5)
	h.down("a", 1)
	// The next session's teach: k1 is gone at the source; k2 has 29.95
	// s left. A late timed record of k1 with no lifetime left (sent
	// as it expired) must not bring it back.
	h.up("a", 2)
	h.define("a", 2, inDef)
	h.must("a", 2, h.timed(1, k1, 3, 0))
	h.must("a", 2, h.timed(2, k2, 5, 29950))
	h.finish("a", 2, false)
	if _, ok := h.s.Lookup("a", inTable, k1); ok {
		t.Fatal("expired key resurrected by a replay")
	}
	e, _ := h.s.Lookup("a", inTable, k2)
	if want := h.clock.Now().Add(29950 * time.Millisecond); e.Deadline != want {
		t.Fatalf("k2 deadline %v, want the remaining lifetime", e.Deadline.Sub(h.clock.Now()))
	}
	// A timed record claiming more than the table expiry is capped.
	h.must("a", 2, h.timed(3, k2, 5, expiry+5000))
	if e, _ = h.s.Lookup("a", inTable, k2); e.Deadline != h.clock.Now().Add(time.Duration(expiry)*time.Millisecond) {
		t.Fatalf("k2 deadline %v, want capped at the expiry", e.Deadline.Sub(h.clock.Now()))
	}
	if r := h.wantState("a", snapshot.Ready); r.Loss.Absent != 0 {
		t.Fatalf("expired keys reported lost: %+v", r)
	}
}

// TestCapacityReconnect: a source at its entry capacity that reconnects
// with a different key set is not refused: the first new key releases
// every unconfirmed earlier-session entry (conservatively counted as
// lost), and the store never holds more than its capacity.
func TestCapacityReconnect(t *testing.T) {
	const capacity = 3
	h := newHarness(t, capacity, "a")
	h.sync("a", 1)
	var old, fresh []peermsg.Key
	for i := range capacity {
		old = append(old, key(t, fmt.Sprintf("2001:db8::%x", i+1)))
		fresh = append(fresh, key(t, fmt.Sprintf("2001:db8:1::%x", i+1)))
		h.must("a", 1, h.upd(peermsg.UpdateID(i+1), old[i], 2))
	}
	h.down("a", 1)
	// Session 2's snapshot: old[0] (confirmed, replacing its entry
	// needs no room) and two new keys.
	h.up("a", 2)
	h.define("a", 2, inDef)
	h.must("a", 2, h.timed(1, old[0], 2, 29000))
	for i, k := range fresh[:capacity-1] {
		h.must("a", 2, h.timed(peermsg.UpdateID(i+2), k, 1, 29000))
		if r := h.state("a"); r.Entries > capacity {
			t.Fatalf("%d entries above capacity %d", r.Entries, capacity)
		}
	}
	h.finish("a", 2, false)
	r := h.wantState("a", snapshot.Degraded) // lost history, not a fault
	if r.Fault != nil || r.Entries != capacity || r.Released != capacity-1 || r.Loss.Absent != capacity-1 {
		t.Fatalf("after the reconnect: %+v", r)
	}
	h.wantCount("a", old[0], 2)
	for _, k := range fresh[:capacity-1] {
		h.wantCount("a", k, 1)
	}
	// Capacity still bounds the current session's own keys.
	h.refused("a", 2, h.timed(9, fresh[capacity-1], 1, 29000), snapshot.ErrCapacity)
}

// TestRepeatedReconnects: many sessions with shifting key sets keep one
// entry per source and key, release replaced state at every finished
// resync, and hold no more entries than the last snapshot's keys; the
// store's heap does not grow with the number of sessions.
func TestRepeatedReconnects(t *testing.T) {
	h := newHarness(t, 64, "a", "b")
	var keys []peermsg.Key
	for i := range 8 {
		keys = append(keys, key(t, fmt.Sprintf("2001:db8::%x", i+1)))
	}
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	h.sync("b", 1)
	var before uint64
	for s := uint64(1); s <= 2000; s++ {
		if s == 100 {
			before = heap()
		}
		h.up("a", s)
		h.define("a", s, inDef)
		// Session s teaches keys s%8 .. s%8+3: a shifting window.
		taught := map[peermsg.Key]bool{}
		for j := range 4 {
			k := keys[(int(s)+j)%len(keys)]
			taught[k] = true
			h.must("a", s, h.timed(peermsg.UpdateID(j+1), k, uint32(s), 20000))
		}
		h.finish("a", s, false)
		r := h.state("a")
		if r.Entries != len(taught) {
			t.Fatalf("session %d: %d entries, want %d", s, r.Entries, len(taught))
		}
		for _, k := range keys {
			h.oneEach(k)
			if e, ok := h.s.Lookup("a", inTable, k); ok != taught[k] || (ok && e.Session != s) {
				t.Fatalf("session %d key %v: %+v %v", s, k, e, ok)
			}
		}
		h.down("a", s)
		h.clock.Advance(time.Millisecond)
		h.s.Observe("b", 1, h.clock.Now())
	}
	if after := heap(); after > before+256<<10 {
		t.Fatalf("heap grew from %d to %d bytes over 1900 sessions", before, after)
	}
}

// TestLossDetectionLimits pins what the store can and cannot detect.
func TestLossDetectionLimits(t *testing.T) {
	k := key(t, "2001:db8::1")
	t.Run("restarted store sees no loss", func(t *testing.T) {
		// After this process restarts, a source that also lost its
		// table (or never had one) is indistinguishable: Ready at once.
		h := newHarness(t, 16, "a")
		h.sync("a", 1)
		if r := h.wantState("a", snapshot.Ready); r.Loss.Absent+r.Loss.Recreated != 0 {
			t.Fatalf("%+v", r)
		}
	})
	t.Run("recreated at or above the count", func(t *testing.T) {
		h := newHarness(t, 16, "a")
		h.sync("a", 1)
		h.must("a", 1, h.upd(1, k, 5))
		h.down("a", 1)
		h.resyncA2(h.timed(1, k, 5, 29000))
		if r := h.wantState("a", snapshot.Ready); r.Loss.Recreated != 0 {
			t.Fatalf("%+v", r)
		}
	})
	t.Run("recreated lower", func(t *testing.T) {
		h := newHarness(t, 16, "a")
		h.sync("a", 1)
		h.must("a", 1, h.upd(1, k, 5))
		h.down("a", 1)
		h.resyncA2(h.timed(1, k, 1, 29000))
		if r := h.wantState("a", snapshot.Degraded); r.Loss.Recreated != 1 || r.Loss.Absent != 0 {
			t.Fatalf("%+v", r)
		}
		h.wantCount("a", k, 1)
	})
	t.Run("lower within a session is not a session loss", func(t *testing.T) {
		h := newHarness(t, 16, "a")
		h.sync("a", 1)
		h.must("a", 1, h.upd(1, k, 5))
		h.must("a", 1, h.upd(2, k, 0)) // a wrap or runtime reset
		if r := h.wantState("a", snapshot.Ready); r.Loss.Recreated != 0 || r.Decreases != 1 {
			t.Fatalf("%+v", r)
		}
	})
	t.Run("expired here before the reconnect", func(t *testing.T) {
		h := newHarness(t, 16, "a")
		h.sync("a", 1)
		h.must("a", 1, h.upd(1, k, 5))
		h.down("a", 1)
		h.clock.Advance(time.Duration(expiry) * time.Millisecond)
		h.resyncA2()
		if r := h.wantState("a", snapshot.Ready); r.Loss.Absent != 0 {
			t.Fatalf("%+v", r)
		}
	})
	t.Run("unreported history after a long gap", func(t *testing.T) {
		// k is last reported at t0; the session drops at t0+6 s and the
		// source keeps counting unreported, then restarts cold; the next
		// session comes up at t0+20 s, when the last report has long
		// decayed. The source may have counted into k until then, so it
		// stays degraded two periods from that moment.
		h := newHarness(t, 16, "a")
		h.sync("a", 1)
		h.must("a", 1, h.upd(1, k, 5))
		h.clock.Advance(6 * time.Second)
		h.down("a", 1)
		h.clock.Advance(14 * time.Second)
		up := h.clock.Now()
		h.resyncA2()
		r := h.wantState("a", snapshot.Degraded)
		want := up.Add(2*time.Duration(period)*time.Millisecond + time.Millisecond)
		if r.Loss.Absent != 1 || r.Loss.RateUntil != want ||
			r.Loss.CountUntil != up.Add(time.Duration(expiry)*time.Millisecond) {
			t.Fatalf("%+v; want rate until %v", r.Loss, want.Sub(up))
		}
		h.clock.Advance(want.Sub(h.clock.Now()) - time.Millisecond)
		h.s.Observe("a", 2, h.clock.Now())
		h.wantState("a", snapshot.Degraded)
		h.clock.Advance(time.Millisecond)
		h.wantState("a", snapshot.Ready)
	})
	t.Run("idle entry lost still covers unreported events", func(t *testing.T) {
		h := newHarness(t, 16, "a")
		h.sync("a", 1)
		h.must("a", 1, h.idle(1, k, 5))
		h.down("a", 1)
		h.resyncA2()
		r := h.wantState("a", snapshot.Degraded)
		want := h.clock.Now().Add(2*time.Duration(period)*time.Millisecond + time.Millisecond)
		if r.Loss.Absent != 1 || r.Loss.RateUntil != want {
			t.Fatalf("%+v", r)
		}
	})
}

// TestLateDefinitionReconciled: a table announced only after the
// finished reply (not stock HAProxy behavior) is reconciled when it is
// announced, so a Ready source never holds an earlier session's entry.
func TestLateDefinitionReconciled(t *testing.T) {
	h := newHarness(t, 16, "a")
	k := key(t, "2001:db8::1")
	h.sync("a", 1)
	h.must("a", 1, h.upd(1, k, 5))
	h.down("a", 1)
	h.up("a", 2)
	h.finish("a", 2, false)
	h.wantState("a", snapshot.Degraded) // the table is not announced
	h.define("a", 2, inDef)
	if e, ok := h.s.Lookup("a", inTable, k); ok {
		t.Fatalf("earlier session's entry kept after a late definition: %+v", e)
	}
	if r := h.state("a"); r.Released != 1 || r.Loss.Absent != 1 || r.State != snapshot.Degraded {
		t.Fatalf("after the late definition: %+v", r)
	}
}
