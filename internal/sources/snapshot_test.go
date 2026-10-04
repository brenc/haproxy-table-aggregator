package sources_test

import (
	"errors"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// snapshotManager starts a manager for inbound sources whose events a
// snapshot store applies; capacity bounds each source's entries.
func snapshotManager(t *testing.T, cfg config.Config, capacity int, opts sources.Options) (
	*sources.Manager, *recorder, *snapshot.Store, string,
) {
	t.Helper()
	cfg.HealthTimeout, cfg.MaxSourceEntries = time.Minute, capacity
	store, err := snapshot.New(snapshot.OptionsFrom(cfg))
	if err != nil {
		t.Fatal(err)
	}
	ln := listen(t)
	opts.Config, opts.Listener, opts.Apply = cfg, ln, store.Apply
	m, r := startManager(t, opts)
	return m, r, store, ln.Addr().String()
}

// waitSource polls the store until the source report satisfies pred.
func waitSource(t *testing.T, store *snapshot.Store, src, what string, pred func(snapshot.SourceReport) bool,
) snapshot.SourceReport {
	t.Helper()
	deadline := time.Now().Add(fakeTimeout)
	for {
		r, _ := store.Source(src)
		if pred(r) {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("source %s: timed out waiting for %s; last %+v", src, what, r)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func inState(want snapshot.State) func(snapshot.SourceReport) bool {
	return func(r snapshot.SourceReport) bool { return r.State == want }
}

// expectAck reads acknowledgements of table 1 until one names want.
func (f *fakePeer) expectAck(want peermsg.UpdateID) {
	f.t.Helper()
	for {
		ack := f.expect(peerwire.ClassStickTable, peerwire.StickTableAck)
		id, upd, err := peermsg.DecodeAck(ack.Body)
		if err != nil || id != 1 {
			f.t.Fatalf("ack %d/%d %v", id, upd, err)
		}
		if upd == want {
			return
		}
	}
}

// TestSnapshotThroughManager applies real session events: two sources'
// values for one key stay separate, one source's complete sync leaves the
// roster unready, and an empty synchronized source is ready while a
// missing one is not.
func TestSnapshotThroughManager(t *testing.T) {
	cfg := fastConfig(config.Source{Name: "a"}, config.Source{Name: "b"})
	_, _, store, addr := snapshotManager(t, cfg, 16, sources.Options{})
	k := mustKey(t, "2001:db8::")

	a := connectFake(t, addr, "a")
	a.established()
	a.define(1, inDef)
	a.update(inDef, 1, "2001:db8::", 10)
	a.expectAck(1)
	a.control(peerwire.ControlResyncFinished)
	a.expect(peerwire.ClassControl, peerwire.ControlResyncConfirm)
	waitSource(t, store, "a", "a ready", inState(snapshot.Ready))
	if r := store.Roster(); r.Ready || r.Sources[1].State != snapshot.Disconnected {
		t.Fatalf("roster ready, or b not disconnected, with b missing: %+v", r)
	}

	// b synchronizes an empty table: ready, with nothing stored.
	b := connectFake(t, addr, "b")
	b.established()
	b.define(1, inDef)
	b.control(peerwire.ControlResyncFinished)
	b.expect(peerwire.ClassControl, peerwire.ControlResyncConfirm)
	if r := waitSource(t, store, "b", "b ready", inState(snapshot.Ready)); r.Entries != 0 {
		t.Fatalf("empty source %+v", r)
	}
	if r := store.Roster(); !r.Ready {
		t.Fatalf("roster not ready: %+v", r.NotReady())
	}

	b.update(inDef, 1, "2001:db8::", 20)
	b.expectAck(1)
	a.update(inDef, 2, "2001:db8::", 11)
	a.expectAck(2)
	c := store.Contributions("t_in", k)
	if len(c) != 2 || c[0].Source != "a" || c[0].Entry.Count != 11 || c[1].Source != "b" || c[1].Entry.Count != 20 {
		t.Fatalf("contributions %+v", c)
	}
}

// TestSnapshotRefusalNotAcked: an update the store refuses is never
// acknowledged; the session ends and the source degrades.
func TestSnapshotRefusalNotAcked(t *testing.T) {
	cfg := fastConfig(config.Source{Name: "a"})
	_, r, store, addr := snapshotManager(t, cfg, 1, sources.Options{})
	f := connectFake(t, addr, "a")
	f.established()
	f.define(1, inDef)
	f.update(inDef, 1, "2001:db8::1", 1)
	f.expectAck(1)
	f.update(inDef, 2, "2001:db8::2", 1)
	if got := f.expectClose(); len(got) != 0 {
		t.Fatalf("daemon sent %v (an ack?) for an update the store refused", got)
	}
	evs := r.waitFor("down", fakeTimeout, func(evs []sources.Event) bool { return len(downs(evs, "a")) == 1 })
	if err := downs(evs, "a")[0].Err; !errors.Is(err, peersession.ErrEventRejected) ||
		!errors.Is(err, snapshot.ErrCapacity) {
		t.Fatalf("ended with %v", err)
	}
	if n := count[peersession.EntryUpdated](evs, "a"); n != 1 {
		t.Fatalf("%d updates queued, want only the accepted one", n)
	}
	rep := waitSource(t, store, "a", "degraded", inState(snapshot.Degraded))
	if _, ok := store.Lookup("a", "t_in", mustKey(t, "2001:db8::2")); ok || rep.Entries != 1 {
		t.Fatalf("refused update stored: %+v", rep)
	}
}

// TestSnapshotSchemaRejected: an input table with another schema is
// reported to the store, which marks that logical table rejected.
func TestSnapshotSchemaRejected(t *testing.T) {
	cfg := fastConfig(config.Source{Name: "a"})
	_, r, store, addr := snapshotManager(t, cfg, 16, sources.Options{})
	f := connectFake(t, addr, "a")
	f.established()
	bad := inDef
	bad.Fields = []peermsg.Field{{Type: peermsg.DataHTTPReqCnt}, {Type: peermsg.DataHTTPReqRate, Period: 5000}}
	f.define(1, bad)
	f.expectClose()
	evs := r.waitFor("down", fakeTimeout, func(evs []sources.Event) bool { return len(downs(evs, "a")) == 1 })
	if n := count[peersession.TableRejected](evs, "a"); n != 1 {
		t.Fatalf("%d TableRejected events\n%s", n, describe(evs))
	}
	rep := waitSource(t, store, "a", "degraded", inState(snapshot.Degraded))
	if !errors.Is(rep.Tables[0].Rejected, peersession.ErrSchema) || rep.Tables[0].Name != "t_in" {
		t.Fatalf("table not rejected explicitly: %+v", rep)
	}
}

// TestSnapshotOutputEchoNotAdmitted: the source's replay of the output
// tables is acknowledged by the session but never reaches the store.
func TestSnapshotOutputEchoNotAdmitted(t *testing.T) {
	cfg := fastConfig(config.Source{Name: "a"})
	cfg.Outputs = outCfg
	m, _, store, addr := snapshotManager(t, cfg, 16, sources.Options{Output: newStore(t)})
	f := connectFake(t, addr, "a")
	f.established()
	f.define(1, inDef)
	f.define(2, aggDef)
	f.update(aggDef, 1, "2001:db8::", 0)
	f.define(3, metaDef)
	f.update(metaDef, 1, "::", 0)
	f.define(1, inDef)
	f.control(peerwire.ControlResyncFinished)
	rep := waitSource(t, store, "a", "ready", inState(snapshot.Ready))
	if st := status(t, m, "a").Stats; st.EchoedUpdates != 2 {
		t.Fatalf("echoed %d updates, want 2", st.EchoedUpdates)
	}
	if rep.Entries != 0 || rep.Accepted != 0 || rep.Refused != 0 {
		t.Fatalf("output replay reached the store: %+v", rep)
	}
	for _, table := range []string{"t_out", "t_meta", "t_in"} {
		for _, k := range []string{"2001:db8::", "::"} {
			if c := store.Contributions(table, mustKey(t, k)); len(c) != 0 {
				t.Fatalf("%s %s admitted: %+v", table, k, c)
			}
		}
	}
}

// TestSnapshotPartialRetry: a "partial" reply leaves the source syncing
// and the session asks again; the finished reply makes it ready.
func TestSnapshotPartialRetry(t *testing.T) {
	cfg := fastConfig(config.Source{Name: "a"})
	m, r, store, addr := snapshotManager(t, cfg, 16, sources.Options{ResyncRetry: 100 * time.Millisecond})
	f := connectFake(t, addr, "a")
	f.established()
	f.define(1, inDef)
	f.update(inDef, 1, "2001:db8::", 3)
	f.expectAck(1)
	f.control(peerwire.ControlResyncPartial)
	f.expect(peerwire.ClassControl, peerwire.ControlResyncConfirm)
	rep := waitSource(t, store, "a", "partial", func(r snapshot.SourceReport) bool { return r.PartialReplies == 1 })
	if rep.State != snapshot.Syncing {
		t.Fatalf("after a partial reply: %+v", rep)
	}
	start := time.Now()
	f.expect(peerwire.ClassControl, peerwire.ControlResyncRequest)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("retry after %v", d)
	}
	f.update(inDef, 7, "2001:db8::", 4) // the second teach
	f.expectAck(7)
	f.control(peerwire.ControlResyncFinished)
	f.expect(peerwire.ClassControl, peerwire.ControlResyncConfirm)
	waitSource(t, store, "a", "ready", inState(snapshot.Ready))
	if e, ok := store.Lookup("a", "t_in", mustKey(t, "2001:db8::")); !ok || e.Count != 4 {
		t.Fatalf("entry %+v", e)
	}
	if st := status(t, m, "a").Stats; st.ResyncRequests != 2 || st.Learn != peersession.LearnFinished {
		t.Fatalf("stats %+v", st)
	}
	// No further request after "finished".
	if fr, err := f.nonHeartbeat(300 * time.Millisecond); err == nil {
		t.Fatalf("daemon sent %v after the finished reply", fr)
	}
	if n := count[peersession.SyncFinished](r.snapshot(), "a"); n != 2 {
		t.Fatalf("%d resync replies reported", n)
	}
}
