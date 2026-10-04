package sources_test

import (
	"sync"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// observeLoop feeds every session's last message read into the store
// until the returned function is called, as htad does.
func observeLoop(m *sources.Manager, store *snapshot.Store) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			store.ObserveStatus(m.Status())
			select {
			case <-done:
				return
			case <-tick.C:
			}
		}
	})
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		wg.Wait()
	}
}

// waitRoster polls the store until pred holds for its roster.
func waitRoster(t *testing.T, store *snapshot.Store, what string, timeout time.Duration,
	pred func(snapshot.Roster) bool,
) snapshot.Roster {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		r := store.Roster()
		if pred(r) {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s; roster %+v", timeout, what, r)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// contributionsOf maps each source to its entry's count for key.
func contributionsOf(store *snapshot.Store, key peermsg.Key) map[string]uint32 {
	out := map[string]uint32{}
	for _, c := range store.Contributions(lab.LabTable, key) {
		out[c.Source] = c.Entry.Count
	}
	return out
}

// TestLiveSnapshots runs the snapshot store against the two stock HAProxy
// nodes: both report one key and keep separate snapshots; the roster
// becomes ready only once both have synchronized, and stays ready across a
// quiet period longer than the health timeout (heartbeats only); a live
// update replaces only its source's value; and a reconnect's resync, which
// replays the node's copy of the published output, admits no output into
// the store.
func TestLiveSnapshots(t *testing.T) {
	ln := listen(t)
	l := labtest.Start(t, lab.Options{Aggregator: &lab.Aggregator{
		Name:      aggPeer,
		NodeAddrs: map[string]string{"a": ln.Addr().String(), "b": closedAddr(t)},
	}})
	a, b := l.Node("a"), l.Node("b")

	// Both nodes count the same key: 10 requests on a, 20 on b.
	const client = "2001:db8:5::1"
	k := mustKey(t, "2001:db8:5::")
	sendTraffic(t, a.LabAddr, client, 10)
	sendTraffic(t, b.LabAddr, client, 20)

	cfg := liveOutputConfig(t, l.Period, config.FileSource{Name: "a"},
		config.FileSource{Name: "b", Address: b.PeersAddr})
	store, err := snapshot.New(snapshot.OptionsFrom(cfg))
	if err != nil {
		t.Fatal(err)
	}
	// Output published for the input key and another, so that the
	// nodes hold copies to replay.
	out, err := output.NewStore(cfg.OutputTables(), output.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	outKey := mustKey(t, "2001:db8:6::")
	for _, key := range []peermsg.Key{k, outKey} {
		if err := out.Set(lab.OutputTable, key, output.AggregateValues(77)); err != nil {
			t.Fatal(err)
		}
	}
	m, _ := startManager(t, sources.Options{Config: cfg, Listener: ln, Output: out, Apply: store.Apply})
	stop := observeLoop(m, store)
	defer stop()

	start := time.Now()
	r := waitRoster(t, store, "roster ready", 20*time.Second, func(r snapshot.Roster) bool { return r.Ready })
	t.Logf("roster ready %v after start: %+v", time.Since(start).Round(time.Millisecond), r.Sources)
	if got := contributionsOf(store, k); len(got) != 2 || got["a"] != 10 || got["b"] != 20 {
		t.Fatalf("contributions %v, want a=10 b=20", got)
	}
	for _, s := range r.Sources {
		if s.Session != 1 || s.Entries != 1 || s.Refused != 0 || !s.Tables[0].Defined {
			t.Fatalf("source %+v", s)
		}
	}

	// Quiet: no traffic for longer than the health timeout; heartbeats
	// alone keep both sources healthy and the roster ready.
	quiet := cfg.HealthTimeout + 2*time.Second
	for end := time.Now().Add(quiet); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
		if now := store.Roster(); !now.Ready {
			t.Fatalf("roster lost readiness while quiet: %+v", now.NotReady())
		}
	}
	for _, src := range []string{"a", "b"} {
		st := status(t, m, src)
		rep, _ := store.Source(src)
		t.Logf("source %s after %v quiet: last read %v ago, %d heartbeats received, state %v",
			src, quiet, time.Since(rep.LastRx).Round(time.Millisecond), st.Stats.RxHeartbeats, rep.State)
		if st.Stats.RxHeartbeats == 0 {
			t.Fatalf("source %s sent no heartbeat while quiet", src)
		}
	}

	// A live update replaces only node a's value.
	sendTraffic(t, a.LabAddr, client, 1)
	waitRoster(t, store, "a=11", 10*time.Second, func(snapshot.Roster) bool {
		got := contributionsOf(store, k)
		return got["a"] == 11 && got["b"] == 20
	})

	// Reconnect a: the new session's resync replays lab_in and the
	// node's copy of the output. The store keeps one entry for a, now
	// delivered by session 2, and nothing from the output tables.
	if !m.Disconnect("a") {
		t.Fatal("source a had no session")
	}
	waitRoster(t, store, "a resynchronized in session 2", 20*time.Second, func(r snapshot.Roster) bool {
		return r.Ready && r.Sources[0].Session == 2
	})
	deadline := time.Now().Add(15 * time.Second)
	for status(t, m, "a").Stats.EchoedUpdates == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("node a's replay carried no output entry: %+v", status(t, m, "a").Stats)
		}
		time.Sleep(20 * time.Millisecond)
	}
	e, ok := store.Lookup("a", lab.LabTable, k)
	if !ok || e.Count != 11 || e.Session != 2 {
		t.Fatalf("source a entry after the resync: %+v %v", e, ok)
	}
	for _, table := range []string{lab.OutputTable, lab.MetaTable} {
		for _, key := range []peermsg.Key{k, outKey, output.MetadataKey} {
			if c := store.Contributions(table, key); len(c) != 0 {
				t.Fatalf("%s %v admitted: %+v", table, key, c)
			}
		}
	}
	r = store.Roster()
	for _, s := range r.Sources {
		if s.Entries != 1 || s.Refused != 0 {
			t.Fatalf("source %s: %d entries, %d refused after the replay", s.Name, s.Entries, s.Refused)
		}
	}
	t.Logf("source a session 2: %d output entries replayed by the node and discarded; store %+v",
		status(t, m, "a").Stats.EchoedUpdates, r.Sources)
}
