package sources_test

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// liveOutputConfig is liveConfig plus the lab's two output tables.
func liveOutputConfig(t *testing.T, period time.Duration, srcs ...config.FileSource) config.Config {
	t.Helper()
	d := func(v time.Duration) *config.Duration { c := config.Duration(v); return &c }
	expire := d(lab.ExpiryFactor * period)
	cfg, err := config.File{
		LocalPeer:                    aggPeer,
		InsecurePlaintextLoopbackLab: true,
		Listen:                       "127.0.0.1:0",
		Sources:                      srcs,
		Tables:                       []config.FileTable{{Name: lab.LabTable, Period: d(period)}},
		Outputs: []config.FileOutput{
			{Name: lab.OutputTable, Kind: "aggregate", Expire: expire},
			{Name: lab.MetaTable, Kind: "metadata", Expire: d(lab.MetaExpire)},
		},
		IdleTimeout:  d(config.MinIdleTimeout),
		ReconnectMin: d(50 * time.Millisecond),
		ReconnectMax: d(time.Second),
	}.Validate()
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// probeCase is one key the output probe looks up.
type probeCase struct {
	client string // the X-Lab-Client address
	key    peermsg.Key
}

// waitProbe polls a node's probe listener until pred holds.
func waitProbe(t *testing.T, n *lab.Node, client, what string, pred func(lab.ProbeResponse) bool) lab.ProbeResponse {
	t.Helper()
	c, err := lab.NewClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseIdle()
	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r, err := c.Probe(ctx, n.ProbeAddr, client)
		cancel()
		if err != nil {
			t.Fatalf("node %s probe %s: %v", n.Name, client, err)
		}
		if pred(r) {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s: timed out waiting for %s; last probe %+v", n.Name, what, r)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// renewLease keeps the store's lease running for the longest lease
// (output.MaxLeaseLength) from each renewal, renewing every quarter
// lease, until the returned function is called.
// That returns when the last renewal was set: the last valid
// publication.
func renewLease(t *testing.T, store *output.Store) (stop func() time.Time) {
	t.Helper()
	const lease = output.MaxLeaseLength
	var mu sync.Mutex
	last := time.Now()
	if err := store.SetLease(last.Add(lease)); err != nil {
		t.Fatal(err)
	}
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		tick := time.NewTicker(lease / 4)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				mu.Lock()
				last = time.Now()
				err := store.SetLease(last.Add(lease))
				mu.Unlock()
				if err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	var once sync.Once
	return func() time.Time {
		once.Do(func() { close(done) })
		<-stopped
		mu.Lock()
		defer mu.Unlock()
		return last
	}
}

// expectRate is a probe predicate for a published aggregate rate under a
// live lease: schema version 2, the rate, aggregate authority, and the
// ACL decision against lab.OutputLimit.
func expectRate(rate uint32) func(lab.ProbeResponse) bool {
	status := http.StatusOK
	if rate >= lab.OutputLimit {
		status = http.StatusTooManyRequests
	}
	return func(r lab.ProbeResponse) bool {
		return r.Version == output.SchemaVersion && r.Rate == int64(rate) && r.Status == status &&
			r.MetaVersion == output.SchemaVersion && r.Aggregate()
	}
}

// outStats returns a source's session statistics for one output table.
func outStats(t *testing.T, m *sources.Manager, source, table string) peersession.OutputStats {
	t.Helper()
	for _, o := range status(t, m, source).Stats.Outputs {
		if o.Table == table {
			return o
		}
	}
	t.Fatalf("source %s: no output stats for %s", source, table)
	return peersession.OutputStats{}
}

// inputCounts returns the lab_in http_req_cnt of every entry on a node.
func inputCounts(t *testing.T, n *lab.Node) map[string]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tbl, err := n.ShowTable(ctx, lab.LabTable)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, e := range tbl.Entries {
		out[e.Key] = e.Data["http_req_cnt"]
	}
	return out
}

// TestLiveOutput publishes known integers into both stock HAProxy nodes'
// output tables over the ordinary peers protocol and reads them back with
// ordinary table_gpt lookups and an ACL: a full teach when the sessions
// start, live changes without a reconnect or resync, input tables left
// untouched, the nodes' replay of the output never taken as input, and
// table IDs that collide across namespaces routed correctly.
func TestLiveOutput(t *testing.T) {
	ln := listen(t)
	l := labtest.Start(t, lab.Options{Aggregator: &lab.Aggregator{
		Name:      aggPeer,
		NodeAddrs: map[string]string{"a": ln.Addr().String(), "b": closedAddr(t)},
		// Node b numbers its tables differently from node a.
		OutputFirst: []string{"b"},
	}})
	a, b := l.Node("a"), l.Node("b")
	nodes := []*lab.Node{a, b}

	// Input traffic before the aggregator exists: lab_in counts 10 on
	// node a and 20 on node b.
	keyA, keyB := mustKey(t, "2001:db8:a::"), mustKey(t, "2001:db8:b::")
	sendTraffic(t, a.LabAddr, "2001:db8:a::1", 10)
	sendTraffic(t, b.LabAddr, "2001:db8:b::1", 20)
	baseline := map[string]map[string]int64{"a": inputCounts(t, a), "b": inputCounts(t, b)}
	if baseline["a"][keyA.String()] != 10 || baseline["b"][keyB.String()] != 20 || len(baseline["a"]) != 1 ||
		len(baseline["b"]) != 1 {
		t.Fatalf("input baseline %v", baseline)
	}
	responderBefore := responderTotal(l)

	// Output published before any session exists, so only the teach at
	// session start can deliver it.
	cfg := liveOutputConfig(t, l.Period,
		config.FileSource{Name: "a"},
		config.FileSource{Name: "b", Address: b.PeersAddr})
	store, err := output.NewStore(cfg.OutputTables(), output.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]probeCase{
		"a":    {"2001:db8:a::1", keyA},
		"b":    {"2001:db8:b::1", keyB},
		"v4":   {"192.0.2.7", mustKey(t, "::ffff:192.0.2.7")},
		"new":  {"2001:db8:c::1", mustKey(t, "2001:db8:c::")},
		"none": {"2001:db8:d::1", mustKey(t, "2001:db8:d::")},
	}
	rates := map[string]uint32{"a": 400, "b": 1500, "v4": 7}
	for name, r := range rates {
		if err := store.Set(lab.OutputTable, cases[name].key, output.AggregateValues(r)); err != nil {
			t.Fatal(err)
		}
	}
	const initialEntries = 3

	m, r := startManager(t, sources.Options{Config: cfg, Listener: ln, Output: store})
	// Keep the output authoritative so that the ACL applies it.
	defer renewLease(t, store)()
	r.waitFor("both sessions up, resynced, and inputs replayed", 20*time.Second, func(evs []sources.Event) bool {
		return resyncedSessions(evs, "a", true) == 1 && resyncedSessions(evs, "b", true) == 1 &&
			countIs("a", keyA, 10)(evs) && countIs("b", keyB, 20)(evs)
	})

	// Full teach: both nodes' ordinary lookups and ACL see every value.
	start := time.Now()
	for _, n := range nodes {
		for name, rate := range rates {
			p := waitProbe(t, n, cases[name].client, name+" taught", expectRate(rate))
			t.Logf("node %s probe %s (%s): %+v", n.Name, name, cases[name].key, p)
		}
		p := waitProbe(t, n, cases["none"].client, "unpublished key reads 0", func(p lab.ProbeResponse) bool {
			return p.Version == 0 && p.Rate == 0 && p.Status == http.StatusOK && p.MetaVersion == output.SchemaVersion
		})
		t.Logf("node %s probe none (%s): %+v", n.Name, cases["none"].key, p)
	}
	t.Logf("taught output visible on both nodes (checked %v after the sessions resynced)",
		time.Since(start).Round(time.Millisecond))
	taught := map[string]peersession.Stats{}
	for _, src := range []string{"a", "b"} {
		s := status(t, m, src)
		st := s.Stats
		// Each table is taught when the node announces it (in its teach
		// answering the daemon's resync request, even while empty), and
		// the node's own resync request gets a teach of whatever it had
		// announced by then.
		if s.Session != 1 || st.Teaches < 2 || st.TaughtUpdates < initialEntries ||
			st.OutputUpdates != st.TaughtUpdates {
			t.Fatalf("source %s after the teach: session %d, %d teaches, %d taught of %d updates",
				src, s.Session, st.Teaches, st.TaughtUpdates, st.OutputUpdates)
		}
		taught[src] = st
		for _, o := range st.Outputs {
			if o.SourceID == 0 {
				t.Fatalf("source %s never announced %s, yet its values arrived", src, o.Table)
			}
		}
		t.Logf("source %s: %d teach(es) (one per table the node announced, one per resync request), %d updates; outputs %+v",
			src, st.Teaches, st.TaughtUpdates, st.Outputs)
	}
	peersBefore := map[string]peerState{"a": showPeer(t, a), "b": showPeer(t, b)}

	// Live change: values cross the ACL limit in both directions and a
	// new key appears, with no reconnect and no resync.
	live := map[string]uint32{"a": 1200, "b": 50, "new": 5}
	start = time.Now()
	for name, rate := range live {
		if err := m.Publish(lab.OutputTable, cases[name].key, output.AggregateValues(rate)); err != nil {
			t.Fatal(err)
		}
		rates[name] = rate
	}
	for _, n := range nodes {
		for name, rate := range live {
			waitProbe(t, n, cases[name].client, name+" live change", expectRate(rate))
		}
	}
	t.Logf("live change visible on both nodes after %v", time.Since(start).Round(time.Millisecond))
	evs := r.snapshot()
	for _, src := range []string{"a", "b"} {
		s := status(t, m, src)
		st, before := s.Stats, taught[src]
		if !s.Up || s.Session != 1 || len(downs(evs, src)) != 0 || len(ups(evs, src)) != 1 {
			t.Fatalf("source %s reconnected during the live change: %+v", src, s)
		}
		if st.Teaches != before.Teaches || st.TaughtUpdates != before.TaughtUpdates ||
			st.OutputUpdates-before.OutputUpdates != uint64(len(live)) {
			t.Fatalf("source %s: live change sent %d updates and %d teaches, want %d updates and no teach",
				src, st.OutputUpdates-before.OutputUpdates, st.Teaches-before.Teaches, len(live))
		}
		n := l.Node(src)
		after := showPeer(t, n)
		if after.Fields["new_conn"] != peersBefore[src].Fields["new_conn"] || after.Status != "ESTA" {
			t.Fatalf("node %s: new_conn %s -> %s, status %s", src, peersBefore[src].Fields["new_conn"],
				after.Fields["new_conn"], after.Status)
		}
		// HAProxy acknowledges the live updates under the ID this side
		// announced for lab_out.
		deadline := time.Now().Add(15 * time.Second)
		for o := outStats(t, m, src, lab.OutputTable); !o.Acked || o.LastAcked != o.LastSent; o = outStats(t, m, src, lab.OutputTable) {
			if time.Now().After(deadline) {
				t.Fatalf("source %s: lab_out not acknowledged up to the last update: %+v", src, o)
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Logf("source %s live: %d updates, no teach; outputs %+v; node new_conn %s", src,
			st.OutputUpdates-before.OutputUpdates, status(t, m, src).Stats.Outputs, after.Fields["new_conn"])
	}

	// Isolation: publishing and probing changed no input table, HAProxy
	// never received a definition or update for an input table from the
	// aggregator, and only lab_in produced input events.
	for _, n := range nodes {
		if got := inputCounts(t, n); len(got) != len(baseline[n.Name]) || got[keyA.String()] != baseline[n.Name][keyA.String()] ||
			got[keyB.String()] != baseline[n.Name][keyB.String()] {
			t.Errorf("node %s lab_in changed: %v, before %v", n.Name, got, baseline[n.Name])
		}
		st := showPeer(t, n)
		for _, in := range []string{lab.LabTable, lab.ProdTable, lab.ProxyTable} {
			if c := st.Tables[in]; c["remote_id"] != "0" || c["last_get"] != "0" {
				t.Errorf("node %s %s: the aggregator announced or updated an input table: %v", n.Name, in, c)
			}
		}
		t.Logf("node %s lab_in %v unchanged; shared tables %v", n.Name, inputCounts(t, n), st.Tables)
	}
	if got := responderTotal(l); got != responderBefore {
		t.Errorf("responder saw %d requests during output checks, want %d", got, responderBefore)
	}
	checkInputOnly(t, r.snapshot())

	// Table IDs: each namespace routes by its own numbering. HAProxy's
	// IDs for its tables (local_id) differ per node; the aggregator's
	// IDs for its outputs are the same on both. A number HAProxy uses for
	// one table and the aggregator for another must not mix them up.
	collisions := 0
	for _, n := range nodes {
		st := showPeer(t, n)
		theirs := map[string]string{} // HAProxy local_id -> table
		for name, c := range st.Tables {
			theirs[c["local_id"]] = name
		}
		for _, out := range status(t, m, n.Name).Stats.Outputs {
			c := st.Tables[out.Table]
			if c["remote_id"] != strconv.FormatUint(uint64(out.LocalID), 10) ||
				c["local_id"] != strconv.FormatUint(uint64(out.SourceID), 10) {
				t.Errorf("node %s %s: HAProxy has local_id %s remote_id %s; aggregator has local %d, source %d",
					n.Name, out.Table, c["local_id"], c["remote_id"], out.LocalID, out.SourceID)
			}
			if other := theirs[strconv.FormatUint(uint64(out.LocalID), 10)]; other != "" && other != out.Table {
				collisions++
				t.Logf("node %s: aggregator ID %d is %s; HAProxy ID %d is %s", n.Name, out.LocalID, out.Table,
					out.LocalID, other)
			}
		}
	}
	if collisions == 0 {
		t.Error("no table-ID collision between the namespaces; the check proved nothing")
	}
	// Across sources: the same remote ID names node a's input lab_in and
	// node b's copy of lab_out. Only the former produced input events.
	inA := showPeer(t, a).Tables[lab.LabTable]["local_id"]
	outB := outStats(t, m, "b", lab.OutputTable).SourceID
	if inA != strconv.FormatUint(uint64(outB), 10) || inA == showPeer(t, b).Tables[lab.LabTable]["local_id"] {
		t.Errorf("node a numbers lab_in %s, node b numbers lab_out %d and lab_in %s; want a shared ID",
			inA, outB, showPeer(t, b).Tables[lab.LabTable]["local_id"])
	}
	t.Logf("remote ID %s is lab_in (input) from source a and lab_out (output copy) from source b", inA)

	// Replay: new sessions make each node replay its tables, including
	// its copy of the output. The copy is acknowledged and discarded:
	// no event, no input contribution. Each new session teaches the full
	// output again.
	for _, src := range []string{"a", "b"} {
		if !m.Disconnect(src) {
			t.Fatalf("source %s had no session to disconnect", src)
		}
	}
	r.waitFor("second sessions up, resynced, and inputs replayed", 20*time.Second, func(evs []sources.Event) bool {
		return resyncedSessions(evs, "a", true) == 2 && resyncedSessions(evs, "b", true) == 2 &&
			replayed(evs, "a", keyA, 10) && replayed(evs, "b", keyB, 20)
	})
	for _, src := range []string{"a", "b"} {
		deadline := time.Now().Add(15 * time.Second)
		for status(t, m, src).Stats.EchoedUpdates == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("source %s: the node's replay carried no output entry: %+v", src, status(t, m, src).Stats)
			}
			time.Sleep(20 * time.Millisecond)
		}
		s := status(t, m, src)
		if s.Session != 2 || s.Stats.Teaches < 2 || s.Stats.TaughtUpdates < initialEntries+1 ||
			s.Stats.OutputUpdates != s.Stats.TaughtUpdates {
			t.Errorf("source %s session %d: %d teaches, %d taught updates", src, s.Session, s.Stats.Teaches,
				s.Stats.TaughtUpdates)
		}
		t.Logf("source %s session 2: %d output entries replayed by the node and discarded; %d teach(es) of %d updates",
			src, s.Stats.EchoedUpdates, s.Stats.Teaches, s.Stats.TaughtUpdates)
	}
	for _, n := range nodes {
		for name, rate := range rates {
			waitProbe(t, n, cases[name].client, name+" after reconnect", expectRate(rate))
		}
		if got := inputCounts(t, n); got[keyA.String()] != baseline[n.Name][keyA.String()] ||
			got[keyB.String()] != baseline[n.Name][keyB.String()] {
			t.Errorf("node %s lab_in changed after replay: %v", n.Name, got)
		}
	}
	evs = r.snapshot()
	checkInputOnly(t, evs)
	contrib := contributions(evs)
	want := map[string]map[peermsg.Key]uint32{"a": {keyA: 10}, "b": {keyB: 20}}
	for src, keys := range want {
		if len(contrib[src]) != len(keys) || contrib[src][keyA] != keys[keyA] || contrib[src][keyB] != keys[keyB] {
			t.Errorf("source %s contributions %v, want %v", src, contrib[src], keys)
		}
	}
}

// replayed holds once the source's latest session replayed key's count.
func replayed(evs []sources.Event, source string, key peermsg.Key, want uint32) bool {
	var session uint64
	for _, ev := range evs {
		if ev.Source == source && ev.Session > session {
			session = ev.Session
		}
	}
	for _, ev := range evs {
		if u, ok := ev.Body.(peersession.EntryUpdated); ok && ev.Source == source && ev.Session == session &&
			u.Table == lab.LabTable && u.Update.Key == key && reqCnt(u) == want {
			return true
		}
	}
	return false
}

// checkInputOnly fails if any table event names a table other than the
// configured input.
func checkInputOnly(t *testing.T, evs []sources.Event) {
	t.Helper()
	for _, ev := range evs {
		switch b := ev.Body.(type) {
		case peersession.TableDefined:
			if b.Definition.Name != lab.LabTable {
				t.Errorf("%s#%d: definition event for %s", ev.Source, ev.Session, b.Definition.Name)
			}
		case peersession.EntryUpdated:
			if b.Table != lab.LabTable {
				t.Errorf("%s#%d: update event for %s key %v", ev.Source, ev.Session, b.Table, b.Update.Key)
			}
		}
	}
}

// contributions is each source's latest lab_in count per key: what an
// aggregator would sum.
func contributions(evs []sources.Event) map[string]map[peermsg.Key]uint32 {
	out := map[string]map[peermsg.Key]uint32{}
	for _, ev := range evs {
		if u, ok := ev.Body.(peersession.EntryUpdated); ok && u.Table == lab.LabTable {
			if out[ev.Source] == nil {
				out[ev.Source] = map[peermsg.Key]uint32{}
			}
			out[ev.Source][u.Update.Key] = reqCnt(u)
		}
	}
	return out
}

func responderTotal(l *lab.Lab) int {
	n := 0
	for _, c := range l.Responder.Counts() {
		n += c
	}
	return n
}

// TestLiveOutputNeedsMatchingTable configures outputs whose names the node
// shares with the aggregator but with another schema: prod_in, an input
// table the daemon does not consume, and lab_out with another expire. The
// daemon must not write either before the node's own definition ends the
// session with ErrSchema, however often the session is retried: stock
// HAProxy would apply the entry to any table of that name with IPv6 keys.
func TestLiveOutputNeedsMatchingTable(t *testing.T) {
	l := labtest.Start(t, lab.Options{Aggregator: &lab.Aggregator{
		Name: aggPeer, NodeAddrs: map[string]string{"a": closedAddr(t), "b": closedAddr(t)},
	}})
	a := l.Node("a")
	key := mustKey(t, "2001:db8:e::")
	d := func(v time.Duration) *config.Duration { c := config.Duration(v); return &c }
	for _, tc := range []struct {
		table  string
		expire time.Duration
	}{
		{lab.ProdTable, lab.ExpiryFactor * l.Period},
		{lab.OutputTable, 2 * lab.ExpiryFactor * l.Period},
	} {
		t.Run(tc.table, func(t *testing.T) {
			cfg, err := config.File{
				LocalPeer:                    aggPeer,
				InsecurePlaintextLoopbackLab: true,
				Sources:                      []config.FileSource{{Name: "a", Address: a.PeersAddr}},
				Tables:                       []config.FileTable{{Name: lab.LabTable, Period: d(l.Period)}},
				Outputs:                      []config.FileOutput{{Name: tc.table, Kind: "aggregate", Expire: d(tc.expire)}},
				IdleTimeout:                  d(config.MinIdleTimeout),
				ReconnectMin:                 d(50 * time.Millisecond),
				ReconnectMax:                 d(200 * time.Millisecond),
			}.Validate()
			if err != nil {
				t.Fatal(err)
			}
			store, err := output.NewStore(cfg.OutputTables(), output.StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if serr := store.Set(tc.table, key, output.AggregateValues(5)); serr != nil {
				t.Fatal(serr)
			}
			_, r := startManager(t, sources.Options{Config: cfg, Output: store})
			evs := r.waitFor("three sessions refused by schema", 20*time.Second, func(evs []sources.Event) bool {
				n := 0
				for _, d := range downs(evs, "a") {
					if errors.Is(d.Err, peersession.ErrSchema) {
						n++
					}
				}
				return n >= 3
			})
			t.Logf("first session end: %v", downs(evs, "a")[0].Err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tbl, err := a.ShowTable(ctx, tc.table)
			if err != nil {
				t.Fatal(err)
			}
			if e, ok := tbl.Entry(key.String()); ok {
				t.Fatalf("node a %s holds the output key after %d sessions: %+v", tc.table, len(ups(evs, "a")), e)
			}
			t.Logf("node a %s after %d refused sessions: %d entries, none for %v", tc.table, len(ups(evs, "a")),
				len(tbl.Entries), key)
		})
	}
}
