package sources_test

import (
	"context"
	"errors"
	"maps"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/aggregate"
	"github.com/brenc/haproxy-table-aggregator/internal/aggregate/aggregatetest"
	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peersession"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// liveTotals checks diagnostic totals of a live store against the trace
// oracle and the known lab traffic.
type liveTotals struct {
	t      *testing.T
	l      *lab.Lab
	store  *snapshot.Store
	oracle *aggregatetest.Oracle
	roster []string
}

// requests is the ground truth for key (the masked client address as
// HAProxy formats it): requests the responder received from every node's
// lab listener.
func (lt *liveTotals) requests(key string) uint64 {
	var n uint64
	for _, node := range lt.l.Nodes {
		n += uint64(lt.l.Responder.Count(lab.Observation{Node: node.Name, Listener: "lab", Key: key}))
	}
	return n
}

// traffic waits until the complete total of key equals the requests
// the responder received for it, and checks each source's counted value
// against the requests its node forwarded.
func (lt *liveTotals) traffic(what string, key peermsg.Key, haproxyKey string) aggregate.Total {
	lt.t.Helper()
	tot := lt.wait(what, key, lt.requests(haproxyKey))
	for _, c := range tot.Sources {
		n := lt.l.Responder.Count(lab.Observation{Node: c.Source, Listener: "lab", Key: haproxyKey})
		if uint64(c.Count) != uint64(n) || (n > 0) != c.Counted {
			lt.t.Fatalf("%s: source %s contributes %+v, its node forwarded %d requests", what, c.Source, c, n)
		}
	}
	return tot
}

// wait polls until the complete total of key equals want and the oracle,
// replaying the accepted trace at the total's own time, agrees on the
// sum, completeness, uncertainty, and every counted value.
func (lt *liveTotals) wait(what string, key peermsg.Key, want uint64) aggregate.Total {
	lt.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		tot, err := aggregate.Count(lt.store, lab.LabTable, key)
		if err != nil {
			lt.t.Fatal(err)
		}
		x := lt.oracle.Want(lt.roster, lab.LabTable, key, tot.At)
		counted := map[string]uint32{}
		for _, c := range tot.Sources {
			if c.Counted {
				counted[c.Source] = c.Count
			}
		}
		agree := x.Sum == tot.Sum && x.Complete == tot.Complete && x.Uncertain == tot.Uncertain &&
			maps.Equal(x.Counted, counted)
		if tot.Complete && tot.Sum == want && agree {
			lt.t.Logf("%s: total %d of %v, sources %v", what, tot.Sum, key, counted)
			return tot
		}
		if time.Now().After(deadline) {
			lt.t.Fatalf("%s: timed out waiting for a complete total %d of %v agreeing with the oracle; "+
				"total %d complete %v uncertain %v sources %+v; oracle %+v", what, want, key, tot.Sum, tot.Complete,
				tot.Uncertain, tot.Sources, x)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// lastLookup sends n requests and returns HAProxy's http_req_cnt lookup
// after the last one.
func lastLookup(t *testing.T, addr, client string, n int) int64 {
	t.Helper()
	c, err := lab.NewClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseIdle()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rs, err := c.SendN(ctx, addr, client, n)
	if err != nil || len(rs) != n {
		t.Fatalf("traffic to %s: %d responses, %v", addr, len(rs), err)
	}
	return rs[n-1].Lookup
}

// TestLiveCounterTotals sums known two-node HTTP traffic from the stock
// HAProxy nodes' snapshots and checks every total against the requests
// the responder received and against the independent trace oracle: a
// replaced value counts once, simultaneous increments converge, a
// reconnect's full replay does not inflate the total, a key one node
// alone counts stays separate, and the nodes number the input table
// differently. A runtime-API set to the 32-bit boundary sums exactly and
// fails the narrow encoding; the next request wraps HAProxy's counter to
// 0, which lowers the total and marks it uncertain instead of producing a
// 2^32 jump. Finally the single-node key expires and leaves the total.
func TestLiveCounterTotals(t *testing.T) {
	ln := listen(t)
	l := labtest.Start(t, lab.Options{Aggregator: &lab.Aggregator{
		Name:        aggPeer,
		NodeAddrs:   map[string]string{"a": ln.Addr().String(), "b": closedAddr(t)},
		OutputFirst: []string{"b"},
	}})
	a, b := l.Node("a"), l.Node("b")

	const client, client2 = "2001:db8:7::1", "2001:db8:8::1"
	const key, key2 = "2001:db8:7::", "2001:db8:8::"
	k, k2 := mustKey(t, key), mustKey(t, key2)
	sendTraffic(t, a.LabAddr, client, 10)
	sendTraffic(t, b.LabAddr, client, 20)
	sendTraffic(t, b.LabAddr, client2, 5)

	cfg := liveOutputConfig(t, l.Period, config.FileSource{Name: "a"},
		config.FileSource{Name: "b", Address: b.PeersAddr})
	store, err := snapshot.New(snapshot.OptionsFrom(cfg))
	if err != nil {
		t.Fatal(err)
	}
	lt := &liveTotals{
		t: t, l: l, store: store, roster: []string{"a", "b"},
		oracle: &aggregatetest.Oracle{Health: cfg.HealthTimeout, Tables: []string{lab.LabTable}},
	}
	apply := func(ev sources.Event) error {
		err := store.Apply(ev)
		lt.oracle.Record(ev, err)
		return err
	}
	m, rec := startManager(t, sources.Options{Config: cfg, Listener: ln, Apply: apply})
	stop := observeBoth(m, store, lt.oracle)
	defer stop()

	if got, want := lt.requests(key), uint64(30); got != want {
		t.Fatalf("responder saw %d requests for %s, want %d", got, key, want)
	}
	lt.traffic("initial snapshots", k, key)
	lt.traffic("key counted by b alone", k2, key2)
	ids := map[string]peermsg.RemoteTableID{}
	for _, ev := range rec.snapshot() {
		if d, ok := ev.Body.(peersession.TableDefined); ok && d.Definition.Name == lab.LabTable {
			ids[ev.Source] = d.ID
		}
	}
	if ids["a"] == ids["b"] {
		t.Fatalf("both nodes announced %s as table ID %d; the collision case needs different IDs", lab.LabTable,
			ids["a"])
	}
	t.Logf("%s table IDs: a=%d b=%d", lab.LabTable, ids["a"], ids["b"])

	// A=11: the replaced value counts once.
	sendTraffic(t, a.LabAddr, client, 1)
	if tot := lt.traffic("a replaced", k, key); tot.Sum != 31 {
		t.Fatalf("total %d after A=11, want 31", tot.Sum)
	}

	// Both nodes increment once at the same time.
	var wg sync.WaitGroup
	for _, n := range []*lab.Node{a, b} {
		wg.Go(func() {
			c, err := lab.NewClient(nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.CloseIdle()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := c.Send(ctx, n.LabAddr, client); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	lt.traffic("both incremented", k, key)

	// A reconnect replays node a's whole table: no inflation.
	if !m.Disconnect("a") {
		t.Fatal("source a had no session")
	}
	waitRoster(t, store, "a resynchronized in session 2", 20*time.Second, func(r snapshot.Roster) bool {
		return r.Ready && r.Sources[0].Session == 2
	})
	tot := lt.traffic("after a's replay", k, key)
	if tot.Uncertain {
		t.Fatalf("replay made the total uncertain: %+v", tot)
	}
	lt.traffic("b's key after a's replay", k2, key2)

	// The 32-bit boundary, set through node a's runtime API.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := "set table " + lab.LabTable + " key " + key + " data.http_req_cnt " + strconv.FormatUint(math.MaxUint32, 10)
	if out, err := lab.RuntimeCommand(ctx, a.Socket, cmd); err != nil {
		t.Fatalf("%s: %v (%q)", cmd, err, out)
	}
	bCount := uint64(contributionOf(t, tot, "b").Count)
	tot = lt.wait("a at the 32-bit boundary", k, math.MaxUint32+bCount)
	if _, err := tot.Uint32(); !errors.Is(err, aggregate.ErrOverflow) {
		t.Fatalf("narrowing %d: %v, want ErrOverflow", tot.Sum, err)
	}
	if tot.Uncertain {
		t.Fatalf("boundary value made the total uncertain: %+v", tot)
	}
	// One more request: HAProxy's unsigned counter wraps to 0.
	if got := lastLookup(t, a.LabAddr, client, 1); got != 0 {
		t.Fatalf("node a's http_req_cnt after the wrap is %d, want 0", got)
	}
	tot = lt.wait("a wrapped", k, bCount)
	c := contributionOf(t, tot, "a")
	if !tot.Uncertain || !c.Discontinuous || c.LastDecrease.From != math.MaxUint32 || c.LastDecrease.To != 0 {
		t.Fatalf("wrap: total %+v, a %+v", tot, c)
	}
	t.Logf("wrap: total %d uncertain, a's last decrease %d -> %d", tot.Sum, c.LastDecrease.From,
		c.LastDecrease.To)

	// Node b's other key expires with no further traffic and leaves the
	// total once. One more request on node b first keeps its entry for
	// the first key alive past that expiry: when both nodes answer the
	// first resync "finished", the two entries' deadlines can fall within
	// the 100 ms margin below, and both would expire.
	sendTraffic(t, b.LabAddr, client, 1)
	lt.wait("b refreshed the first key", k, bCount+1)
	e, ok := store.Lookup("b", lab.LabTable, k2)
	if !ok {
		t.Fatal("b's entry for the second key expired early")
	}
	expiredBefore := sourceReport(t, store, "b").Expired
	time.Sleep(time.Until(e.Deadline) + 100*time.Millisecond)
	tot = lt.wait("b's key expired", k2, 0)
	if c := contributionOf(t, tot, "b"); c.Present {
		t.Fatalf("expired entry still present: %+v", c)
	}
	if n := sourceReport(t, store, "b").Expired - expiredBefore; n != 1 {
		t.Fatalf("source b expired %d entries, want 1", n)
	}
	lt.wait("b's key still expired", k2, 0)
	if dup, missing := l.Responder.Anomalies(); len(dup) != 0 || missing != 0 {
		t.Fatalf("responder anomalies: %d duplicates, %d without ID", len(dup), missing)
	}
}

func contributionOf(t *testing.T, tot aggregate.Total, src string) aggregate.Contribution {
	t.Helper()
	for _, c := range tot.Sources {
		if c.Source == src {
			return c
		}
	}
	t.Fatalf("no source %s in %+v", src, tot.Sources)
	return aggregate.Contribution{}
}

func sourceReport(t *testing.T, store *snapshot.Store, src string) snapshot.SourceReport {
	t.Helper()
	r, ok := store.Source(src)
	if !ok {
		t.Fatalf("no source %s", src)
	}
	return r
}

// observeBoth is observeLoop that also records every observation in the
// oracle.
func observeBoth(m *sources.Manager, store *snapshot.Store, o *aggregatetest.Oracle) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			for _, st := range m.Status() {
				if st.Up {
					store.Observe(st.Name, st.Session, st.Stats.LastRx)
					o.Observe(st.Name, st.Session, st.Stats.LastRx)
				}
			}
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
