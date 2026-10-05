package sources_test

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/aggregate"
	"github.com/brenc/haproxy-table-aggregator/internal/aggregate/aggregatetest"
	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
	"github.com/brenc/haproxy-table-aggregator/internal/rate"
	"github.com/brenc/haproxy-table-aggregator/internal/snapshot"
	"github.com/brenc/haproxy-table-aggregator/internal/sources"
)

// Comparison slack between an evaluated reading and HAProxy's own: the
// evaluator lags the source by the transport delay (up to rateLagSlack
// on loopback, including session scheduling), and HAProxy's even tick
// and millisecond clock differ from the local clock by a few ms
// (rateClockSlack).
const (
	rateLagSlack   = 50 * time.Millisecond
	rateClockSlack = 5 * time.Millisecond
)

// rateSend is one scheduled batch of requests, at a fraction of the
// period after the start.
type rateSend struct {
	at   float64
	node string
	n    int
}

// rateCase is one live comparison: a traffic schedule on both nodes and
// how long to sample, in periods. full also waits for decay to 0 and for
// both entries to expire.
type rateCase struct {
	period   time.Duration
	sends    []rateSend
	sampleTo float64
	step     float64
	full     bool
}

// TestLiveRates compares the evaluated per-source http_req_rate of
// stock HAProxy nodes with each node's own reading ("show table") at
// controlled times, sums the sources at common times against the trace
// oracle, and quantifies the estimate's error against the exact
// sliding-window count of timestamped requests. The nodes' windows start
// at different times and see different histories. At 10 s it runs
// through rotation and idle decay to 0 with no new peer updates, then
// entry expiry with no resurrection; at 60 s it is a targeted comparison
// through the first rotation and the proportional decay of a previous
// period, but not the 2-period idle decay, which would take over two
// minutes (that and every other semantic case runs at 60 s with an
// injected clock in packages rate and aggregate).
func TestLiveRates(t *testing.T) {
	cases := []rateCase{
		{
			period: 10 * time.Second,
			sends: append([]rateSend{{0, "a", 12}, {0.37, "b", 1}, {0.9, "b", 6}},
				steadySends("a", 0.2, 1.45, 0.1)...),
			sampleTo: 3.3, step: 0.05, full: true,
		},
		{
			period: 60 * time.Second,
			sends: append(append([]rateSend{{0, "a", 12}, {0.04, "b", 1}, {0.09, "b", 6}},
				steadySends("a", 0.02, 0.12, 0.02)...), steadySends("a", 1.02, 1.1, 0.03)...),
			sampleTo: 1.3, step: 0.02,
		},
	}
	for _, c := range cases {
		t.Run(c.period.String(), func(t *testing.T) {
			t.Parallel()
			liveRates(t, c)
		})
	}
}

func steadySends(node string, from, to, step float64) []rateSend {
	var out []rateSend
	for i := 0; from+float64(i)*step < to; i++ {
		out = append(out, rateSend{from + float64(i)*step, node, 1})
	}
	return out
}

func liveRates(t *testing.T, rc rateCase) {
	ln := listen(t)
	l := labtest.Start(t, lab.Options{Period: rc.period, Aggregator: &lab.Aggregator{
		Name:      aggPeer,
		NodeAddrs: map[string]string{"a": ln.Addr().String(), "b": closedAddr(t)},
	}})
	cfg := liveOutputConfig(t, l.Period, config.FileSource{Name: "a"},
		config.FileSource{Name: "b", Address: l.Node("b").PeersAddr})
	store, err := snapshot.New(snapshot.OptionsFrom(cfg))
	if err != nil {
		t.Fatal(err)
	}
	oracle := &aggregatetest.Oracle{Health: cfg.HealthTimeout, Tables: []string{lab.LabTable}}
	apply := func(ev sources.Event) error {
		applyErr := store.Apply(ev)
		oracle.Record(ev, applyErr)
		return applyErr
	}
	m, _ := startManager(t, sources.Options{Config: cfg, Listener: ln, Apply: apply})
	stop := observeBoth(m, store, oracle)
	defer stop()
	waitRoster(t, store, "both sources ready", 20*time.Second, func(r snapshot.Roster) bool { return r.Ready })

	const client, haKey = "2001:db8:20::1", "2001:db8:20::"
	k := mustKey(t, haKey)
	pms := uint32(rc.period / time.Millisecond)
	field := fmt.Sprintf("http_req_rate(%d)", pms)
	nodes := []string{"a", "b"}
	c, err := lab.NewClient(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseIdle()
	sent := map[string][]time.Time{} // response times per node
	periods := func(f float64) time.Duration { return time.Duration(f * float64(rc.period)) }

	// synced waits until the store holds each node's latest count, so
	// that every request sent so far is in the evaluated counter.
	synced := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			ok := true
			for _, n := range nodes {
				e, found := store.Lookup(n, lab.LabTable, k)
				if want := len(sent[n]); (want > 0 || found) && (!found || int(e.Count) != want) {
					ok = false
				}
			}
			if ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("store did not receive the latest counts: sent a %d, b %d", len(sent["a"]), len(sent["b"]))
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	var samples, exactMatches, compared int
	var maxErr, sumAbsErr float64
	// sample compares every source's evaluated reading with its node's
	// own at about the same time, and the sum with the oracle and with
	// the exact sliding-window count.
	sample := func(label string) aggregate.RateTotal {
		t.Helper()
		synced()
		for _, n := range nodes {
			node := l.Node(n)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			q0 := time.Now()
			tbl, showErr := node.ShowTable(ctx, lab.LabTable)
			q1 := time.Now()
			cancel()
			if showErr != nil {
				t.Fatalf("%s: node %s: %v", label, n, showErr)
			}
			var native int64
			nEntry, onNode := tbl.Entry(haKey)
			if onNode {
				native = nEntry.Data[field]
			}
			e, inStore := store.Lookup(n, lab.LabTable, k)
			if !inStore {
				if onNode && native != 0 {
					t.Fatalf("%s: node %s reads %d for a key the store no longer holds", label, n, native)
				}
				continue
			}
			ctr := rate.Counter{Value: e.Rate, Period: e.Period, Received: e.Received}
			hi, lo := ctr.At(q0.Add(-rateClockSlack)), ctr.At(q1.Add(rateLagSlack))
			mid := ctr.At(q0.Add(q1.Sub(q0) / 2))
			if native < 0 || uint64(native) > hi || uint64(native) < lo {
				t.Fatalf("%s: node %s reads %d, evaluated %d (bounds [%d, %d]) for %+v received %v ago",
					label, n, native, mid, lo, hi, e.Rate, time.Since(e.Received))
			}
			compared++
			if uint64(native) == mid {
				exactMatches++
			} else {
				t.Logf("%s: node %s reads %d, evaluated %d at the query midpoint (bounds [%d, %d], query %v)",
					label, n, native, mid, lo, hi, q1.Sub(q0))
			}
		}
		r, rateErr := aggregate.Rate(store, lab.LabTable, k)
		if rateErr != nil {
			t.Fatal(rateErr)
		}
		x := oracle.WantRate([]string{"a", "b"}, lab.LabTable, k, pms, r.At)
		counted := map[string]uint64{}
		for _, c := range r.Sources {
			if c.Counted {
				counted[c.Source] = c.Estimate
			}
		}
		if !r.Complete || r.Sum != x.Sum || !x.Complete || !maps.Equal(counted, x.Counted) || r.Uncertain {
			t.Fatalf("%s: rate %+v, oracle %+v", label, r, x)
		}
		var exact, twoPeriods uint64
		for _, n := range nodes {
			for _, at := range sent[n] {
				if age := r.At.Sub(at); age >= 0 && age < rc.period {
					exact++
				}
				if age := r.At.Sub(at); age >= 0 && age <= 2*rc.period {
					twoPeriods++
				}
			}
		}
		if r.Sum > twoPeriods {
			t.Fatalf("%s: rate %d exceeds the %d requests of the last two periods", label, r.Sum, twoPeriods)
		}
		errv := float64(r.Sum) - float64(exact)
		maxErr, sumAbsErr = math.Max(maxErr, math.Abs(errv)), sumAbsErr+math.Abs(errv)
		samples++
		t.Logf("%s: rate %d (a %d, b %d), exact sliding count %d, error %+.0f", label, r.Sum, counted["a"],
			counted["b"], exact, errv)
		return r
	}

	sends := slices.Clone(rc.sends)
	slices.SortStableFunc(sends, func(x, y rateSend) int {
		switch {
		case x.at < y.at:
			return -1
		case x.at > y.at:
			return 1
		default:
			return 0
		}
	})
	start := time.Now()
	// Samples are offset from the send schedule so that they do not fall
	// on the millisecond where a reading steps, where the comparison
	// is decided by clock granularity rather than by the estimator.
	nextSample := 0.013
	for nextSample <= rc.sampleTo {
		if len(sends) > 0 && sends[0].at <= nextSample {
			s := sends[0]
			sends = sends[1:]
			time.Sleep(time.Until(start.Add(periods(s.at))))
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			for range s.n {
				if _, sendErr := c.Send(ctx, l.Node(s.node).LabAddr, client); sendErr != nil {
					cancel()
					t.Fatalf("send to %s: %v", s.node, sendErr)
				}
				sent[s.node] = append(sent[s.node], time.Now())
			}
			cancel()
			continue
		}
		time.Sleep(time.Until(start.Add(periods(nextSample))))
		sample(fmt.Sprintf("t=%.2fP", time.Since(start).Seconds()/rc.period.Seconds()))
		nextSample += rc.step
	}
	for _, n := range nodes {
		if got := l.Responder.Count(lab.Observation{Node: n, Listener: "lab", Key: haKey}); got != len(sent[n]) {
			t.Fatalf("responder saw %d requests on %s, sent %d", got, n, len(sent[n]))
		}
	}
	t.Logf("%d samples, %d of %d per-source readings equal at the query midpoint; sum vs exact sliding count: "+
		"max |error| %.0f, mean %.2f", samples, exactMatches, compared, maxErr, sumAbsErr/float64(samples))
	if !rc.full {
		return
	}

	// Decay: by now every reading is 0 on both sides, and no peer update
	// arrived since the last request.
	r := sample("decayed")
	if r.Sum != 0 || !r.Next.IsZero() {
		t.Fatalf("decayed rate %d next %v", r.Sum, r.Next)
	}
	if due, ok := (aggregate.Cadence{}).Due(r); ok {
		t.Fatalf("decayed rate due again at %v", due)
	}
	var deadline time.Time
	for _, n := range nodes {
		e, ok := store.Lookup(n, lab.LabTable, k)
		last := sent[n][len(sent[n])-1]
		if !ok || e.Received.After(last.Add(time.Second)) {
			t.Fatalf("source %s: entry %+v present %v; last request at %v: an update arrived during silence", n, e,
				ok, last)
		}
		deadline = later(deadline, e.Deadline)
	}
	// Expiry: the entries leave and nothing brings the key back.
	time.Sleep(time.Until(deadline) + 200*time.Millisecond)
	r, err = aggregate.Rate(store, lab.LabTable, k)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range r.Sources {
		if c.Present {
			t.Fatalf("expired entry still contributes: %+v", c)
		}
	}
	if r.Sum != 0 || !r.Complete {
		t.Fatalf("after expiry: %+v", r)
	}
	for _, n := range nodes {
		tbl, err := l.Node(n).ShowTable(t.Context(), lab.LabTable)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := tbl.Entry(haKey); ok {
			t.Fatalf("node %s still holds the key after its expiry", n)
		}
	}
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
