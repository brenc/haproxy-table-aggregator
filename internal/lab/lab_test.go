package lab_test

import (
	"context"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/lab"
	"github.com/brenc/haproxy-table-aggregator/internal/lab/labtest"
)

// Both rate-period variants the plan requires.
var periods = []time.Duration{10 * time.Second, 60 * time.Second}

func forEachPeriod(t *testing.T, fn func(t *testing.T, period time.Duration)) {
	t.Helper()
	for _, p := range periods {
		t.Run(p.String(), func(t *testing.T) {
			t.Parallel()
			fn(t, p)
		})
	}
}

func newClient(t *testing.T, source net.IP) *lab.Client {
	t.Helper()
	c, err := lab.NewClient(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.CloseIdle)
	return c
}

func showTable(t *testing.T, n *lab.Node, name string) lab.Table {
	t.Helper()
	tbl, err := n.ShowTable(t.Context(), name)
	if err != nil {
		t.Fatalf("node %s: show table %s: %v", n.Name, name, err)
	}
	if tbl.Name != name || tbl.Type != "ipv6" {
		t.Fatalf("node %s: table header name=%q type=%q, want %q ipv6", n.Name, tbl.Name, tbl.Type, name)
	}
	if tbl.Used != int64(len(tbl.Entries)) {
		t.Fatalf("node %s: table %s used=%d but %d entries dumped", n.Name, name, tbl.Used, len(tbl.Entries))
	}
	return tbl
}

// tableCounts maps each key to http_req_cnt and checks that the entry's
// rate (sent well within one period) and expiry agree with the schema.
func tableCounts(t *testing.T, tbl lab.Table, period time.Duration) map[string]int64 {
	t.Helper()
	rateField := fmt.Sprintf("http_req_rate(%d)", period.Milliseconds())
	out := map[string]int64{}
	for _, e := range tbl.Entries {
		cnt, ok := e.Data["http_req_cnt"]
		if !ok {
			t.Fatalf("entry %s: no http_req_cnt in %v", e.Key, e.Data)
		}
		rate, ok := e.Data[rateField]
		if !ok {
			t.Fatalf("entry %s: no %s in %v", e.Key, rateField, e.Data)
		}
		if rate != cnt {
			t.Errorf("entry %s: %s=%d, want %d (all requests within one period)", e.Key, rateField, rate, cnt)
		}
		if e.Exp <= 2*period || e.Exp > lab.ExpiryFactor*period {
			t.Errorf("entry %s: exp=%v, want in (%v, %v]", e.Key, e.Exp, 2*period, lab.ExpiryFactor*period)
		}
		out[e.Key] = cnt
	}
	return out
}

func requireNoAnomalies(t *testing.T, r *lab.Responder) {
	t.Helper()
	dups, missing := r.Anomalies()
	if len(dups) > 0 || missing > 0 {
		t.Fatalf("responder anomalies: duplicate ids %v, %d requests without id", dups, missing)
	}
}

func TestTwoNodeIndependentCounts(t *testing.T) {
	forEachPeriod(t, func(t *testing.T, period time.Duration) {
		l := labtest.Start(t, lab.Options{Period: period})
		if l.Record.GoVersion != runtime.Version() || l.Record.HAProxySHA256 == "" ||
			l.Record.HAProxyVersion == "" || !strings.Contains(l.Record.HAProxyVV, l.Record.HAProxyVersion) {
			t.Fatalf("incomplete run record: %+v", l.Record)
		}
		c := newClient(t, nil)
		const client, key = "2001:db8:ab:cd:1:2:3:4", "2001:db8:ab:cd::"
		want := map[string]int{"a": 10, "b": 20}

		for _, n := range l.Nodes {
			cfg, err := os.ReadFile(n.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(cfg), "peers") || strings.Contains(string(cfg), "peer ") {
				t.Fatalf("node %s config mentions peers:\n%s", n.Name, cfg)
			}
			resps, err := c.SendN(t.Context(), n.LabAddr, client, want[n.Name])
			if err != nil {
				t.Fatalf("node %s: %v", n.Name, err)
			}
			last := resps[len(resps)-1]
			if last.Key != key || last.Lookup != int64(want[n.Name]) {
				t.Fatalf("node %s: last response key=%q lookup=%d, want %q %d",
					n.Name, last.Key, last.Lookup, key, want[n.Name])
			}
		}
		requireNoAnomalies(t, l.Responder)

		for _, n := range l.Nodes {
			observed := l.Responder.Count(lab.Observation{Node: n.Name, Listener: "lab", Key: key})
			if observed != want[n.Name] {
				t.Fatalf("node %s: responder observed %d, want %d", n.Name, observed, want[n.Name])
			}
			got := tableCounts(t, showTable(t, n, lab.LabTable), period)
			wantTable := map[string]int64{key: int64(observed)}
			if !maps.Equal(got, wantTable) {
				t.Fatalf("node %s: %s = %v, want %v", n.Name, lab.LabTable, got, wantTable)
			}
			if prod := showTable(t, n, lab.ProdTable); len(prod.Entries) != 0 {
				t.Fatalf("node %s: %s unexpectedly has %v", n.Name, lab.ProdTable, prod.Keys())
			}
			peers, err := n.Runtime(t.Context(), "show peers")
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(peers) != "" {
				t.Fatalf("node %s: show peers is not empty:\n%s", n.Name, peers)
			}
		}
	})
}

type keyCase struct {
	header string
	n      int
}

func TestLabKeyCases(t *testing.T) {
	cases := []keyCase{
		{"2001:db8:1:1::1", 3},
		{"2001:db8:1:1:ffff:ffff:ffff:ffff", 4},
		{"2001:db8:1:2::1", 5},
		{"192.0.2.10", 2},
		{"192.0.2.11", 6},
		// ipmask converts an IPv6 sample to IPv4 before masking whenever
		// HAProxy's v6tov4 accepts it: IPv4-mapped (::ffff:0:0/96),
		// IPv4-compatible (::/96), and 6to4 (2002::/16). Such clients are
		// keyed by the embedded IPv4 /32, coalescing with native IPv4 and,
		// for 6to4, across the whole /48 rather than per /64.
		{"::ffff:192.0.2.10", 1},
		{"2002:c000:20a:1::1", 2},
		{"2002:c000:20a:2::1", 1},
		{"::192.0.2.11", 1},
	}
	// Keys as the ipv6 table stores them, and as HAProxy formats the
	// tracked sample (IPv4 stays IPv4 until stored in the table).
	wantTable := map[string]int64{
		"2001:db8:1:1::":    7,
		"2001:db8:1:2::":    5,
		"::ffff:192.0.2.10": 6,
		"::ffff:192.0.2.11": 7,
	}
	wantObserved := map[string]int{
		"2001:db8:1:1::": 7,
		"2001:db8:1:2::": 5,
		"192.0.2.10":     6,
		"192.0.2.11":     7,
	}
	forEachPeriod(t, func(t *testing.T, period time.Duration) {
		l := labtest.Start(t, lab.Options{Period: period})
		a, b := l.Node("a"), l.Node("b")
		c := newClient(t, nil)
		for _, kc := range cases {
			if _, err := c.SendN(t.Context(), a.LabAddr, kc.header, kc.n); err != nil {
				t.Fatalf("client %s: %v", kc.header, err)
			}
		}
		for _, bad := range []string{"", "not-an-ip"} {
			r, err := c.Send(t.Context(), a.LabAddr, bad)
			if err != nil {
				t.Fatal(err)
			}
			if r.Status != http.StatusBadRequest || l.Responder.Seen(r.ID) {
				t.Fatalf("header %q: status %d, reached responder %v; want 400, not forwarded",
					bad, r.Status, l.Responder.Seen(r.ID))
			}
		}
		requireNoAnomalies(t, l.Responder)

		for key, want := range wantObserved {
			got := l.Responder.Count(lab.Observation{Node: "a", Listener: "lab", Key: key})
			if got != want {
				t.Errorf("responder key %s: observed %d, want %d", key, got, want)
			}
		}
		if got := tableCounts(t, showTable(t, a, lab.LabTable), period); !maps.Equal(got, wantTable) {
			t.Fatalf("node a %s = %v, want %v", lab.LabTable, got, wantTable)
		}
		if tbl := showTable(t, b, lab.LabTable); len(tbl.Entries) != 0 {
			t.Fatalf("node b received keys it never tracked: %v", tbl.Keys())
		}
	})
}

// TestProductionMasking drives the production-style listener, whose key is
// src,ipmask(32,64), from real loopback sources. The synthetic client
// header is sent on purpose and must be ignored there.
//
// ::1 is the only IPv6 source available without privileges, and it lies in
// the IPv4-compatible range ::/96, so ipmask keys it as IPv4 0.0.0.1. /64
// masking of native IPv6 sources through src is covered by
// TestProxyProtocolMasking.
func TestProductionMasking(t *testing.T) {
	forEachPeriod(t, func(t *testing.T, period time.Duration) {
		l := labtest.Start(t, lab.Options{Period: period})
		a := l.Node("a")
		type source struct {
			ip      string
			addr    string
			n       int
			tracked string
			stored  string
		}
		sources := []source{
			{"127.0.0.2", a.ProdAddr4, 3, "127.0.0.2", "::ffff:127.0.0.2"},
			{"127.0.0.3", a.ProdAddr4, 4, "127.0.0.3", "::ffff:127.0.0.3"},
		}
		if a.ProdAddr6 != "" {
			sources = append(sources, source{"::1", a.ProdAddr6, 5, "0.0.0.1", "::ffff:0.0.0.1"})
		} else {
			t.Log("IPv6 loopback unavailable; skipping ::1 source")
		}
		want := map[string]int64{}
		for _, s := range sources {
			c := newClient(t, net.ParseIP(s.ip))
			if _, err := c.SendN(t.Context(), s.addr, "2001:db8:dead:beef::1", s.n); err != nil {
				t.Fatalf("source %s: %v", s.ip, err)
			}
			want[s.stored] = int64(s.n)
		}
		requireNoAnomalies(t, l.Responder)
		for _, s := range sources {
			got := l.Responder.Count(lab.Observation{Node: "a", Listener: "prod", Key: s.tracked})
			if got != s.n {
				t.Errorf("source %s: responder observed %d, want %d", s.ip, got, s.n)
			}
		}
		if got := tableCounts(t, showTable(t, a, lab.ProdTable), period); !maps.Equal(got, want) {
			t.Fatalf("node a %s = %v, want %v", lab.ProdTable, got, want)
		}
		if tbl := showTable(t, a, lab.LabTable); len(tbl.Entries) != 0 {
			t.Fatalf("spoofed header reached %s: %v", lab.LabTable, tbl.Keys())
		}
	})
}

// TestProxyProtocolMasking runs the production key rule src,ipmask(32,64)
// against native IPv6 and IPv4 sources. The sources are declared in PROXY
// protocol headers, which the lab trusts only on its loopback PROXY
// listener; the src fetch and key expression are the production ones.
func TestProxyProtocolMasking(t *testing.T) {
	type source struct {
		addr    string
		n       int
		tracked string
	}
	sources := []source{
		{"2001:db8:1:1::5", 3, "2001:db8:1:1::"},
		{"2001:db8:1:1:ffff:ffff:ffff:9", 4, "2001:db8:1:1::"},
		{"2001:db8:1:2::1", 5, "2001:db8:1:2::"},
		{"192.0.2.10", 2, "192.0.2.10"},
		// 6to4 (2002::/16) sources are converted to their embedded IPv4
		// (2002:c000:20a:: embeds 192.0.2.10) before masking, so a whole
		// 6to4 /48 shares one key with that native IPv4 client.
		{"2002:c000:20a:1::1", 1, "192.0.2.10"},
		{"2002:c000:20a:2::1", 1, "192.0.2.10"},
		// An IPv4-mapped TCP6 source is keyed as plain IPv4.
		{"::ffff:192.0.2.11", 2, "192.0.2.11"},
		{"192.0.2.11", 1, "192.0.2.11"},
	}
	wantTable := map[string]int64{
		"2001:db8:1:1::":    7,
		"2001:db8:1:2::":    5,
		"::ffff:192.0.2.10": 4,
		"::ffff:192.0.2.11": 3,
	}
	wantObserved := map[string]int{
		"2001:db8:1:1::": 7,
		"2001:db8:1:2::": 5,
		"192.0.2.10":     4,
		"192.0.2.11":     3,
	}
	forEachPeriod(t, func(t *testing.T, period time.Duration) {
		l := labtest.Start(t, lab.Options{Period: period})
		a := l.Node("a")
		for _, s := range sources {
			c, err := lab.NewProxyClient(netip.MustParseAddr(s.addr))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.CloseIdle)
			resps, err := c.SendN(t.Context(), a.ProxyAddr, "2001:db8:dead:beef::1", s.n)
			if err != nil {
				t.Fatalf("source %s: %v", s.addr, err)
			}
			if got := resps[0].Key; got != s.tracked {
				t.Fatalf("source %s: tracked key %q, want %q", s.addr, got, s.tracked)
			}
		}
		requireNoAnomalies(t, l.Responder)
		for key, want := range wantObserved {
			got := l.Responder.Count(lab.Observation{Node: "a", Listener: "proxy", Key: key})
			if got != want {
				t.Errorf("responder key %s: observed %d, want %d", key, got, want)
			}
		}
		if got := tableCounts(t, showTable(t, a, lab.ProxyTable), period); !maps.Equal(got, wantTable) {
			t.Fatalf("node a %s = %v, want %v", lab.ProxyTable, got, wantTable)
		}

		// PROXY headers are honoured nowhere else, and the PROXY listener
		// refuses connections that do not start with one.
		pc, err := lab.NewProxyClient(netip.MustParseAddr("2001:db8:9::1"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pc.CloseIdle)
		r, err := pc.Send(t.Context(), a.ProdAddr4, "")
		if err == nil && (r.Status != http.StatusBadRequest || l.Responder.Seen(r.ID)) {
			t.Fatalf("PROXY header on prod listener: status %d, forwarded %v", r.Status, l.Responder.Seen(r.ID))
		}
		r, err = newClient(t, nil).Send(t.Context(), a.ProxyAddr, "")
		if err == nil || l.Responder.Seen(r.ID) {
			t.Fatalf("plain request on PROXY listener: err=%v status=%d, want refused", err, r.Status)
		}
		requireNoAnomalies(t, l.Responder)
		if tbl := showTable(t, a, lab.ProdTable); len(tbl.Entries) != 0 {
			t.Fatalf("%s tracked a PROXY-declared source: %v", lab.ProdTable, tbl.Keys())
		}
		if tbl := showTable(t, a, lab.ProxyTable); int64(len(tbl.Entries)) != int64(len(wantTable)) {
			t.Fatalf("%s gained entries from refused requests: %v", lab.ProxyTable, tbl.Keys())
		}
		if tbl := showTable(t, l.Node("b"), lab.ProxyTable); len(tbl.Entries) != 0 {
			t.Fatalf("node b %s has %v", lab.ProxyTable, tbl.Keys())
		}
	})
}

func TestStartRejectsBadBinary(t *testing.T) {
	_, err := lab.Start(context.Background(), lab.Options{HAProxy: "/nonexistent/haproxy"})
	if err == nil {
		t.Fatal("Start succeeded with a missing binary")
	}
	_, err = lab.Start(context.Background(), lab.Options{})
	if err == nil {
		t.Fatal("Start succeeded without a binary")
	}
}

func TestStartRejectsLongSocketPath(t *testing.T) {
	bin := labtest.HAProxy(t)
	// A lab directory too deep for a runtime socket must be refused before
	// any process starts; nothing may be left behind.
	base := t.TempDir()
	deep := base + "/" + strings.Repeat("d", 100)
	if err := os.Mkdir(deep, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := lab.Start(t.Context(), lab.Options{HAProxy: bin, BaseDir: deep}); err == nil ||
		!strings.Contains(err.Error(), "limit") {
		t.Fatalf("Start with long BaseDir: err=%v, want socket path limit error", err)
	}
	entries, err := os.ReadDir(deep)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed Start left %d entries behind, first %q", len(entries), entries[0].Name())
	}
}
