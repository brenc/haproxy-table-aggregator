package lab

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/brenc/haproxy-table-aggregator/internal/output"
)

func TestRenderAggregatorPeers(t *testing.T) {
	base := testParams("fd@3", "fd@4", "fd@5")
	plain, err := renderConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "peers") || strings.Contains(string(plain), "localpeer") {
		t.Fatalf("config without an aggregator has peers:\n%s", plain)
	}
	withAgg := withAggregator(base, "fd@6", "fd@7")
	cfg, err := renderConfig(withAgg)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"    localpeer a\n",
		"peers agg\n    bind fd@6\n    server a\n    server agg 127.0.0.1:9\n",
		"http_req_rate(10000) peers agg\n\nbackend " + ProdTable,
	} {
		if !strings.Contains(string(cfg), want) {
			t.Errorf("config lacks %q:\n%s", want, cfg)
		}
	}
	if n := strings.Count(string(cfg), " peers agg\n"); n != 5 {
		t.Errorf("%d tables attached to the peers section, want 5", n)
	}
	for _, want := range []string{
		"backend lab_out\n    stick-table type ipv6 size 1k expire 30000 store gpt(4) peers agg\n",
		"backend lab_meta\n    stick-table type ipv6 size 16 expire 2000 store gpt(4) peers agg\n",
		"frontend probe\n    bind fd@7\n",
		"acl agg_over  var(txn.lab_key),table_gpt(1,lab_out) ge 1000\n",
		"acl agg_lease var(txn.agg_left) -m int le 2000\n",
		"http-request return status 429 hdr X-Lab-Key \"%[var(txn.lab_key)]\" hdr X-Lab-Out-Version",
	} {
		if !strings.Contains(string(cfg), want) {
			t.Errorf("config lacks %q:\n%s", want, cfg)
		}
	}
	// Output tables are only ever read: no rule tracks them.
	for _, line := range strings.Split(string(cfg), "\n") {
		if strings.Contains(line, "track-sc") && (strings.Contains(line, OutputTable) || strings.Contains(line, MetaTable)) {
			t.Errorf("output table tracked: %s", line)
		}
	}
	if strings.Index(string(cfg), "backend "+OutputTable) < strings.Index(string(cfg), "backend "+LabTable) {
		t.Error("output tables declared first without OutputFirst")
	}
	withAgg.OutputFirst = true
	first, err := renderConfig(withAgg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Index(string(first), "backend "+OutputTable) > strings.Index(string(first), "backend "+LabTable) ||
		strings.Count(string(first), "backend "+OutputTable+"\n") != 1 {
		t.Errorf("OutputFirst config:\n%s", first)
	}
}

func testParams(lab, prod, proxy string) configParams {
	return configParams{
		Node: "a", Socket: "/s", LabBind: lab, ProdBinds: []string{prod}, ProxyBind: proxy,
		Responder: "127.0.0.1:1", PeriodMS: "10000", ExpireMS: "30000", LabTable: LabTable, ProdTable: ProdTable,
		ProxyTable: ProxyTable, ClientHdr: ClientHeader, NodeHdr: NodeHeader, ListenerHdr: ListenerHeader,
		KeyHdr: KeyHeader, LookupHdr: LookupHeader,
	}
}

func withAggregator(p configParams, peers, probe string) configParams {
	p.AggName, p.AggAddr, p.PeersBind, p.ProbeBind = "agg", "127.0.0.1:9", peers, probe
	p.OutTable, p.MetaTable, p.OutSlots, p.OutLimit = OutputTable, MetaTable, OutputSlots, OutputLimit
	p.MetaExpMS, p.LeaseMS, p.ProbeHdrs = millis(MetaExpire), output.LeaseWindowMillis, probeHeaders(OutputTable, MetaTable)
	return p
}

// TestConfigValidates runs the stock binary's configuration check (-c)
// on every lab configuration variant. The lab's own configurations bind
// inherited descriptors, which a standalone check cannot open, so these
// use fixed loopback ports, which a check does not bind. zero-warning
// makes any warning fail too.
func TestConfigValidates(t *testing.T) {
	haproxy := os.Getenv("HTA_HAPROXY")
	if haproxy == "" {
		t.Skip("HTA_HAPROXY not set")
	}
	base := testParams("127.0.0.1:10001", "127.0.0.1:10002", "127.0.0.1:10003")
	base.Socket = filepath.Join(t.TempDir(), "s.sock")
	agg := withAggregator(base, "127.0.0.1:10004", "127.0.0.1:10005")
	first := agg
	first.OutputFirst = true
	for name, p := range map[string]configParams{"plain": base, "aggregator": agg, "output first": first} {
		cfg, err := renderConfig(p)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "haproxy.cfg")
		if werr := os.WriteFile(path, cfg, 0o600); werr != nil {
			t.Fatal(werr)
		}
		out, err := exec.CommandContext(t.Context(), haproxy, "-c", "-V", "-f", path).CombinedOutput()
		if err != nil {
			t.Errorf("%s: haproxy -c -V: %v\n%s\n%s", name, err, out, cfg)
			continue
		}
		t.Logf("%s: haproxy -c -V: %s", name, strings.TrimSpace(string(out)))
	}
}

func TestValidateAggregator(t *testing.T) {
	nodes := []string{"a", "b"}
	for name, agg := range map[string]*Aggregator{
		"empty name":        {Addr: "127.0.0.1:1"},
		"spaced name":       {Name: "a g", Addr: "127.0.0.1:1"},
		"node name":         {Name: "a", Addr: "127.0.0.1:1"},
		"bad address":       {Name: "agg", Addr: "nowhere"},
		"bad override":      {Name: "agg", Addr: "127.0.0.1:1", NodeAddrs: map[string]string{"b": "x"}},
		"off-host":          {Name: "agg", Addr: "192.0.2.1:1"},
		"hostname":          {Name: "agg", Addr: "localhost:1"},
		"wildcard":          {Name: "agg", Addr: "0.0.0.0:1"},
		"port 0":            {Name: "agg", Addr: "127.0.0.1:0"},
		"off-host override": {Name: "agg", Addr: "127.0.0.1:1", NodeAddrs: map[string]string{"a": "198.51.100.1:1"}},
	} {
		if err := validateAggregator(agg, nodes); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := validateAggregator(&Aggregator{Name: "agg", Addr: "127.0.0.1:1", OutputFirst: []string{"c"}}, nodes); err == nil {
		t.Error("OutputFirst naming no node: accepted")
	}
	if err := validateAggregator(&Aggregator{Name: "agg", NodeAddrs: map[string]string{"a": "127.0.0.1:1", "b": "[::1]:2"}}, nodes); err != nil {
		t.Errorf("per-node addresses: %v", err)
	}
}

// frozenExample returns the HAProxy configuration block frozen in the
// phase 06 plan.
func frozenExample(t *testing.T) string {
	t.Helper()
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "plans", "06-freshness-gate.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(doc), "\n```haproxy\n")
	block, _, closed := strings.Cut(rest, "\n```\n")
	if !ok || !closed {
		t.Fatal("no haproxy block in the phase 06 plan")
	}
	return block + "\n"
}

// TestFrozenExampleValidates checks the phase 06 frozen example with the
// stock binary's configuration check, and that the lab's probe listener,
// which every freshness experiment decides with, uses its authority
// expressions verbatim (with the lab's key variable).
func TestFrozenExampleValidates(t *testing.T) {
	block := frozenExample(t)
	cfg, err := renderConfig(withAggregator(testParams("fd@3", "fd@4", "fd@5"), "fd@6", "fd@7"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(block, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "acl agg_") && !strings.HasPrefix(line, "http-request set-var(txn.agg_") {
			continue
		}
		if strings.HasPrefix(line, "http-request set-var(txn.agg_key)") {
			continue // the lab keys its probe from a header
		}
		n++
		if want := strings.ReplaceAll(line, "txn.agg_key", "txn.lab_key"); !strings.Contains(string(cfg), "    "+want+"\n") {
			t.Errorf("lab probe lacks the frozen %q", want)
		}
	}
	if n != 8 {
		t.Fatalf("found %d frozen authority expressions, want 8", n)
	}
	haproxy := os.Getenv("HTA_HAPROXY")
	if haproxy == "" {
		t.Skip("HTA_HAPROXY not set")
	}
	path := filepath.Join(t.TempDir(), "frozen.cfg")
	if werr := os.WriteFile(path, []byte("global\n    zero-warning\n"+strings.TrimPrefix(block, "global\n")), 0o600); werr != nil {
		t.Fatal(werr)
	}
	out, err := exec.CommandContext(t.Context(), haproxy, "-c", "-V", "-f", path).CombinedOutput()
	if err != nil {
		t.Fatalf("haproxy -c -V: %v\n%s\n%s", err, out, block)
	}
	t.Logf("frozen example: haproxy -c -V: %s", strings.TrimSpace(string(out)))
}
