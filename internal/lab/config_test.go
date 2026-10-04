package lab

import (
	"strings"
	"testing"
)

func TestRenderAggregatorPeers(t *testing.T) {
	base := configParams{
		Node: "a", Socket: "/s", LabBind: "fd@3", ProdBinds: []string{"fd@4"}, ProxyBind: "fd@5",
		Responder: "127.0.0.1:1", PeriodMS: "10000", ExpireMS: "30000", LabTable: LabTable, ProdTable: ProdTable,
		ProxyTable: ProxyTable,
	}
	plain, err := renderConfig(base)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "peers") || strings.Contains(string(plain), "localpeer") {
		t.Fatalf("config without an aggregator has peers:\n%s", plain)
	}
	withAgg := base
	withAgg.AggName, withAgg.AggAddr, withAgg.PeersBind = "agg", "127.0.0.1:9", "fd@6"
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
	if n := strings.Count(string(cfg), " peers agg\n"); n != 3 {
		t.Errorf("%d tables attached to the peers section, want 3", n)
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
	if err := validateAggregator(&Aggregator{Name: "agg", NodeAddrs: map[string]string{"a": "127.0.0.1:1", "b": "[::1]:2"}}, nodes); err != nil {
		t.Errorf("per-node addresses: %v", err)
	}
}
