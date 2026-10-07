package lab

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
)

// exampleDir holds the two-proxy example configurations.
var exampleDir = filepath.Join("..", "..", "examples", "two-node")

// enforceLines returns the decision lines of the lab listener's
// enforcement with limit, normalized to the example's names: the lab's
// variables and output tables renamed, diagnostics dropped.
func enforceLines(t *testing.T, limit int) []string {
	t.Helper()
	p := withAggregator(testParams("fd@3", "fd@4", "fd@5"), "fd@6", "fd@7")
	p.Limit, p.AuthHdr, p.DenyHdr, p.LocalRateHdr, p.AggRateHdr = limit, "A", "D", "L", "R"
	cfg, err := renderConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	_, lab, ok := strings.Cut(string(cfg), "\nfrontend lab\n")
	lab, _, _ = strings.Cut(lab, "\nfrontend ")
	if !ok {
		t.Fatal("no lab frontend")
	}
	r := strings.NewReplacer("txn.lab_", "txn.agg_", OutputTable, "req_agg", MetaTable, "req_meta")
	var out []string
	for _, line := range strings.Split(lab, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "acl "), strings.HasPrefix(line, "http-request deny deny_status 429"),
			strings.HasPrefix(line, "http-request set-var(txn.agg_"),
			strings.HasPrefix(line, "http-request set-var(txn.lab_lrate)"):
			out = append(out, r.Replace(line))
		}
	}
	return out
}

// TestRenderEnforcement checks the lab listener's enforcement: absent
// without a limit; with one, the frozen phase 06 authority expressions,
// the local limit on the node's own rate always, the same full limit on
// the aggregate only under authority, local first.
func TestRenderEnforcement(t *testing.T) {
	plain, err := renderConfig(withAggregator(testParams("fd@3", "fd@4", "fd@5"), "fd@6", "fd@7"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "deny_status 429") {
		t.Fatalf("enforcement without a limit:\n%s", plain)
	}
	lines := enforceLines(t, 77)
	want := []string{
		"http-request set-var(txn.agg_lrate) sc_http_req_rate(0)",
		"acl agg_over  var(txn.agg_key),table_gpt(1,req_agg) ge 77",
		"acl local_over var(txn.agg_lrate) -m int ge 77",
		"http-request deny deny_status 429 if local_over",
		"http-request deny deny_status 429 if agg_meta agg_lease agg_v2 agg_gen agg_over",
	}
	joined := strings.Join(lines, "\n")
	for _, w := range want {
		if !strings.Contains(joined, w) {
			t.Errorf("enforcement lacks %q:\n%s", w, joined)
		}
	}
	if strings.Index(joined, want[3]) > strings.Index(joined, want[4]) {
		t.Errorf("aggregate rule before the local one:\n%s", joined)
	}
	for _, line := range strings.Split(frozenExample(t), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "acl agg_") && !strings.HasPrefix(line, "acl agg_over") ||
			strings.HasPrefix(line, "http-request set-var(txn.agg_") && !strings.Contains(line, "txn.agg_key") {
			line = strings.NewReplacer(OutputTable, "req_agg", MetaTable, "req_meta").Replace(line)
			if !strings.Contains(joined, line) {
				t.Errorf("enforcement lacks the frozen %q", line)
			}
		}
	}
}

// TestExampleConfigs checks the two-proxy example: htad.json loads and
// names the tables the HAProxy files declare, each HAProxy file makes
// exactly the lab listener's enforcement decisions (with limit 100), and,
// with HTA_HAPROXY set, each passes the stock configuration check with
// zero-warning.
func TestExampleConfigs(t *testing.T) {
	cfg, err := config.Load(filepath.Join(exampleDir, "htad.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Tables) != 1 || cfg.Tables[0].Name != "req_in" || cfg.Tables[0].Period != 10*time.Second ||
		len(cfg.Outputs) != 2 || cfg.Outputs[0].Name != "req_agg" || cfg.Outputs[0].Input != "req_in" ||
		cfg.Outputs[0].Expire != 30*time.Second || cfg.Outputs[1].Name != "req_meta" ||
		cfg.Outputs[1].Kind != output.KindMetadata || cfg.Outputs[1].Expire != output.MaxLease {
		t.Fatalf("htad.json: %+v", cfg)
	}
	want := enforceLines(t, 100)
	haproxy := os.Getenv("HTA_HAPROXY")
	for _, node := range []string{"a", "b"} {
		path := filepath.Join(exampleDir, "haproxy-"+node+".cfg")
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		doc := string(b)
		var got []string
		for _, line := range strings.Split(doc, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "acl ") || strings.HasPrefix(line, "http-request deny") ||
				strings.HasPrefix(line, "http-request set-var(") && !strings.HasPrefix(line, "http-request set-var(txn.agg_key)") {
				got = append(got, line)
			}
		}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("%s decides with\n%s\nwant the lab's\n%s", path, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		for _, w := range []string{
			"    localpeer " + node + "\n",
			"    server " + node + "\n    server agg 127.0.0.1:10000\n",
			"backend req_in\n    stick-table type ipv6 size 1m expire 30s store http_req_cnt,http_req_rate(10s) peers agg\n",
			"backend req_agg\n    stick-table type ipv6 size 1m expire 30s store gpt(4) peers agg\n",
			"backend req_meta\n    stick-table type ipv6 size 16 expire 2s store gpt(4) peers agg\n",
			"    http-request set-var(txn.agg_key) src,ipmask(32,64)\n    http-request track-sc0 var(txn.agg_key) table req_in\n",
		} {
			if !strings.Contains(doc, w) {
				t.Errorf("%s lacks %q", path, w)
			}
		}
		if src, ok := cfg.Source(node); !ok || !strings.Contains(doc, "    bind "+src.Address+"\n") {
			t.Errorf("%s: peers bind is not htad.json's source address %+v", path, src)
		}
		if n := strings.Count(doc, "track-sc"); n != 1 {
			t.Errorf("%s tracks %d times; only the input may be tracked", path, n)
		}
		if haproxy == "" {
			continue
		}
		checked := filepath.Join(t.TempDir(), "haproxy-"+node+".cfg")
		zw := strings.Replace(doc, "\nglobal\n", "\nglobal\n    zero-warning\n", 1)
		if werr := os.WriteFile(checked, []byte(zw), 0o600); werr != nil {
			t.Fatal(werr)
		}
		out, err := exec.CommandContext(t.Context(), haproxy, "-c", "-V", "-f", checked).CombinedOutput()
		if err != nil {
			t.Errorf("%s: haproxy -c -V: %v\n%s", path, err, out)
			continue
		}
		t.Logf("%s: haproxy -c -V (zero-warning): %s", path, strings.TrimSpace(string(out)))
	}
	if haproxy == "" {
		t.Skip("HTA_HAPROXY not set: configuration check skipped")
	}
}
