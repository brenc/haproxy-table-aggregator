package lab

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// Captured from stock HAProxy 3.4.6 and 3.2.25, which print the same
// format.
const tableDump = `# table: lab_in, type: ipv6, size:1024, used:3
0x5579836ef8d0: key=2001:db8:1:1:: use=0 exp=29900 shard=0 http_req_cnt=2 http_req_rate(10000)=2
0x5579836f3ac0: key=::ffff:192.0.2.10 use=0 exp=29951 shard=0 http_req_cnt=2 http_req_rate(10000)=2
0x5579836f39e0: key=2001:db8:1:2:: use=0 exp=29914 shard=0 http_req_cnt=1 http_req_rate(10000)=1

`

func TestParseTable(t *testing.T) {
	tbl, err := ParseTable(tableDump)
	if err != nil {
		t.Fatal(err)
	}
	if tbl.Name != "lab_in" || tbl.Type != "ipv6" || tbl.Size != 1024 || tbl.Used != 3 {
		t.Fatalf("header = %+v", tbl)
	}
	if got := strings.Join(tbl.Keys(), ","); got != "2001:db8:1:1::,::ffff:192.0.2.10,2001:db8:1:2::" {
		t.Fatalf("keys = %s", got)
	}
	e, ok := tbl.Entry("::ffff:192.0.2.10")
	if !ok {
		t.Fatal("missing entry")
	}
	if e.Exp != 29951*time.Millisecond || e.Data["http_req_cnt"] != 2 || e.Data["http_req_rate(10000)"] != 2 {
		t.Fatalf("entry = %+v", e)
	}
	if _, ok := tbl.Entry("::1"); ok {
		t.Fatal("found absent key")
	}
}

func TestParseTableEmpty(t *testing.T) {
	tbl, err := ParseTable("# table: prod_in, type: ipv6, size:1024, used:0\n\n")
	if err != nil {
		t.Fatal(err)
	}
	if len(tbl.Entries) != 0 || tbl.Used != 0 {
		t.Fatalf("got %+v", tbl)
	}
}

func TestParseTableErrors(t *testing.T) {
	for name, dump := range map[string]string{
		"empty":       "",
		"unknown":     "No such table: nope\n",
		"no header":   "0x1: key=::1 use=0 exp=1 http_req_cnt=1\n",
		"bad number":  "# table: t, type: ipv6, size:1, used:1\n0x1: key=::1 exp=1 http_req_cnt=x\n",
		"bad exp":     "# table: t, type: ipv6, size:1, used:1\n0x1: key=::1 exp=soon\n",
		"no key":      "# table: t, type: ipv6, size:1, used:1\n0x1: use=0 exp=1\n",
		"two headers": "# table: t, type: ipv6, size:1, used:0\n# table: u, type: ipv6, size:1, used:0\n",
		"bad size":    "# table: t, type: ipv6, size:big, used:0\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseTable(dump); err == nil {
				t.Fatal("ParseTable accepted malformed dump")
			}
		})
	}
	if _, err := ParseTable("No such table: nope\n"); !errors.Is(err, ErrNoTable) {
		t.Fatalf("unknown table error = %v, want ErrNoTable", err)
	}
}

func TestRuntimeCommandRejectsChaining(t *testing.T) {
	for _, cmd := range []string{"show info; shutdown frontend lab", "show info\nshow peers"} {
		if _, err := RuntimeCommand(t.Context(), "/nonexistent.sock", cmd); err == nil ||
			!strings.Contains(err.Error(), "single command") {
			t.Fatalf("%q: err = %v", cmd, err)
		}
	}
}

func TestRenderConfig(t *testing.T) {
	cfg, err := renderConfig(configParams{
		Node: "a", Socket: "/tmp/x/a.sock", LabBind: "fd@3",
		ProdBinds: []string{"fd@4", "fd@5"}, ProxyBind: "fd@6", Responder: "127.0.0.1:9",
		PeriodMS: "10000", ExpireMS: "30000",
		LabTable: LabTable, ProdTable: ProdTable, ProxyTable: ProxyTable, ClientHdr: ClientHeader,
		NodeHdr: NodeHeader, ListenerHdr: ListenerHeader, KeyHdr: KeyHeader,
		LookupHdr: LookupHeader,
	})
	if err != nil {
		t.Fatal(err)
	}
	s := string(cfg)
	for _, want := range []string{
		"stick-table type ipv6 size 1k expire 30000 store http_req_cnt,http_req_rate(10000)",
		"bind fd@3", "bind fd@4", "bind fd@5", "bind fd@6 accept-proxy",
		"track-sc0 var(txn.lab_key) table proxy_in",
		"http-request set-var(txn.lab_key) src,ipmask(32,64)",
		"track-sc0 var(txn.lab_key) table lab_in",
		"var(txn.lab_key),table_http_req_cnt(lab_in)",
		"server responder 127.0.0.1:9",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("config lacks %q", want)
		}
	}
	if strings.Contains(s, "peers") {
		t.Error("config must not define peers")
	}
	// The synthetic header must feed only the lab frontend, and PROXY
	// headers must be accepted only by the proxy frontend.
	_, prod, _ := strings.Cut(s, "frontend prod")
	if strings.Contains(prod, ClientHeader) {
		t.Error("production-style frontend reads the synthetic client header")
	}
	if strings.Count(s, "accept-proxy") != 1 {
		t.Error("accept-proxy must appear on exactly one bind")
	}
	if strings.Count(s, "src,ipmask(32,64)") != 2 {
		t.Error("prod and proxy frontends must share the production key rule")
	}
}

func TestMaxSocketPath(t *testing.T) {
	// "<path>.4194304.tmp" plus NUL must fit in a 108-byte sun_path.
	if got := maxSocketPath + len(".4194304.tmp") + 1; got != 108 || maxSocketPath != 95 {
		t.Fatalf("maxSocketPath = %d (worst-case bind length %d)", maxSocketPath, got)
	}
}

func TestValidateNodeNames(t *testing.T) {
	if err := validateNodeNames([]string{"a", "b2"}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]string{{""}, {"A"}, {"a b"}, {"a", "a"}, {"a;"}} {
		if err := validateNodeNames(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
