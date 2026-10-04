package config_test

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/config"
	"github.com/brenc/haproxy-table-aggregator/internal/output"
)

const example = `{
  "local_peer": "agg",
  "insecure_plaintext_loopback_lab": true,
  "listen": "127.0.0.1:10000",
  "sources": [
    {"name": "a"},
    {"name": "b", "address": "127.0.0.1:10001"}
  ],
  "tables": [{"name": "lab_in", "period": "10s"}],
  "outputs": [
    {"name": "lab_out", "kind": "aggregate", "expire": "30s"},
    {"name": "lab_meta", "kind": "metadata", "expire": "2s"}
  ]
}`

func TestExampleAndDefaults(t *testing.T) {
	c, err := config.Parse([]byte(example))
	if err != nil {
		t.Fatal(err)
	}
	if c.LocalPeer != "agg" || c.Listen != "127.0.0.1:10000" || len(c.Sources) != 2 ||
		c.Sources[1] != (config.Source{Name: "b", Address: "127.0.0.1:10001"}) ||
		c.Tables[0] != (config.Table{Name: "lab_in", Period: 10 * time.Second}) {
		t.Fatalf("parsed %+v", c)
	}
	if c.Heartbeat != config.DefaultHeartbeat || c.IdleTimeout != config.DefaultIdleTimeout ||
		c.HandshakeTimeout != config.DefaultHandshakeTimeout || c.ReconnectMin != config.DefaultReconnectMin ||
		c.ReconnectMax != config.DefaultReconnectMax || c.EventQueue != config.DefaultEventQueue ||
		c.EventTimeout != config.DefaultEventTimeout || c.MaxSessionTables != config.DefaultMaxSessionTables ||
		!c.RequestResync || c.HealthTimeout != config.DefaultHealthTimeout ||
		c.MaxSourceEntries != config.DefaultMaxSourceEntries {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if s, ok := c.Source("b"); !ok || s.Address == "" {
		t.Fatal("Source lookup")
	}
	want := []output.Table{
		{Name: "lab_out", Kind: output.KindAggregate, Expiry: 30000},
		{Name: "lab_meta", Kind: output.KindMetadata, Expiry: 2000},
	}
	if got := c.OutputTables(); !slices.Equal(got, want) {
		t.Fatalf("output tables %+v, want %+v", got, want)
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "htad.json")
	if err := os.WriteFile(path, []byte(example), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(path); err != nil {
		t.Fatal(err)
	}
	big := filepath.Join(dir, "big.json")
	if err := os.WriteFile(big, make([]byte, config.MaxFileSize+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(big); !errors.Is(err, config.ErrInvalid) {
		t.Fatalf("oversized file: %v", err)
	}
	if _, err := config.Load(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("missing file accepted")
	}
}

func TestInvalid(t *testing.T) {
	edit := func(old, repl string) string {
		if !strings.Contains(example, old) {
			t.Fatalf("example lacks %q", old)
		}
		return strings.Replace(example, old, repl, 1)
	}
	addField := func(field string) string { return edit(`"local_peer": "agg",`, `"local_peer": "agg", `+field+`,`) }
	cases := map[string]string{
		"no plaintext switch":      edit(`"insecure_plaintext_loopback_lab": true`, `"insecure_plaintext_loopback_lab": false`),
		"unknown field":            addField(`"tls": {}`),
		"trailing data":            example + `{}`,
		"not json":                 `local_peer = agg`,
		"empty local peer":         edit(`"local_peer": "agg"`, `"local_peer": ""`),
		"local peer with space":    edit(`"local_peer": "agg"`, `"local_peer": "a g"`),
		"non-loopback listen":      edit(`"127.0.0.1:10000"`, `"10.0.0.1:10000"`),
		"wildcard listen":          edit(`"127.0.0.1:10000"`, `"0.0.0.0:10000"`),
		"hostname listen":          edit(`"127.0.0.1:10000"`, `"localhost:10000"`),
		"listen without port":      edit(`"127.0.0.1:10000"`, `"127.0.0.1"`),
		"non-loopback source":      edit(`"127.0.0.1:10001"`, `"192.0.2.1:10001"`),
		"source port 0":            edit(`"127.0.0.1:10001"`, `"127.0.0.1:0"`),
		"no sources":               edit(`{"name": "a"},`+"\n"+`    {"name": "b", "address": "127.0.0.1:10001"}`, ``),
		"duplicate source":         edit(`{"name": "a"}`, `{"name": "b"}`),
		"source named local peer":  edit(`{"name": "a"}`, `{"name": "agg"}`),
		"inbound source no listen": edit(`"listen": "127.0.0.1:10000",`, ``),
		"no tables":                edit(`[{"name": "lab_in", "period": "10s"}]`, `[]`),
		"duplicate table":          edit(`[{"name": "lab_in", "period": "10s"}]`, `[{"name": "x", "period": "1s"}, {"name": "x", "period": "1s"}]`),
		"table without period":     edit(`{"name": "lab_in", "period": "10s"}`, `{"name": "lab_in"}`),
		"sub-millisecond period":   edit(`"10s"`, `"1500us"`),
		"bad duration":             edit(`"10s"`, `"ten seconds"`),
		"numeric duration":         edit(`"10s"`, `10`),
		"bad table name":           edit(`"lab_in"`, `"lab in"`),
		"heartbeat too long":       addField(`"heartbeat": "5s"`),
		"idle below minimum":       addField(`"idle_timeout": "3s"`),
		"reconnect inverted":       addField(`"reconnect_min": "2s", "reconnect_max": "1s"`),
		"event timeout too long":   addField(`"event_timeout": "3s"`),
		"event queue zero":         addField(`"event_queue": 0`),
		"too many session tables":  addField(`"max_session_tables": 70000`),
		"timing too short":         addField(`"handshake_timeout": "1ms"`),
		"duplicate key":            addField(`"local_peer": "other"`),
		"trailing comma in array":  edit(`{"name": "a"},`, `{"name": "a"},,`),
		"missing comma in array":   edit(`{"name": "a"},`, `{"name": "a"}`),
		"scalars in array":         edit(`[{"name": "lab_in", "period": "10s"}]`, `[1 2]`),
		"truncated":                strings.SplitAfter(example, `{"name": "a"}`)[0],
		"trailing comma in object": edit(`{"name": "a"}`, `{"name": "a",}`),
		"deep nesting":             addField(`"sources2": ` + strings.Repeat("[", 20000)),
		"case-folded key":          edit(`"insecure_plaintext_loopback_lab"`, `"Insecure_Plaintext_Loopback_Lab"`),
		"case-folded nested key":   edit(`{"name": "a"}`, `{"NAME": "a"}`),
		"duplicate nested key":     edit(`{"name": "a"}`, `{"name": "a", "name": "c"}`),
		"output named like input":  edit(`"name": "lab_out"`, `"name": "lab_in"`),
		"duplicate output":         edit(`"name": "lab_meta"`, `"name": "lab_out"`),
		"bad output name":          edit(`"name": "lab_out"`, `"name": "lab out"`),
		"unknown output kind":      edit(`"kind": "metadata"`, `"kind": "readiness"`),
		"long metadata expire":     edit(`"expire": "2s"`, `"expire": "2001ms"`),
		"output without kind":      edit(`"kind": "metadata", `, ``),
		"output without expire":    edit(`, "expire": "30s"}`, `}`),
		"output expire too long":   edit(`"expire": "30s"`, `"expire": "600h"`),
		"output expire sub-ms":     edit(`"expire": "30s"`, `"expire": "1500us"`),
		"more tables than session": addField(`"max_session_tables": 2`),
		"outputs without resync":   addField(`"request_resync": false`),
		"resync off, no outputs":   strings.Split(example, ",\n  \"outputs\"")[0] + `, "request_resync": false}`,
		"short health timeout":     addField(`"health_timeout": "3s"`),
		"long health timeout":      addField(`"health_timeout": "9s"`),
		"no source entries":        addField(`"max_source_entries": 0`),
		"too many source entries":  addField(`"max_source_entries": 4194305`),
		"more inputs than session": edit(`[{"name": "lab_in", "period": "10s"}]`,
			`[{"name": "x", "period": "1s"}, {"name": "y", "period": "1s"}], "max_session_tables": 1`),
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				_, err := config.Parse([]byte(doc))
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, config.ErrInvalid) {
					t.Fatalf("accepted: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Parse did not return")
			}
		})
	}
}

func TestValidVariants(t *testing.T) {
	cases := map[string]string{
		"ipv6 loopback":    strings.Replace(example, `"127.0.0.1:10000"`, `"[::1]:10000"`, 1),
		"ephemeral listen": strings.Replace(example, `"127.0.0.1:10000"`, `"127.0.0.1:0"`, 1),
		"outbound only": strings.Replace(strings.Replace(example, `"listen": "127.0.0.1:10000",`, ``, 1),
			`{"name": "a"}`, `{"name": "a", "address": "127.0.0.2:1"}`, 1),
		"no outputs": strings.Split(example, ",\n  \"outputs\"")[0] + "\n}",
		"timing set": strings.Replace(strings.Split(example, ",\n  \"outputs\"")[0]+"\n}", `"local_peer": "agg",`,
			`"local_peer": "agg", "heartbeat": "1s", "idle_timeout": "4s", "event_timeout": "500ms", "request_resync": true, `+
				`"health_timeout": "4s", "max_source_entries": 10,`, 1),
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			c, err := config.Parse([]byte(doc))
			if err != nil {
				t.Fatal(err)
			}
			if name == "timing set" && (c.Heartbeat != time.Second || !c.RequestResync ||
				c.HealthTimeout != 4*time.Second || c.MaxSourceEntries != 10) {
				t.Fatalf("%+v", c)
			}
			if name == "no outputs" && len(c.Outputs) != 0 {
				t.Fatalf("outputs %+v", c.Outputs)
			}
		})
	}
}

func TestCheckPlaintextGate(t *testing.T) {
	c, err := config.Parse([]byte(example))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CheckPlaintextGate(); err != nil {
		t.Fatalf("validated config: %v", err)
	}
	for name, mutate := range map[string]func(*config.Config){
		"switch unset":        func(c *config.Config) { c.InsecurePlaintextLoopbackLab = false },
		"wildcard listen":     func(c *config.Config) { c.Listen = "0.0.0.0:10000" },
		"hostname listen":     func(c *config.Config) { c.Listen = "localhost:10000" },
		"off-host source":     func(c *config.Config) { c.Sources[1].Address = "192.0.2.1:10001" },
		"source port 0":       func(c *config.Config) { c.Sources[1].Address = "127.0.0.1:0" },
		"zoned ipv6 loopback": func(c *config.Config) { c.Listen = "[::1%lo]:1" },
	} {
		cc := c
		cc.Sources = append([]config.Source(nil), c.Sources...)
		mutate(&cc)
		if err := cc.CheckPlaintextGate(); !errors.Is(err, config.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
