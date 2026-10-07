package lab

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/output"
)

// Stick-table names used by every lab node. LabTable is fed only by the
// isolated lab listener, whose keys come from the synthetic ClientHeader.
// ProdTable is fed only by the production-style listener, whose keys come
// from the real connection source. ProxyTable is fed only by the
// PROXY-protocol listener, which runs the unchanged production key rule
// against a source address the harness declares in a PROXY header.
const (
	LabTable   = "lab_in"
	ProdTable  = "prod_in"
	ProxyTable = "proxy_in"
)

// Output tables, present only when the lab has an aggregator, which
// writes them over the peers protocol. No request rule tracks them; only
// the probe listener reads them. Both store gpt(4) (output schema
// version 2, package output). The aggregate table expires like the input
// tables; the metadata table, which holds the lease marker, after
// MetaExpire.
const (
	// OutputTable is the aggregate output table, keyed like LabTable.
	OutputTable = "lab_out"
	// MetaTable is the metadata table, with one entry under ::.
	MetaTable = "lab_meta"
	// OutputSlots is the gpt array length of both output tables.
	OutputSlots = 4
	// MetaExpire is MetaTable's expire: output.MaxLease.
	MetaExpire = output.MaxLease
)

// OutputLimit is the probe listener's aggregate limit: a lookup whose
// output is authoritative (see output package documentation) and whose
// rate is at or above it is answered 429, any other 200.
const OutputLimit = 1000

// HTTP headers exchanged between the harness, HAProxy, and the responder.
const (
	// ClientHeader carries the synthetic client address on the lab listener.
	// HAProxy trusts it only there; the production-style listener ignores it.
	ClientHeader = "X-Lab-Client"
	// RequestIDHeader carries the harness-assigned unique request ID.
	RequestIDHeader = "X-Lab-Request-Id"
	// NodeHeader is set by HAProxy to the node name before forwarding.
	NodeHeader = "X-Lab-Node"
	// ListenerHeader is set by HAProxy to "lab", "prod", or "proxy" before
	// forwarding.
	ListenerHeader = "X-Lab-Listener"
	// KeyHeader is set by HAProxy to the tracked key on the forwarded
	// request and on the response.
	KeyHeader = "X-Lab-Key"
	// LookupHeader is set by HAProxy on the response to the http_req_cnt it
	// looks up with the same key expression used for tracking.
	LookupHeader = "X-Lab-Lookup-Cnt"
	// The probe listener's response headers: table_gpt lookups of the
	// OutputTable entry for the probed key (version, rate, generation) and of
	// the MetaTable entry :: (version, generation), the lease time left as the
	// ACL computes it, and the authority decisions. A missing entry reads
	// 0.
	OutVersionHeader  = "X-Lab-Out-Version"
	OutRateHeader     = "X-Lab-Out-Rate"
	OutGenHeader      = "X-Lab-Out-Gen"
	MetaVersionHeader = "X-Lab-Meta-Version"
	MetaGenHeader     = "X-Lab-Meta-Gen"
	LeaseLeftHeader   = "X-Lab-Lease-Left"
	// PIDHeader is the answering HAProxy process's PID.
	PIDHeader = "X-Lab-Pid"
	// AuthorityHeader is "aggregate" when the frozen phase 06 rule makes
	// the key's aggregate entry authoritative, else "local".
	AuthorityHeader = "X-Lab-Authority"
	// RelativeAuthorityHeader applies the same rule without the absolute
	// deadline check, trusting the marker's relative lifetime alone. It
	// exists to show what that weaker rule would decide; nothing
	// enforces it.
	RelativeAuthorityHeader = "X-Lab-Relative-Authority"
	// With Aggregator.Limit set, the lab listener enforces and answers
	// with its decision: AuthorityHeader as on the probe listener, the
	// limit that denied the request in DenyHeader ("local", "aggregate",
	// or empty when none did), the local http_req_rate after tracking
	// the request in LocalRateHeader, and the aggregate output's rate
	// slot in AggRateHeader (0 for a missing entry).
	DenyHeader      = "X-Lab-Deny"
	LocalRateHeader = "X-Lab-Local-Rate"
	AggRateHeader   = "X-Lab-Agg-Rate"
)

// ExpiryFactor is how many rate periods a table entry outlives its last
// update. Decay tests need entries to survive more than two periods.
const ExpiryFactor = 3

// configParams feeds configTemplate. Listener fields are HAProxy bind
// addresses; the lab binds inherited sockets with fd@N so no port is ever
// chosen by probing and released.
type configParams struct {
	Node        string
	Socket      string
	LabBind     string
	ProdBinds   []string
	ProxyBind   string
	Responder   string
	PeriodMS    string
	ExpireMS    string
	LabTable    string
	ProdTable   string
	ProxyTable  string
	ClientHdr   string
	NodeHdr     string
	ListenerHdr string
	KeyHdr      string
	LookupHdr   string
	// Probe response headers (see probeHeaders).
	ProbeHdrs string
	// Aggregator peers section; empty AggName means none.
	AggName   string
	AggAddr   string
	PeersBind string
	// With an aggregator: the output tables, declared before the input
	// tables when OutputFirst (which changes HAProxy's table IDs), and
	// the probe listener.
	OutputFirst bool
	ProbeBind   string
	OutTable    string
	MetaTable   string
	MetaExpMS   string
	OutSlots    int
	OutLimit    int
	LeaseMS     int
	// Enforcement response headers.
	AuthHdr, DenyHdr, LocalRateHdr, AggRateHdr string
	// Limit, when non-zero, makes the lab listener enforce the full
	// per-proxy threshold: locally always, on the aggregate output while
	// it is authoritative.
	Limit int
}

// The key expression is stored once in txn.lab_key and reused for tracking,
// lookup, and the forwarded key header, so tracking and lookup can never use
// different keys. Both listeners mask IPv6 to /64 and keep IPv4 at /32.
// Nodes never replicate input counters to each other: without an aggregator
// the tables have no peers section, and with one each node's peers section
// holds only the node itself and the aggregator.
var configTemplate = template.Must(template.New("haproxy.cfg").Parse(`# Generated by the haproxy-table-aggregator lab for node {{.Node}}.
global
{{- if .AggName}}
    localpeer {{.Node}}
{{- end}}
    nbthread 1
    maxconn 256
    zero-warning
    stats socket {{.Socket}} mode 600 level admin
    stats timeout 10s

defaults
    mode http
    retries 0
    timeout connect 2s
    timeout client 10s
    timeout server 10s
{{- if .AggName}}

# Peers section shared only with the aggregator.
peers agg
    bind {{.PeersBind}}
    server {{.Node}}
    server {{.AggName}} {{.AggAddr}}
{{- end}}

{{- if .OutputFirst}}{{template "outputs" .}}{{end}}

backend {{.LabTable}}
    stick-table type ipv6 size 1k expire {{.ExpireMS}} store http_req_cnt,http_req_rate({{.PeriodMS}}){{if .AggName}} peers agg{{end}}

backend {{.ProdTable}}
    stick-table type ipv6 size 1k expire {{.ExpireMS}} store http_req_cnt,http_req_rate({{.PeriodMS}}){{if .AggName}} peers agg{{end}}

backend {{.ProxyTable}}
    stick-table type ipv6 size 1k expire {{.ExpireMS}} store http_req_cnt,http_req_rate({{.PeriodMS}}){{if .AggName}} peers agg{{end}}
{{- if and .AggName (not .OutputFirst)}}{{template "outputs" .}}{{end}}

# Isolated lab listener: the synthetic client header is trusted here only.
frontend lab
    bind {{.LabBind}}
    http-request set-var(txn.lab_key) req.hdr_ip({{.ClientHdr}}),ipmask(32,64) if { req.hdr_cnt({{.ClientHdr}}) eq 1 }
    http-request deny deny_status 400 unless { var(txn.lab_key) -m found }
    http-request track-sc0 var(txn.lab_key) table {{.LabTable}}
{{- if .Limit}}{{template "enforce" .}}{{end}}
    http-request set-header {{.NodeHdr}} {{.Node}}
    http-request set-header {{.ListenerHdr}} lab
    http-request set-header {{.KeyHdr}} %[var(txn.lab_key)]
    http-after-response set-header {{.LookupHdr}} %[var(txn.lab_key),table_http_req_cnt({{.LabTable}})]
    http-after-response set-header {{.KeyHdr}} %[var(txn.lab_key)]
    default_backend responder

# Production-style listener: keys come from the source address only.
frontend prod
{{- range .ProdBinds}}
    bind {{.}}
{{- end}}
    http-request set-var(txn.lab_key) src,ipmask(32,64)
    http-request track-sc0 var(txn.lab_key) table {{.ProdTable}}
    http-request set-header {{.NodeHdr}} {{.Node}}
    http-request set-header {{.ListenerHdr}} prod
    http-request set-header {{.KeyHdr}} %[var(txn.lab_key)]
    http-after-response set-header {{.LookupHdr}} %[var(txn.lab_key),table_http_req_cnt({{.ProdTable}})]
    http-after-response set-header {{.KeyHdr}} %[var(txn.lab_key)]
    default_backend responder

# PROXY-protocol listener: the production key rule, unchanged, fed with
# arbitrary IPv4/IPv6 sources declared by the harness. Accepting PROXY from
# any client is a test-only trust confined to this loopback listener; a
# deployment accepts it only from its own load balancers, if at all.
frontend proxy
    bind {{.ProxyBind}} accept-proxy
    http-request set-var(txn.lab_key) src,ipmask(32,64)
    http-request track-sc0 var(txn.lab_key) table {{.ProxyTable}}
    http-request set-header {{.NodeHdr}} {{.Node}}
    http-request set-header {{.ListenerHdr}} proxy
    http-request set-header {{.KeyHdr}} %[var(txn.lab_key)]
    http-after-response set-header {{.LookupHdr}} %[var(txn.lab_key),table_http_req_cnt({{.ProxyTable}})]
    http-after-response set-header {{.KeyHdr}} %[var(txn.lab_key)]
    default_backend responder

{{- if .AggName}}

# Output probe: answers from the aggregator's output with the frozen
# phase 06 authority rule (package output). It tracks nothing, so probing
# never changes an input table, and it has no local limit: 429 means the
# aggregate limit applied, 200 that it did not.
frontend probe
    bind {{.ProbeBind}}
    http-request set-var(txn.lab_key) req.hdr_ip({{.ClientHdr}}),ipmask(32,64) if { req.hdr_cnt({{.ClientHdr}}) eq 1 }
    http-request deny deny_status 400 unless { var(txn.lab_key) -m found }
    http-request set-var(txn.agg_now) date(0,ms),and(4294967295)
    http-request set-var(txn.agg_left) ipv6(::),table_gpt(1,{{.MetaTable}}),sub(txn.agg_now),and(4294967295)
    http-request set-var(txn.agg_gen) ipv6(::),table_gpt(2,{{.MetaTable}})
    acl agg_meta  ipv6(::),table_gpt(0,{{.MetaTable}}) eq 2
    acl agg_lease var(txn.agg_left) -m int le {{.LeaseMS}}
    acl agg_v2    var(txn.lab_key),table_gpt(0,{{.OutTable}}) eq 2
    acl agg_gen   var(txn.lab_key),table_gpt(2,{{.OutTable}}),sub(txn.agg_gen) eq 0
    acl agg_over  var(txn.lab_key),table_gpt(1,{{.OutTable}}) ge {{.OutLimit}}
    http-request set-var(txn.agg_auth) str(aggregate) if agg_meta agg_lease agg_v2 agg_gen
    http-request set-var(txn.agg_auth) str(local) unless agg_meta agg_lease agg_v2 agg_gen
    http-request set-var(txn.agg_rel) str(aggregate) if agg_meta agg_v2 agg_gen
    http-request set-var(txn.agg_rel) str(local) unless agg_meta agg_v2 agg_gen
    http-request return status 429 {{.ProbeHdrs}} if agg_meta agg_lease agg_v2 agg_gen agg_over
    http-request return status 200 {{.ProbeHdrs}}
{{- end}}

backend responder
    server responder {{.Responder}}
{{- define "enforce"}}
    # Enforcement (phase 10), with the frozen phase 06 authority rule.
    http-request set-var(txn.lab_lrate) sc_http_req_rate(0)
    http-request set-var(txn.agg_now) date(0,ms),and(4294967295)
    http-request set-var(txn.agg_left) ipv6(::),table_gpt(1,{{.MetaTable}}),sub(txn.agg_now),and(4294967295)
    http-request set-var(txn.agg_gen) ipv6(::),table_gpt(2,{{.MetaTable}})
    acl agg_meta  ipv6(::),table_gpt(0,{{.MetaTable}}) eq 2
    acl agg_lease var(txn.agg_left) -m int le {{.LeaseMS}}
    acl agg_v2    var(txn.lab_key),table_gpt(0,{{.OutTable}}) eq 2
    acl agg_gen   var(txn.lab_key),table_gpt(2,{{.OutTable}}),sub(txn.agg_gen) eq 0
    acl agg_over  var(txn.lab_key),table_gpt(1,{{.OutTable}}) ge {{.Limit}}
    acl local_over var(txn.lab_lrate) -m int ge {{.Limit}}
    http-request set-var(txn.lab_auth) str(aggregate) if agg_meta agg_lease agg_v2 agg_gen
    http-request set-var(txn.lab_auth) str(local) unless agg_meta agg_lease agg_v2 agg_gen
    # Local protection: always active, with the full per-proxy limit,
    # never divided by the number of proxies.
    http-request set-var(txn.lab_deny) str(local) if local_over
    http-request deny deny_status 429 if local_over
    # Aggregate protection: the same limit on the cluster-wide rate, only
    # while the output is authoritative.
    http-request set-var(txn.lab_deny) str(aggregate) if agg_meta agg_lease agg_v2 agg_gen agg_over
    http-request deny deny_status 429 if agg_meta agg_lease agg_v2 agg_gen agg_over
    http-after-response set-header {{.AuthHdr}} %[var(txn.lab_auth)]
    http-after-response set-header {{.DenyHdr}} %[var(txn.lab_deny)]
    http-after-response set-header {{.LocalRateHdr}} %[var(txn.lab_lrate)]
    http-after-response set-header {{.AggRateHdr}} %[var(txn.lab_key),table_gpt(1,{{.OutTable}})]
{{- end}}
{{- define "outputs"}}

# Output tables, written only by the aggregator over the peers protocol.
backend {{.OutTable}}
    stick-table type ipv6 size 1k expire {{.ExpireMS}} store gpt({{.OutSlots}}) peers agg

backend {{.MetaTable}}
    stick-table type ipv6 size 16 expire {{.MetaExpMS}} store gpt({{.OutSlots}}) peers agg
{{- end}}
`))

func renderConfig(p configParams) ([]byte, error) {
	var buf bytes.Buffer
	if err := configTemplate.Execute(&buf, p); err != nil {
		return nil, fmt.Errorf("render haproxy config: %w", err)
	}
	return buf.Bytes(), nil
}

// probeHeaders renders the probe listener's response headers.
func probeHeaders(outTable, metaTable string) string {
	out := func(slot int) string {
		return fmt.Sprintf("%%[var(txn.lab_key),table_gpt(%d,%s)]", slot, outTable)
	}
	hdrs := []struct{ name, value string }{
		{KeyHeader, "%[var(txn.lab_key)]"},
		{OutVersionHeader, out(0)},
		{OutRateHeader, out(1)},
		{OutGenHeader, out(2)},
		{MetaVersionHeader, fmt.Sprintf("%%[ipv6(::),table_gpt(0,%s)]", metaTable)},
		{MetaGenHeader, "%[var(txn.agg_gen)]"},
		{LeaseLeftHeader, "%[var(txn.agg_left)]"},
		{AuthorityHeader, "%[var(txn.agg_auth)]"},
		{RelativeAuthorityHeader, "%[var(txn.agg_rel)]"},
		{PIDHeader, "%[pid]"},
	}
	parts := make([]string, len(hdrs))
	for i, h := range hdrs {
		parts[i] = fmt.Sprintf("hdr %s %q", h.name, h.value)
	}
	return strings.Join(parts, " ")
}

func millis(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10)
}
