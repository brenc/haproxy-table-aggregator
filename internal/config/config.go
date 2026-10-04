// Package config loads and validates the aggregator daemon's configuration:
// a strict JSON document naming the local peer identity, the optional
// listen address, the explicit source peers, the input tables, the output
// tables, and session timing. Nothing is discovered from the environment.
//
// Until mutual TLS exists (phase 13), every peers session is plaintext, so a
// configuration is accepted only when it sets the explicit isolated-lab
// switch "insecure_plaintext_loopback_lab" and every address is a loopback
// IP literal.
//
// Example:
//
//	{
//	  "local_peer": "agg",
//	  "insecure_plaintext_loopback_lab": true,
//	  "listen": "127.0.0.1:10000",
//	  "sources": [
//	    {"name": "a"},
//	    {"name": "b", "address": "127.0.0.1:10001"}
//	  ],
//	  "tables": [{"name": "lab_in", "period": "10s"}],
//	  "outputs": [
//	    {"name": "lab_out", "kind": "aggregate", "expire": "30s"},
//	    {"name": "lab_meta", "kind": "metadata", "expire": "2s"}
//	  ]
//	}
//
// Output tables are optional; their layout is fixed by package output.
// Each must match the source's own stick table of that name (gpt(4) and
// the same expire), no output name may be an input table name, a
// metadata table expires after at most output.MaxLease, and outputs need
// request_resync (the default).
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/output"
	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// MaxFileSize bounds the configuration file.
const MaxFileSize = 1 << 20

// Bounds on list sizes. They keep a mistaken configuration from creating
// unbounded per-source or per-table state.
const (
	MaxSources = 64
	MaxTables  = 64
)

// Defaults applied to omitted settings.
const (
	// DefaultHeartbeat matches HAProxy's own heartbeat interval.
	DefaultHeartbeat        = 3 * time.Second
	DefaultIdleTimeout      = 10 * time.Second
	DefaultHandshakeTimeout = 5 * time.Second
	DefaultReconnectMin     = 100 * time.Millisecond
	DefaultReconnectMax     = 5 * time.Second
	DefaultEventQueue       = 1024
	DefaultEventTimeout     = time.Second
	DefaultMaxSessionTables = 32
)

// Limits on timing settings. HAProxy declares a peer dead when a whole
// 5-second reconnect window passes without a message from it, so the
// heartbeat interval must stay below that with margin. HAProxy itself
// heartbeats every 3 seconds when idle, so an idle timeout below 4 seconds
// would drop quiet but healthy sources.
const (
	MaxHeartbeat   = 4 * time.Second
	MinIdleTimeout = 4 * time.Second
	maxTiming      = time.Minute
	minTiming      = 10 * time.Millisecond
	maxEventQueue  = 1 << 20
)

// ErrInvalid is wrapped by every validation error.
var ErrInvalid = errors.New("config: invalid configuration")

// Config is a validated configuration. Durations are positive and every
// field is filled in, defaults included.
type Config struct {
	// InsecurePlaintextLoopbackLab records the explicit switch that
	// permits plaintext sessions on loopback; see CheckPlaintextGate.
	InsecurePlaintextLoopbackLab bool
	// LocalPeer is this daemon's peer name: the name HAProxy configures
	// for it as a "server" in its peers section, and the name it sends in
	// its own hellos.
	LocalPeer string
	// Listen is the loopback address accepting sessions that sources
	// open, or empty for none.
	Listen string
	// Sources are the configured source peers, in file order.
	Sources []Source
	// Tables are the input tables, in file order.
	Tables []Table
	// Outputs are the output tables, in file order, which is the order
	// sessions announce them in. Their names differ from every input
	// table's.
	Outputs []Output
	// Heartbeat is the longest the daemon stays silent on a session.
	Heartbeat time.Duration
	// IdleTimeout fails a session after this long without a complete
	// message from the source.
	IdleTimeout time.Duration
	// HandshakeTimeout bounds dialing plus the whole hello exchange.
	HandshakeTimeout time.Duration
	// ReconnectMin and ReconnectMax bound the jittered exponential
	// backoff between outbound connection attempts to one source.
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	// EventQueue is the capacity of the application event queue.
	EventQueue int
	// EventTimeout is how long a session waits for room in a full event
	// queue before failing without acknowledging the update.
	EventTimeout time.Duration
	// MaxSessionTables bounds the table IDs one session may bind: every
	// table the source shares with this peer counts, input or not, so it
	// is at least the number of input and output tables.
	MaxSessionTables int
	// RequestResync makes each new session ask its source for a full
	// resynchronization.
	RequestResync bool
}

// Source is one configured source peer. Its Name is both its logical
// source name and its HAProxy peer name (the "localpeer" of that HAProxy
// process), which it must present in its hello.
type Source struct {
	// Name is the source's peer name.
	Name string
	// Address is the source's peers bind (host:port) that the daemon
	// dials, or empty when the daemon only accepts sessions from it.
	Address string
}

// Table is one input table, identified by its HAProxy table name.
type Table struct {
	// Name is the stick-table name.
	Name string
	// Period is the http_req_rate period the table must announce.
	Period time.Duration
}

// Output is one output table the daemon publishes to every source.
type Output struct {
	// Name is the stick-table name.
	Name string
	// Kind fixes the slot layout (see package output).
	Kind output.Kind
	// Expire is the source table's configured expire, whole
	// milliseconds from 1ms to MaxExpire.
	Expire time.Duration
}

// MaxExpire is the largest output table expire: HAProxy stores table
// expiry as a signed 32-bit millisecond count.
const MaxExpire = time.Duration(peermsg.MaxRemaining) * time.Millisecond

// OutputTables returns the outputs as package output configures them.
func (c *Config) OutputTables() []output.Table {
	out := make([]output.Table, 0, len(c.Outputs))
	for _, o := range c.Outputs {
		//nolint:gosec // G115: Validate bounds Expire to MaxExpire, below 2^31 ms.
		out = append(out, output.Table{Name: o.Name, Kind: o.Kind, Expiry: peermsg.Millis(o.Expire.Milliseconds())})
	}
	return out
}

// Source returns the configured source with the given name.
func (c *Config) Source(name string) (Source, bool) {
	for _, s := range c.Sources {
		if s.Name == name {
			return s, true
		}
	}
	return Source{}, false
}

// Duration is a JSON duration written as a Go duration string ("3s").
type Duration time.Duration

// UnmarshalJSON parses a quoted Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string such as \"3s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// File is the JSON document. Omitted optional fields take defaults.
type File struct {
	LocalPeer                    string       `json:"local_peer"`
	InsecurePlaintextLoopbackLab bool         `json:"insecure_plaintext_loopback_lab"`
	Listen                       string       `json:"listen"`
	Sources                      []FileSource `json:"sources"`
	Tables                       []FileTable  `json:"tables"`
	Outputs                      []FileOutput `json:"outputs"`
	Heartbeat                    *Duration    `json:"heartbeat"`
	IdleTimeout                  *Duration    `json:"idle_timeout"`
	HandshakeTimeout             *Duration    `json:"handshake_timeout"`
	ReconnectMin                 *Duration    `json:"reconnect_min"`
	ReconnectMax                 *Duration    `json:"reconnect_max"`
	EventQueue                   *int         `json:"event_queue"`
	EventTimeout                 *Duration    `json:"event_timeout"`
	MaxSessionTables             *int         `json:"max_session_tables"`
	RequestResync                *bool        `json:"request_resync"`
}

// FileSource is one entry of File.Sources.
type FileSource struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

// FileTable is one entry of File.Tables.
type FileTable struct {
	Name   string    `json:"name"`
	Period *Duration `json:"period"`
}

// FileOutput is one entry of File.Outputs.
type FileOutput struct {
	Name   string    `json:"name"`
	Kind   string    `json:"kind"`
	Expire *Duration `json:"expire"`
}

// Load reads and validates the configuration file at path.
func Load(path string) (Config, error) {
	f, err := os.Open(path) //nolint:gosec // G304: the operator names the config file.
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, MaxFileSize+1))
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if len(b) > MaxFileSize {
		return Config{}, fmt.Errorf("%w: file exceeds %d bytes", ErrInvalid, MaxFileSize)
	}
	return Parse(b)
}

// Parse decodes and validates a JSON configuration. Unknown fields,
// duplicate keys, keys that differ from the documented names only in case
// (which encoding/json would accept), duplicate documents, and trailing
// data are errors.
func Parse(b []byte) (Config, error) {
	if err := checkKeys(json.NewDecoder(bytes.NewReader(b)), reflect.TypeFor[File](), ""); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var f File
	if err := dec.Decode(&f); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("%w: trailing data after the configuration object", ErrInvalid)
	}
	return f.Validate()
}

// checkKeys walks the next JSON value against type t and rejects object
// keys that are repeated or are not exactly a json tag of t. Syntax errors
// are returned: the walk cannot continue past one.
func checkKeys(dec *json.Decoder, t reflect.Type, path string) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch tok {
	case json.Delim('{'):
		fields := map[string]reflect.Type{}
		if t.Kind() == reflect.Struct {
			for f := range t.Fields() {
				name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
				fields[name] = f.Type
			}
		}
		seen := map[string]bool{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return err
			}
			key, _ := kt.(string)
			if seen[key] {
				return fmt.Errorf("duplicate key %q%s", key, path)
			}
			seen[key] = true
			ft, ok := fields[key]
			if !ok {
				return fmt.Errorf("unknown key %q%s", key, path)
			}
			if err := checkKeys(dec, ft, " in "+key); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	case json.Delim('['):
		elem := t
		if t.Kind() == reflect.Slice {
			elem = t.Elem()
		}
		for dec.More() {
			if err := checkKeys(dec, elem, path); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	}
	return nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Validate checks f and returns the configuration with defaults applied.
func (f File) Validate() (Config, error) {
	if !f.InsecurePlaintextLoopbackLab {
		return Config{}, invalid("mutual TLS is not implemented yet, so peers sessions are plaintext; " +
			"set insecure_plaintext_loopback_lab to true to run an isolated loopback lab")
	}
	if _, err := peerwire.ParsePeerNameLine([]byte(f.LocalPeer)); err != nil {
		return Config{}, invalid("local_peer: %v", err)
	}
	c := Config{LocalPeer: f.LocalPeer, InsecurePlaintextLoopbackLab: true}
	if f.Listen != "" {
		if err := checkLoopback(f.Listen, true); err != nil {
			return Config{}, invalid("listen: %v", err)
		}
		c.Listen = f.Listen
	}
	if err := c.addSources(f.Sources); err != nil {
		return Config{}, err
	}
	if err := c.addTables(f.Tables); err != nil {
		return Config{}, err
	}
	if err := c.addOutputs(f.Outputs); err != nil {
		return Config{}, err
	}
	if err := c.setTiming(f); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c *Config) addSources(sources []FileSource) error {
	if len(sources) == 0 || len(sources) > MaxSources {
		return invalid("sources: need 1 to %d sources, have %d", MaxSources, len(sources))
	}
	seen := map[string]bool{}
	for i, s := range sources {
		if _, err := peerwire.ParsePeerNameLine([]byte(s.Name)); err != nil {
			return invalid("sources[%d].name: %v", i, err)
		}
		if s.Name == c.LocalPeer {
			return invalid("sources[%d].name: %q is local_peer", i, s.Name)
		}
		if seen[s.Name] {
			return invalid("sources[%d].name: duplicate source %q", i, s.Name)
		}
		seen[s.Name] = true
		if s.Address == "" {
			if c.Listen == "" {
				return invalid("sources[%d]: no address and no listen address, so no session could ever reach it", i)
			}
		} else if err := checkLoopback(s.Address, false); err != nil {
			return invalid("sources[%d].address: %v", i, err)
		}
		c.Sources = append(c.Sources, Source(s))
	}
	return nil
}

func (c *Config) addTables(tables []FileTable) error {
	if len(tables) == 0 || len(tables) > MaxTables {
		return invalid("tables: need 1 to %d tables, have %d", MaxTables, len(tables))
	}
	seen := map[string]bool{}
	for i, t := range tables {
		if err := (peermsg.Definition{Name: t.Name, KeyType: peermsg.KeyTypeIPv6, Expiry: 1}).Validate(); err != nil {
			return invalid("tables[%d].name: %v", i, err)
		}
		if seen[t.Name] {
			return invalid("tables[%d].name: duplicate table %q", i, t.Name)
		}
		seen[t.Name] = true
		if t.Period == nil {
			return invalid("tables[%d].period: required", i)
		}
		p := time.Duration(*t.Period)
		if p < time.Millisecond || p%time.Millisecond != 0 || p.Milliseconds() > math.MaxUint32 {
			return invalid("tables[%d].period: %v must be whole milliseconds from 1ms to %d ms", i, p, uint64(math.MaxUint32))
		}
		c.Tables = append(c.Tables, Table{Name: t.Name, Period: p})
	}
	return nil
}

func (c *Config) addOutputs(outputs []FileOutput) error {
	if len(outputs) > MaxTables {
		return invalid("outputs: at most %d output tables, have %d", MaxTables, len(outputs))
	}
	seen := map[string]bool{}
	for _, t := range c.Tables {
		seen[t.Name] = true
	}
	for i, o := range outputs {
		if err := (peermsg.Definition{Name: o.Name, KeyType: peermsg.KeyTypeIPv6, Expiry: 1}).Validate(); err != nil {
			return invalid("outputs[%d].name: %v", i, err)
		}
		if seen[o.Name] {
			return invalid("outputs[%d].name: %q is already an input or output table; the aggregator never "+
				"writes input tables", i, o.Name)
		}
		seen[o.Name] = true
		kind, err := output.ParseKind(o.Kind)
		if err != nil {
			return invalid("outputs[%d].kind: %v", i, err)
		}
		if o.Expire == nil {
			return invalid("outputs[%d].expire: required; it must equal the HAProxy table's expire", i)
		}
		e := time.Duration(*o.Expire)
		if e < time.Millisecond || e%time.Millisecond != 0 || e > MaxExpire {
			return invalid("outputs[%d].expire: %v must be whole milliseconds from 1ms to %v", i, e, MaxExpire)
		}
		if kind == output.KindMetadata && e > output.MaxLease {
			return invalid("outputs[%d].expire: a metadata table holds the lease marker, so it expires after at "+
				"most %v, not %v", i, output.MaxLease, e)
		}
		c.Outputs = append(c.Outputs, Output{Name: o.Name, Kind: kind, Expire: e})
	}
	return nil
}

func durationOr(d *Duration, def time.Duration) time.Duration {
	if d == nil {
		return def
	}
	return time.Duration(*d)
}

func (c *Config) setTiming(f File) error {
	c.Heartbeat = durationOr(f.Heartbeat, DefaultHeartbeat)
	c.IdleTimeout = durationOr(f.IdleTimeout, DefaultIdleTimeout)
	c.HandshakeTimeout = durationOr(f.HandshakeTimeout, DefaultHandshakeTimeout)
	c.ReconnectMin = durationOr(f.ReconnectMin, DefaultReconnectMin)
	c.ReconnectMax = durationOr(f.ReconnectMax, DefaultReconnectMax)
	c.EventTimeout = durationOr(f.EventTimeout, DefaultEventTimeout)
	for _, d := range []struct {
		name string
		v    time.Duration
	}{
		{"heartbeat", c.Heartbeat},
		{"idle_timeout", c.IdleTimeout},
		{"handshake_timeout", c.HandshakeTimeout},
		{"reconnect_min", c.ReconnectMin},
		{"reconnect_max", c.ReconnectMax},
		{"event_timeout", c.EventTimeout},
	} {
		if d.v < minTiming || d.v > maxTiming {
			return invalid("%s: %v is outside %v..%v", d.name, d.v, minTiming, maxTiming)
		}
	}
	switch {
	case c.Heartbeat > MaxHeartbeat:
		return invalid("heartbeat: %v exceeds %v; HAProxy drops a peer silent for a whole 5s window", c.Heartbeat, MaxHeartbeat)
	case c.IdleTimeout < MinIdleTimeout:
		return invalid("idle_timeout: %v is below %v; HAProxy heartbeats only every 3s", c.IdleTimeout, MinIdleTimeout)
	case c.ReconnectMax < c.ReconnectMin:
		return invalid("reconnect_max %v is below reconnect_min %v", c.ReconnectMax, c.ReconnectMin)
	case c.EventTimeout >= c.Heartbeat:
		return invalid("event_timeout %v must be below heartbeat %v so a slow consumer cannot silence a session",
			c.EventTimeout, c.Heartbeat)
	}
	c.EventQueue = DefaultEventQueue
	if f.EventQueue != nil {
		c.EventQueue = *f.EventQueue
	}
	if c.EventQueue < 1 || c.EventQueue > maxEventQueue {
		return invalid("event_queue: %d is outside 1..%d", c.EventQueue, maxEventQueue)
	}
	c.MaxSessionTables = DefaultMaxSessionTables
	if f.MaxSessionTables != nil {
		c.MaxSessionTables = *f.MaxSessionTables
	}
	if c.MaxSessionTables < 1 || c.MaxSessionTables > peermsg.MaxTables {
		return invalid("max_session_tables: %d is outside 1..%d", c.MaxSessionTables, peermsg.MaxTables)
	}
	if n := len(c.Tables) + len(c.Outputs); c.MaxSessionTables < n {
		return invalid("max_session_tables %d is below the %d configured input and output tables; every table a "+
			"source shares in a session counts, input or not", c.MaxSessionTables, n)
	}
	c.RequestResync = f.RequestResync == nil || *f.RequestResync
	if len(c.Outputs) > 0 && !c.RequestResync {
		return invalid("request_resync: outputs need it; a source announces its output tables, which the " +
			"daemon waits for before writing them, only when it teaches")
	}
	return nil
}

// CheckPlaintextGate re-checks, on a Config that may not have come from
// Validate, the rule every plaintext session depends on until mutual TLS
// exists: the explicit lab switch is set, and the listen address and every
// source address are loopback IP literals. Errors wrap ErrInvalid.
func (c *Config) CheckPlaintextGate() error {
	if !c.InsecurePlaintextLoopbackLab {
		return invalid("plaintext peers sessions need InsecurePlaintextLoopbackLab")
	}
	if c.Listen != "" {
		if err := CheckLoopbackAddr(c.Listen, true); err != nil {
			return invalid("listen: %v", err)
		}
	}
	for i, s := range c.Sources {
		if s.Address != "" {
			if err := CheckLoopbackAddr(s.Address, false); err != nil {
				return invalid("sources[%d].address: %v", i, err)
			}
		}
	}
	return nil
}

// CheckLoopbackAddr requires "ip:port" with a loopback IP literal. A port
// of 0 (an ephemeral port) is allowed only for a listen address.
func CheckLoopbackAddr(hostport string, listen bool) error {
	return checkLoopback(hostport, listen)
}

func checkLoopback(hostport string, listen bool) error {
	host, portS, err := net.SplitHostPort(hostport)
	if err != nil {
		return err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("host %q must be an IP literal: %w", host, err)
	}
	if !ip.IsLoopback() || ip.Zone() != "" {
		return fmt.Errorf("%s is not a loopback address; plaintext sessions are limited to an isolated loopback lab", ip)
	}
	port, err := strconv.ParseUint(portS, 10, 16)
	if err != nil {
		return fmt.Errorf("port %q: %w", portS, err)
	}
	if port == 0 && !listen {
		return errors.New("port 0")
	}
	return nil
}
