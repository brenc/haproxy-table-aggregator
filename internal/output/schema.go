// Package output holds the aggregator's published output: the schema of
// the output and metadata stick tables it writes into each HAProxy source
// over the ordinary peers protocol, the readiness lease that makes the
// output authoritative, and the Store that peers sessions teach from.
//
// # Schema version 2
//
// Every output table is an IPv6-keyed HAProxy stick table that stores
// exactly gpt(4), an array of four unsigned 32-bit general-purpose tags,
// and is attached only to the peers section shared with the aggregator:
//
//	backend lab_out
//	    stick-table type ipv6 size 1m expire 30s store gpt(4) peers agg
//	backend lab_meta
//	    stick-table type ipv6 size 16 expire 2s store gpt(4) peers agg
//
// No request rule tracks or increments an output table; HAProxy only
// reads it with the table_gpt converter. That converter returns 0 for a
// missing key, so slot 0 carries a non-zero schema version: a lookup that
// does not see SchemaVersion there has no usable output, whatever the
// other slots read.
//
// An aggregate table (KindAggregate) is keyed by the same expression as the
// input table it summarizes (src,ipmask(32,64) in the example rules):
//
//	slot 0  SchemaVersion (2)
//	slot 1  aggregate rate, in estimated requests per configured period
//	        of the input's http_req_rate (not per second), 0 to
//	        math.MaxUint32; a larger rate is written as math.MaxUint32,
//	        a lower bound that still exceeds any smaller limit (package
//	        publish)
//	slot 2  generation of the peers session that wrote it, never 0
//	slot 3  reserved, always 0
//
// A metadata table (KindMetadata) holds one entry, under MetadataKey: the
// lease marker. Sessions write it from the Store's lease; it is never
// set directly:
//
//	slot 0  SchemaVersion (2)
//	slot 1  lease deadline: the low 32 bits of the Unix time in
//	        milliseconds, on the aggregator's wall clock, until which
//	        the output is authoritative; 0 in a revocation
//	slot 2  generation of the peers session that wrote it, or 0 in a
//	        revocation
//	slot 3  reserved, always 0
//
// # Authority
//
// An aggregate entry is authoritative for a request only while all of
// these hold, evaluated by HAProxy itself:
//
//   - the metadata entry exists and carries SchemaVersion (HAProxy
//     removes it at most its relative lifetime after its last delivery);
//   - its deadline is in the future and at most MaxLease ahead of
//     HAProxy's wall clock: (deadline - date(0,ms)) mod 2^32 is at most
//     LeaseWindowMillis;
//   - the aggregate entry carries SchemaVersion and the same non-zero
//     generation as the metadata entry.
//
// Otherwise the request falls back to local protection. Every proxy
// keeps its local limit active either way; authority only adds the
// aggregate limit. Missing keys, missing or revoked metadata, an expired
// deadline, and entries written by any other session therefore all select
// local protection.
//
// Each peers session picks a random non-zero generation and writes it
// into every entry it sends, values and markers alike, so a marker
// certifies only values its own session wrote, in order, ahead of it.
// HAProxy has more writers than the session: on a soft reload the old
// process teaches the new one whatever it held, and that teach can land
// after the session's newer values. Such entries carry an older
// session's generation, so a fresh marker never certifies them; an older
// session's marker taught the same way certifies only its own session's
// values, and only until its own deadline.
//
// The marker is sent as a timed update whose remaining lifetime is the
// time left until the deadline, so HAProxy also drops it on its own
// clock. A delayed marker carries its original absolute deadline, so it
// cannot grant a fresh lease to data that waited in a buffer, and a
// reload's internal resynchronization carries both the deadline and the
// remaining lifetime unchanged. The absolute deadline assumes that the
// aggregator's and HAProxy's wall clocks agree; see the phase 06 plan for
// the skew bound and the behavior outside it.
//
// Example lookups (HAProxy configuration):
//
//	http-request set-var(txn.agg_key) src,ipmask(32,64)
//	http-request set-var(txn.agg_now) date(0,ms),and(4294967295)
//	http-request set-var(txn.agg_left) ipv6(::),table_gpt(1,lab_meta),sub(txn.agg_now),and(4294967295)
//	http-request set-var(txn.agg_gen) ipv6(::),table_gpt(2,lab_meta)
//	acl agg_meta  ipv6(::),table_gpt(0,lab_meta) eq 2
//	acl agg_lease var(txn.agg_left) -m int le 2000
//	acl agg_v2    var(txn.agg_key),table_gpt(0,lab_out) eq 2
//	acl agg_gen   var(txn.agg_key),table_gpt(2,lab_out),sub(txn.agg_gen) eq 0
//	acl agg_over  var(txn.agg_key),table_gpt(1,lab_out) ge 1000
//	http-request deny deny_status 429 if agg_meta agg_lease agg_v2 agg_gen agg_over
package output

import (
	"fmt"
	"math"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

// SchemaVersion is the output schema version this package writes into
// slot 0 of every entry.
const SchemaVersion = 2

// Slots is the gpt array length of every output table: gpt(4).
const Slots = 4

// Slot indices, as used by table_gpt(<idx>,<table>).
const (
	// SlotVersion holds SchemaVersion in every entry.
	SlotVersion = 0
	// SlotRate holds an aggregate table's rate.
	SlotRate = 1
	// SlotDeadline holds the metadata entry's lease deadline.
	SlotDeadline = 1
	// SlotGeneration holds the writing session's generation in every
	// entry.
	SlotGeneration = 2
	// SlotReserved is reserved; version 2 writes 0.
	SlotReserved = 3
)

// MaxLease is the bound HAProxy's ACL places on the time left until a
// deadline, and the metadata table's expire: aggregate authority ends
// within it of the last valid publication. Leases themselves are capped
// at MaxLeaseLength.
const MaxLease = 2 * time.Second

// ClockBudget is the accepted disagreement between the aggregator's wall
// clock and every HAProxy host's (owner decision of 2026-10-04: NTP keeps
// them within 1 s).
const ClockBudget = time.Second

// MaxLeaseLength is the longest lease Store.SetLease accepts: MaxLease
// minus ClockBudget. A marker's authority ends at its deadline as HAProxy's
// clock reads it, so with clocks within the budget even a marker delivered
// late cannot keep authority more than MaxLease past the data it
// certifies.
const MaxLeaseLength = MaxLease - ClockBudget

// LeaseWindowMillis is MaxLease in milliseconds, the constant the ACL
// compares (deadline - now) mod 2^32 against.
const LeaseWindowMillis = 2000

// Values is one entry's gpt array, slot 0 first.
type Values [Slots]uint32

// Kind is the role of an output table, which fixes its slot layout.
type Kind uint8

// Output table kinds.
const (
	// KindAggregate is a per-key aggregate rate table.
	KindAggregate Kind = iota + 1
	// KindMetadata is the single-entry metadata (lease marker) table.
	KindMetadata
)

// String returns the kind's configuration name.
func (k Kind) String() string {
	switch k {
	case KindAggregate:
		return "aggregate"
	case KindMetadata:
		return "metadata"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// ParseKind parses a kind's configuration name.
func ParseKind(s string) (Kind, error) {
	switch s {
	case "aggregate":
		return KindAggregate, nil
	case "metadata":
		return KindMetadata, nil
	default:
		return 0, fmt.Errorf("output table kind %q: want aggregate or metadata", s)
	}
}

// MetadataKey is the key of the metadata table's single entry: the IPv6
// unspecified address, written ipv6(::) in an HAProxy expression.
var MetadataKey peermsg.Key

// Definition returns the peers-protocol definition of an output table
// named name whose HAProxy entries expire after expiry: IPv6 keys and
// gpt(Slots). The aggregator announces exactly this, and requires the
// source to announce the same for the table.
func Definition(name string, expiry peermsg.Millis) peermsg.Definition {
	return peermsg.Definition{
		Name:    name,
		KeyType: peermsg.KeyTypeIPv6,
		Expiry:  expiry,
		Fields:  []peermsg.Field{{Type: peermsg.DataGPT, ArrayLen: Slots}},
	}
}

// AggregateValues returns an aggregate entry carrying rate. Its
// generation slot is 0; each session fills in its own when it writes the
// entry. Narrowing a larger rate is the caller's policy: package publish
// writes math.MaxUint32 for a rate that does not fit, and never wraps.
func AggregateValues(rate uint32) Values {
	return Values{SlotVersion: SchemaVersion, SlotRate: rate}
}

// MarkerValues returns the metadata entry of a valid lease: deadline is
// the low 32 bits of the deadline's Unix time in milliseconds and gen the
// writing session's generation (non-zero).
func MarkerValues(deadline, gen uint32) Values {
	return Values{SlotVersion: SchemaVersion, SlotDeadline: deadline, SlotGeneration: gen}
}

// RevocationValues returns the metadata entry that withdraws authority at
// once: no aggregate entry carries generation 0, so it never matches.
func RevocationValues() Values {
	return Values{SlotVersion: SchemaVersion}
}

// DeadlineValue returns the low 32 bits of t's Unix time in milliseconds,
// the form HAProxy compares with date(0,ms),and(4294967295).
func DeadlineValue(t time.Time) uint32 {
	ms := t.UnixMilli() % (1 << 32)
	if ms < 0 {
		ms += 1 << 32
	}
	if ms < 0 || ms > math.MaxUint32 {
		return 0 // unreachable: ms is reduced modulo 2^32
	}
	return uint32(ms)
}

// LeaseLeft returns what HAProxy's ACL computes from a deadline value at
// wall time now: (deadline - now) mod 2^32 in milliseconds. A lease is
// live while it is at most LeaseWindowMillis.
func LeaseLeft(deadline uint32, now time.Time) uint32 {
	return deadline - DeadlineValue(now)
}

// checkAggregate validates v for an aggregate entry as published: slot 0
// must be SchemaVersion and the generation and reserved slots 0.
func checkAggregate(v Values) error {
	if v[SlotVersion] != SchemaVersion {
		return fmt.Errorf("slot %d is %d, want schema version %d", SlotVersion, v[SlotVersion], SchemaVersion)
	}
	for _, i := range []int{SlotGeneration, SlotReserved} {
		if v[i] != 0 {
			return fmt.Errorf("slot %d is %d, want 0 (sessions write the generation)", i, v[i])
		}
	}
	return nil
}
