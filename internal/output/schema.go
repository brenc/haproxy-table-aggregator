// Package output holds the aggregator's published output: the schema of
// the output and metadata stick tables it writes into each HAProxy source
// over the ordinary peers protocol, and the Store that peers sessions
// teach from.
//
// # Schema version 1
//
// Every output table is an IPv6-keyed HAProxy stick table that stores
// exactly gpt(4), an array of four unsigned 32-bit general-purpose tags,
// and is attached only to the peers section shared with the aggregator:
//
//	backend lab_out
//	    stick-table type ipv6 size 1m expire 30s store gpt(4) peers agg
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
//	slot 0  SchemaVersion (1)
//	slot 1  aggregate rate, in estimated requests per configured period
//	        of the input's http_req_rate (not per second), 0 to
//	        math.MaxUint32; larger values saturate (see RateValue)
//	slot 2  reserved for phase 06 (publication generation), always 0
//	slot 3  reserved, always 0
//
// A metadata table (KindMetadata) holds one entry, under MetadataKey:
//
//	slot 0  SchemaVersion (1)
//	slots 1-3  reserved for phase 06 (readiness), always 0
//
// Version 1 makes no freshness or authority claim. A published entry means
// only that the aggregator wrote these integers; phase 06 defines when an
// ACL may treat aggregate output as authoritative, and until then ACLs must
// not derive authority from either table.
//
// Example lookups (HAProxy configuration):
//
//	http-request set-var(txn.key) src,ipmask(32,64)
//	acl agg_v1   var(txn.key),table_gpt(0,lab_out) eq 1
//	acl agg_over var(txn.key),table_gpt(1,lab_out) ge 1000
//	http-request deny deny_status 429 if agg_v1 agg_over
//	acl meta_v1  ipv6(::),table_gpt(0,lab_meta) eq 1
package output

import (
	"fmt"
	"math"

	"github.com/brenc/haproxy-table-aggregator/internal/peermsg"
)

// SchemaVersion is the output schema version this package writes into
// slot 0 of every entry.
const SchemaVersion = 1

// Slots is the gpt array length of every output table: gpt(4).
const Slots = 4

// Slot indices, as used by table_gpt(<idx>,<table>).
const (
	// SlotVersion holds SchemaVersion in every entry.
	SlotVersion = 0
	// SlotRate holds an aggregate table's rate.
	SlotRate = 1
	// SlotGeneration is reserved for phase 06; version 1 writes 0.
	SlotGeneration = 2
	// SlotReserved is reserved; version 1 writes 0.
	SlotReserved = 3
)

// Values is one entry's gpt array, slot 0 first.
type Values [Slots]uint32

// Kind is the role of an output table, which fixes its slot layout.
type Kind uint8

// Output table kinds.
const (
	// KindAggregate is a per-key aggregate rate table.
	KindAggregate Kind = iota + 1
	// KindMetadata is the single-entry metadata table.
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

// RateValue converts a rate to its slot value. Rates above math.MaxUint32
// saturate at math.MaxUint32, which compares at or above every limit an
// ACL can express against the slot, so saturation can only make
// enforcement stricter; it never wraps to a small value.
func RateValue(rate uint64) uint32 {
	if rate > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(rate)
}

// AggregateValues returns an aggregate entry carrying rate (see
// RateValue); the reserved slots are 0.
func AggregateValues(rate uint32) Values {
	return Values{SlotVersion: SchemaVersion, SlotRate: rate}
}

// MetadataValues returns the version 1 metadata entry: the schema version
// and reserved zeros.
func MetadataValues() Values {
	return Values{SlotVersion: SchemaVersion}
}

// check validates v for a table of kind k: slot 0 must be SchemaVersion
// and reserved slots must be 0.
func (k Kind) check(v Values) error {
	if v[SlotVersion] != SchemaVersion {
		return fmt.Errorf("slot %d is %d, want schema version %d", SlotVersion, v[SlotVersion], SchemaVersion)
	}
	first := SlotRate
	if k == KindAggregate {
		first = SlotGeneration
	}
	for i := first; i < Slots; i++ {
		if v[i] != 0 {
			return fmt.Errorf("reserved slot %d of a %v entry is %d, want 0", i, k, v[i])
		}
	}
	return nil
}
