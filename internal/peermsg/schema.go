package peermsg

import (
	"fmt"
	"net/netip"
	"time"
)

// Millis is a duration in whole milliseconds as carried on the wire: a
// table expiry, a counter period, a remaining entry lifetime, or a
// counter age. It is never an absolute time; HAProxy's millisecond clock
// is private to each process and wraps.
type Millis uint32

// Duration converts m to a time.Duration.
func (m Millis) Duration() time.Duration { return time.Duration(m) * time.Millisecond }

// String formats m as a duration.
func (m Millis) String() string { return m.Duration().String() }

// KeyType is a stick-table key type as numbered on the wire (HAProxy's
// PEER_KT_* values, which differ from its internal sample types).
type KeyType uint8

// KeyTypeIPv6 is the only supported key type: a 16-byte IPv6 address.
// HAProxy stores IPv4 sources in IPv6 tables as IPv4-mapped addresses.
const KeyTypeIPv6 KeyType = 5

// KeyLen returns the fixed key length for t, or 0 if t is unsupported.
func (t KeyType) KeyLen() uint32 {
	if t == KeyTypeIPv6 {
		return IPv6KeyLen
	}
	return 0
}

// IPv6KeyLen is the key length of an IPv6 table.
const IPv6KeyLen = 16

// Key is an IPv6 stick-table key exactly as stored and sent by HAProxy.
// Equality is byte equality: an IPv4-mapped address (::ffff:a.b.c.d), an
// IPv4-compatible address (::a.b.c.d), and a native IPv6 address are
// distinct keys even when they embed the same IPv4 address.
type Key [IPv6KeyLen]byte

// KeyFromAddr returns the key HAProxy stores for a in an IPv6 table: a
// 16-byte address unchanged, or a 4-byte IPv4 address as its IPv4-mapped
// form. It fails for an invalid address or one with a zone.
func KeyFromAddr(a netip.Addr) (Key, error) {
	if !a.IsValid() || a.Zone() != "" {
		return Key{}, fmt.Errorf("peermsg: invalid key address %v", a)
	}
	return Key(a.As16()), nil
}

// Addr returns k as a 16-byte address. It does not unmap IPv4-mapped
// keys, so distinct keys always give distinct addresses.
func (k Key) Addr() netip.Addr { return netip.AddrFrom16(k) }

// String formats k with netip.Addr.String: IPv4-mapped keys as
// ::ffff:a.b.c.d, others in RFC 5952 IPv6 form. That matches HAProxy's
// "show table" except for IPv4-compatible keys, which Go prints in hex
// (::c633:6407) and HAProxy dotted (::198.51.100.7); both parse to the
// same address, so compare parsed addresses, not strings.
func (k Key) String() string { return k.Addr().String() }

// DataType is a stick-table stored data type as numbered on the wire
// (HAProxy's STKTABLE_DT_* values, bit positions in a definition's data
// types bitfield).
type DataType uint8

// Supported data types. Every other type is rejected with ErrDataType.
const (
	// DataHTTPReqCnt is http_req_cnt, an unsigned 32-bit counter.
	DataHTTPReqCnt DataType = 9
	// DataHTTPReqRate is http_req_rate(period), a frequency counter.
	DataHTTPReqRate DataType = 10
	// DataGPT is gpt(n), an array of n unsigned 32-bit general-purpose
	// tags: the selected output form.
	DataGPT DataType = 22
)

// MaxArrayLen is the largest array element count HAProxy accepts
// (STKTABLE_MAX_DT_ARRAY_SIZE).
const MaxArrayLen = 100

// maxDataTypes is the width of the wire data types bitfield.
const maxDataTypes = 64

// String returns HAProxy's name for a supported type, or "data_type(N)".
func (t DataType) String() string {
	switch t {
	case DataHTTPReqCnt:
		return "http_req_cnt"
	case DataHTTPReqRate:
		return "http_req_rate"
	case DataGPT:
		return "gpt"
	default:
		return fmt.Sprintf("data_type(%d)", uint8(t))
	}
}

// valueKind is the shape of a supported data type's value.
type valueKind uint8

const (
	kindUnsupported valueKind = iota
	kindUint                  // one unsigned 32-bit integer
	kindFreq                  // a frequency counter, with a period
	kindUintArray             // an array of unsigned 32-bit integers
)

func (t DataType) kind() valueKind {
	switch t {
	case DataHTTPReqCnt:
		return kindUint
	case DataHTTPReqRate:
		return kindFreq
	case DataGPT:
		return kindUintArray
	default:
		return kindUnsupported
	}
}

// Field is one stored data type of a table, with the parameters the
// definition message carries for it.
type Field struct {
	// Type is the data type.
	Type DataType
	// ArrayLen is the element count of an array type (DataGPT), 1 to
	// MaxArrayLen; zero for scalar types.
	ArrayLen uint32
	// Period is the frequency-counter period of DataHTTPReqRate, greater
	// than zero; zero for other types.
	Period Millis
}

// String formats f as HAProxy's configuration does.
func (f Field) String() string {
	switch f.Type.kind() {
	case kindFreq:
		return fmt.Sprintf("%v(%d)", f.Type, uint32(f.Period))
	case kindUintArray:
		return fmt.Sprintf("%v(%d)", f.Type, f.ArrayLen)
	case kindUint, kindUnsupported:
	}
	return f.Type.String()
}

// Definition is the schema a peer announces for one table. The table ID
// is not part of it: IDs belong to the announcing peer's namespace (see
// RemoteTableID and LocalTableID).
type Definition struct {
	// Name is the table name, which identifies the table across peers.
	Name string
	// KeyType is the key type; only KeyTypeIPv6 is supported.
	KeyType KeyType
	// Expiry is the table's configured entry lifetime. An ordinary update
	// restarts an entry's lifetime at the receiver's own table expiry. It
	// must be non-zero: HAProxy announces 0 for a table without "expire",
	// whose entries never age out, and a lifetime of 0 would read as
	// "already expired", so such tables are rejected with ErrExpiry.
	Expiry Millis
	// Fields are the stored data types in strictly ascending Type order,
	// which is also their order in every update.
	Fields []Field
}

// Validate checks that d is inside the supported subset: a valid name,
// the IPv6 key type, a non-zero expiry, and supported fields in ascending
// order with valid parameters. It returns an ErrSchema error naming the
// first problem.
func (d Definition) Validate() error {
	if !validName(d.Name) {
		return schemaErr(ErrTableName, "table name %q", d.Name)
	}
	if d.KeyType != KeyTypeIPv6 {
		return schemaErr(ErrKeyType, "table %s: key type %d", d.Name, d.KeyType)
	}
	if d.Expiry == 0 {
		return schemaErr(ErrExpiry, "table %s: no expiry", d.Name)
	}
	for i, f := range d.Fields {
		if i > 0 && f.Type <= d.Fields[i-1].Type {
			return schemaErr(ErrFieldOrder, "table %s: field %v after %v", d.Name, f.Type, d.Fields[i-1].Type)
		}
		if err := f.validate(); err != nil {
			return fmt.Errorf("table %s: %w", d.Name, err)
		}
	}
	return nil
}

func (f Field) validate() error {
	k := f.Type.kind()
	if k == kindUnsupported {
		return schemaErr(ErrDataType, "data type %d", uint8(f.Type))
	}
	if k == kindUintArray {
		if f.ArrayLen == 0 || f.ArrayLen > MaxArrayLen {
			return schemaErr(ErrArrayLength, "%v: %d elements", f.Type, f.ArrayLen)
		}
	} else if f.ArrayLen != 0 {
		return schemaErr(ErrArrayLength, "%v is not an array", f.Type)
	}
	if k == kindFreq {
		if f.Period == 0 {
			return schemaErr(ErrPeriod, "%v: zero period", f.Type)
		}
	} else if f.Period != 0 {
		return schemaErr(ErrPeriod, "%v has no period", f.Type)
	}
	return nil
}

// Field returns the field of type t, if the table stores it.
func (d Definition) Field(t DataType) (Field, bool) {
	for _, f := range d.Fields {
		if f.Type == t {
			return f, true
		}
	}
	return Field{}, false
}

// validName accepts a non-empty name of printable, non-space ASCII, the
// characters HAProxy permits in proxy and table identifiers and more.
func validName(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		if name[i] < 0x21 || name[i] > 0x7e {
			return false
		}
	}
	return true
}
