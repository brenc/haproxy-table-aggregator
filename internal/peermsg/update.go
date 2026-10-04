package peermsg

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// UpdateID identifies an entry update within one table of one peer
// session. It is a 32-bit serial number that wraps from 2^32-1 to 0, so
// numeric magnitude says nothing about order: a resynchronization replays
// IDs from an arbitrary origin, wrapping round to the start. This package
// never compares IDs except for equality and Next.
type UpdateID uint32

// Next returns the ID an implicit update after u carries: u+1, wrapping.
func (u UpdateID) Next() UpdateID { return u + 1 }

// FreqCounter is one frequency counter (for example http_req_rate) as
// carried on the wire. It is HAProxy's estimator state, not a rate.
type FreqCounter struct {
	// Age is the time from the start of the counter's current period to
	// the moment the sender encoded it (the sender's now_ms minus the
	// counter's curr_tick, modulo 2^32). It is a duration relative to the
	// sender's clock at send time, not a timestamp; combine it with the
	// update's reception time. A counter whose period never started has
	// curr_tick 0, so its age is simply the sender's now_ms: HAProxy's
	// clock starts about 20 seconds before it wraps, so that value is
	// near 2^32 ms only early in the sender's life and arbitrary later.
	// Age magnitude therefore never identifies an unstarted counter; only
	// Curr and Prev both being zero says it holds no events.
	Age Millis
	// Curr is the event count of the current period.
	Curr uint32
	// Prev is the event count of the previous period.
	Prev uint32
}

// Value is the value of one table field in an update. Exactly the member
// matching the field's type is used; the others must be zero.
type Value struct {
	// Type is the field's data type.
	Type DataType
	// Uint holds DataHTTPReqCnt.
	Uint uint32
	// Freq holds DataHTTPReqRate.
	Freq FreqCounter
	// Array holds DataGPT; its length must equal the field's ArrayLen.
	Array []uint32
}

// Update is one entry update: a complete replacement of the entry's
// values for the fields the table definition declares.
type Update struct {
	// ID is the update ID. For a decoded update without an explicit ID
	// it is derived as the previous update's ID plus one.
	ID UpdateID
	// ExplicitID records whether the message carried ID (types 0x80 and
	// 0x85). When encoding, false sends no ID, and the receiver derives
	// it from its own last received ID; see AppendUpdate.
	ExplicitID bool
	// Timed records whether the message carried Remaining (types 0x85
	// and 0x86).
	Timed bool
	// Remaining is the entry's remaining lifetime at the sender when it
	// encoded a timed update, at most MaxRemaining. It is zero, and
	// meaningless, otherwise.
	Remaining Millis
	// Key is the entry key.
	Key Key
	// Values holds one value per definition field, in field order.
	Values []Value
	// Received is when the session read the message, as passed to the
	// decoder. Keep the monotonic reading: wire durations (Remaining,
	// FreqCounter.Age) are relative to it. It is ignored by AppendUpdate.
	Received time.Time
}

// MaxRemaining is the largest remaining lifetime a timed update may
// carry. HAProxy reads the 4-byte field into a signed int, so larger
// values are negative to it: its cap at the table expiry no longer
// applies, and the pinned releases disagree on the outcome (lab probe:
// 0x80000000 gave exp=2147483647 on 3.4.6 and exp=0 on 3.2.25, and
// 0xffffffff left no entry on either). Stock senders never produce such
// a value, because table expiry is itself a signed int, so this package
// rejects it in both directions with ErrRemaining.
const MaxRemaining Millis = 0x7fffffff

// Lifetime returns the lifetime the update grants the entry from its
// reception, given the receiving table's configured expiry, which a valid
// Definition guarantees is non-zero (see Definition.Expiry and
// ErrExpiry). A result of 0 therefore always means "already expired",
// never "no expiry". An ordinary
// update restarts the full table expiry. A timed update carries the
// sender's remaining lifetime, which HAProxy caps at the receiving
// table's expiry for values up to MaxRemaining; Lifetime applies the same
// cap, so replay can never extend a lifetime past the table's own expiry.
// A timed update whose Remaining exceeds MaxRemaining (never produced by
// the decoder) grants no lifetime: it returns 0.
func (u Update) Lifetime(tableExpiry Millis) Millis {
	if !u.Timed {
		return tableExpiry
	}
	if u.Remaining > MaxRemaining {
		return 0
	}
	return min(u.Remaining, tableExpiry)
}

// Value returns the value of type t, if the update carries one.
func (u Update) Value(t DataType) (Value, bool) {
	for _, v := range u.Values {
		if v.Type == t {
			return v, true
		}
	}
	return Value{}, false
}

// IsUpdateType reports whether typ is one of the four entry update types.
func IsUpdateType(typ peerwire.MessageType) bool {
	switch typ {
	case peerwire.StickTableUpdate, peerwire.StickTableIncrementalUpdate,
		peerwire.StickTableTimedUpdate, peerwire.StickTableIncrementalTimedUpdate:
		return true
	default:
		return false
	}
}

func updateType(explicit, timed bool) peerwire.MessageType {
	switch {
	case explicit && timed:
		return peerwire.StickTableTimedUpdate
	case timed:
		return peerwire.StickTableIncrementalTimedUpdate
	case explicit:
		return peerwire.StickTableUpdate
	default:
		return peerwire.StickTableIncrementalUpdate
	}
}

// DecodeUpdate decodes the body of an entry update message of type typ
// for a table with definition def, recording received as its reception
// time.
//
// Wire layout: a 4-byte big-endian update ID (types 0x80 and 0x85 only),
// a 4-byte big-endian remaining lifetime in milliseconds (0x85 and 0x86
// only), the 16-byte key, then each field's value in definition order:
// an unsigned integer, a frequency counter as age, current count, and
// previous count, or an array as that many integers.
//
// A message without an explicit ID gets prev.Next(); havePrev must then be
// true, or the update fails with ErrImplicitID, because HAProxy would
// derive the ID from session state this decoder was not given. Every
// value must fit its 32-bit field; HAProxy would truncate a larger one,
// but this decoder rejects it (ErrMalformed wrapping peerwire.ErrIntRange)
// so that a value is never silently wrapped. Bytes after the last field
// fail with ErrTrailingData, as they mean the peer's schema disagrees
// with def. A remaining lifetime above MaxRemaining fails with
// ErrRemaining. A def outside the supported subset fails with its
// ErrSchema error.
func DecodeUpdate(typ peerwire.MessageType, body []byte, def Definition, prev UpdateID, havePrev bool,
	received time.Time,
) (Update, error) {
	if !IsUpdateType(typ) {
		return Update{}, newErr(ErrUnknownMessage, nil, nil, "type %#02x is not an entry update", uint8(typ))
	}
	if err := def.Validate(); err != nil {
		return Update{}, err
	}
	u := Update{
		ExplicitID: typ == peerwire.StickTableUpdate || typ == peerwire.StickTableTimedUpdate,
		Timed:      typ == peerwire.StickTableTimedUpdate || typ == peerwire.StickTableIncrementalTimedUpdate,
		Received:   received,
	}
	c := peerwire.NewCursor(body)
	if u.ExplicitID {
		id, err := c.Fixed32()
		if err != nil {
			return Update{}, malformed(err, "table %s: update ID", def.Name)
		}
		u.ID = UpdateID(id)
	} else {
		if !havePrev {
			return Update{}, stateErr(ErrImplicitID, "table %s", def.Name)
		}
		u.ID = prev.Next()
	}
	if u.Timed {
		r, err := c.Fixed32()
		if err != nil {
			return Update{}, malformed(err, "table %s: remaining lifetime", def.Name)
		}
		u.Remaining = Millis(r)
		if u.Remaining > MaxRemaining {
			return Update{}, newErr(ErrMalformed, ErrRemaining, nil,
				"table %s: remaining lifetime %#x exceeds %#x", def.Name, r, uint32(MaxRemaining))
		}
	}
	key, err := c.Bytes(IPv6KeyLen)
	if err != nil {
		return Update{}, malformed(err, "table %s: key", def.Name)
	}
	u.Key = Key(key)
	u.Values = make([]Value, 0, len(def.Fields))
	for _, f := range def.Fields {
		v, err := decodeValue(c, f)
		if err != nil {
			return Update{}, malformed(err, "table %s key %v: %v", def.Name, u.Key, f)
		}
		u.Values = append(u.Values, v)
	}
	if c.Remaining() != 0 {
		return Update{}, newErr(ErrMalformed, ErrTrailingData, nil,
			"table %s key %v: %d bytes after the last field", def.Name, u.Key, c.Remaining())
	}
	return u, nil
}

func decodeValue(c *peerwire.Cursor, f Field) (Value, error) {
	v := Value{Type: f.Type}
	var err error
	switch f.Type.kind() {
	case kindUint:
		v.Uint, err = c.Uint32()
	case kindFreq:
		var age uint32
		if age, err = c.Uint32(); err == nil {
			v.Freq.Age = Millis(age)
			if v.Freq.Curr, err = c.Uint32(); err == nil {
				v.Freq.Prev, err = c.Uint32()
			}
		}
	case kindUintArray:
		v.Array = make([]uint32, f.ArrayLen)
		for i := range v.Array {
			if v.Array[i], err = c.Uint32(); err != nil {
				return Value{}, fmt.Errorf("element %d: %w", i, err)
			}
		}
	case kindUnsupported:
		// Unreachable: def was validated.
		return Value{}, schemaErr(ErrDataType, "data type %d", uint8(f.Type))
	}
	return v, err
}

// AppendUpdate appends a complete entry update message for a table with
// definition def and returns the extended slice. The message type follows
// u.ExplicitID and u.Timed. u.Values must match def.Fields exactly (same
// count, types, and array lengths, unused members zero), or it fails with
// ErrValues: a partial entry is never encoded or completed. A timed
// update with Remaining above MaxRemaining fails with ErrRemaining. On
// error dst is returned unchanged.
//
// An update without an explicit ID tells the receiver to use its last
// received ID for this table plus one; the caller must know that equals
// u.ID. HAProxy 3.4.6 records every such ID byte-swapped on
// little-endian hosts, acknowledges the swapped value, and derives the
// next implicit ID from it (see the phase 03 record), so senders should
// set ExplicitID on every update they send to it.
func AppendUpdate(dst []byte, def Definition, u Update) ([]byte, error) {
	if err := def.Validate(); err != nil {
		return dst, err
	}
	if len(u.Values) != len(def.Fields) {
		return dst, schemaErr(ErrValues, "table %s: %d values for %d fields", def.Name, len(u.Values), len(def.Fields))
	}
	for i, f := range def.Fields {
		if err := checkValue(f, u.Values[i]); err != nil {
			return dst, fmt.Errorf("table %s key %v: %w", def.Name, u.Key, err)
		}
	}
	if u.Timed && u.Remaining > MaxRemaining {
		return dst, newErr(ErrMalformed, ErrRemaining, nil,
			"table %s key %v: remaining lifetime %#x exceeds %#x", def.Name, u.Key, uint32(u.Remaining), uint32(MaxRemaining))
	}
	var body []byte
	if u.ExplicitID {
		body = appendFixed32(body, uint32(u.ID))
	}
	if u.Timed {
		body = appendFixed32(body, uint32(u.Remaining))
	}
	body = append(body, u.Key[:]...)
	for _, v := range u.Values {
		switch v.Type.kind() {
		case kindUint:
			body = peerwire.AppendUint(body, uint64(v.Uint))
		case kindFreq:
			body = peerwire.AppendUint(body, uint64(v.Freq.Age))
			body = peerwire.AppendUint(body, uint64(v.Freq.Curr))
			body = peerwire.AppendUint(body, uint64(v.Freq.Prev))
		case kindUintArray:
			for _, e := range v.Array {
				body = peerwire.AppendUint(body, uint64(e))
			}
		case kindUnsupported:
		}
	}
	return appendTableFrame(dst, updateType(u.ExplicitID, u.Timed), body)
}

func checkValue(f Field, v Value) error {
	if v.Type != f.Type {
		return schemaErr(ErrValues, "value of type %v where %v is due", v.Type, f.Type)
	}
	k := f.Type.kind()
	if k != kindUint && v.Uint != 0 {
		return schemaErr(ErrValues, "%v: Uint set", f.Type)
	}
	if k != kindFreq && v.Freq != (FreqCounter{}) {
		return schemaErr(ErrValues, "%v: Freq set", f.Type)
	}
	if k == kindUintArray {
		if uint64(len(v.Array)) != uint64(f.ArrayLen) {
			return schemaErr(ErrValues, "%v: %d elements, want %d", f.Type, len(v.Array), f.ArrayLen)
		}
	} else if v.Array != nil {
		return schemaErr(ErrValues, "%v: Array set", f.Type)
	}
	return nil
}

func appendFixed32(b []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(b, v) }
