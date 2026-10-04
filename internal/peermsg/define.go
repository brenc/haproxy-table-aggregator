package peermsg

import (
	"fmt"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// RemoteTableID is a table ID announced by the remote peer of one session.
// It means nothing outside that session: two sources may announce the same
// number for different tables. Update and switch messages received from
// the peer refer to it, and acknowledgements sent to the peer carry it.
type RemoteTableID uint32

// LocalTableID is a table ID this side announces in its own definitions.
// Acknowledgements received from the peer carry it.
type LocalTableID uint32

// MaxEncodedMessage bounds every message this package encodes, header and
// length included: stock HAProxy processes a message only if all of it
// fits in its tune.bufsize, 16384 bytes by default.
const MaxEncodedMessage = 16384

// DecodeDefinition decodes the body of a table definition message
// (peerwire.StickTableDefine).
//
// Wire layout, every integer in HAProxy's encoding: table ID, name length,
// name, key type, key length, data types bitfield, expiry, then for each
// frequency-counter or array type in ascending type order its type number
// followed by its parameters (array: element count, plus a period for an
// array of counters; counter: period).
//
// It is stricter than HAProxy in three documented ways. The expiry and
// parameters are mandatory (doc/peers.txt calls them mandatory; HAProxy
// tolerates their absence from older peers). 32-bit fields must fit in 32
// bits rather than being truncated. Bytes after the last parameter are
// rejected with ErrTrailingData instead of skipped: doc/peers.txt lets
// later versions append fields, and a release that does fails closed here
// until compatibility testing admits it.
//
// Errors wrap ErrMalformed for an unparseable body, or ErrSchema for a
// well-formed definition outside the supported subset. With an ErrSchema
// error the returned ID and Definition.Name are valid, so the caller can
// record which table was rejected; the other fields are not.
func DecodeDefinition(body []byte) (RemoteTableID, Definition, error) {
	c := peerwire.NewCursor(body)
	id, err := c.Uint32()
	if err != nil {
		return 0, Definition{}, malformed(err, "definition table ID")
	}
	if id == 0 {
		return 0, Definition{}, newErr(ErrMalformed, ErrTableID, nil, "definition with table ID 0")
	}
	nameLen, err := c.Uint()
	if err != nil {
		return 0, Definition{}, malformed(err, "definition name length")
	}
	name, err := c.Bytes(nameLen)
	if err != nil {
		return 0, Definition{}, malformed(err, "definition name")
	}
	if !validName(string(name)) {
		return 0, Definition{}, newErr(ErrMalformed, ErrTableName, nil, "definition name %q", name)
	}
	rid := RemoteTableID(id)
	d := Definition{Name: string(name)}
	keyType, err := c.Uint()
	if err != nil {
		return 0, Definition{}, malformed(err, "table %s key type", d.Name)
	}
	keyLen, err := c.Uint()
	if err != nil {
		return 0, Definition{}, malformed(err, "table %s key length", d.Name)
	}
	bits, err := c.Uint()
	if err != nil {
		return 0, Definition{}, malformed(err, "table %s data types", d.Name)
	}
	expiry, err := c.Uint32()
	if err != nil {
		return 0, Definition{}, malformed(err, "table %s expiry", d.Name)
	}
	d.Expiry = Millis(expiry)
	if keyType != uint64(KeyTypeIPv6) {
		return rid, d, schemaErr(ErrKeyType, "table %s: key type %d", d.Name, keyType)
	}
	d.KeyType = KeyTypeIPv6
	if keyLen != IPv6KeyLen {
		return rid, d, schemaErr(ErrKeyLength, "table %s: key length %d for an IPv6 key", d.Name, keyLen)
	}
	if d.Expiry == 0 {
		return rid, d, schemaErr(ErrExpiry, "table %s: no expiry", d.Name)
	}
	for t := range maxDataTypes {
		if bits&(1<<t) == 0 {
			continue
		}
		f, err := decodeFieldParams(c, DataType(t), d.Name)
		if err != nil {
			return rid, d, err
		}
		d.Fields = append(d.Fields, f)
	}
	if c.Remaining() != 0 {
		return 0, Definition{}, newErr(ErrMalformed, ErrTrailingData, nil,
			"table %s: %d bytes after definition", d.Name, c.Remaining())
	}
	return rid, d, nil
}

// decodeFieldParams reads the parameters of type t, if it has any.
func decodeFieldParams(c *peerwire.Cursor, t DataType, table string) (Field, error) {
	f := Field{Type: t}
	k := t.kind()
	if k == kindUnsupported {
		return f, schemaErr(ErrDataType, "table %s: data type %d", table, uint8(t))
	}
	if k == kindUint {
		return f, nil
	}
	prefix, err := c.Uint()
	if err != nil {
		return f, malformed(err, "table %s: %v parameters", table, t)
	}
	if prefix != uint64(t) {
		return f, schemaErr(ErrFieldOrder, "table %s: parameters for type %d where %v is due", table, prefix, t)
	}
	if k == kindUintArray {
		n, err := c.Uint32()
		if err != nil {
			return f, malformed(err, "table %s: %v element count", table, t)
		}
		f.ArrayLen = n
	} else {
		p, err := c.Uint32()
		if err != nil {
			return f, malformed(err, "table %s: %v period", table, t)
		}
		f.Period = Millis(p)
	}
	if err := f.validate(); err != nil {
		return f, fmt.Errorf("table %s: %w", table, err)
	}
	return f, nil
}

// AppendDefinition appends a complete definition message announcing def
// under the local table ID id, and returns the extended slice. It fails
// with ErrTableID (ErrMalformed) for ID 0, an ErrSchema error if def is
// outside the supported subset, or ErrTooLarge if the message would
// exceed MaxEncodedMessage. On error dst is returned unchanged.
func AppendDefinition(dst []byte, id LocalTableID, def Definition) ([]byte, error) {
	if id == 0 {
		return dst, newErr(ErrMalformed, ErrTableID, nil, "local table ID 0")
	}
	if err := def.Validate(); err != nil {
		return dst, err
	}
	body := peerwire.AppendUint(nil, uint64(id))
	body = peerwire.AppendUint(body, uint64(len(def.Name)))
	body = append(body, def.Name...)
	body = peerwire.AppendUint(body, uint64(def.KeyType))
	body = peerwire.AppendUint(body, uint64(def.KeyType.KeyLen()))
	var bits uint64
	var params []byte
	for _, f := range def.Fields {
		bits |= 1 << f.Type
		switch f.Type.kind() {
		case kindUintArray:
			params = peerwire.AppendUint(params, uint64(f.Type))
			params = peerwire.AppendUint(params, uint64(f.ArrayLen))
		case kindFreq:
			params = peerwire.AppendUint(params, uint64(f.Type))
			params = peerwire.AppendUint(params, uint64(f.Period))
		case kindUint, kindUnsupported:
		}
	}
	body = peerwire.AppendUint(body, bits)
	body = peerwire.AppendUint(body, uint64(def.Expiry))
	body = append(body, params...)
	return appendTableFrame(dst, peerwire.StickTableDefine, body)
}

// appendTableFrame frames body as a stick-table message within
// MaxEncodedMessage.
func appendTableFrame(dst []byte, typ peerwire.MessageType, body []byte) ([]byte, error) {
	size := peerwire.HeaderLen + peerwire.EncodedLen(uint64(len(body))) + len(body)
	if size > MaxEncodedMessage {
		return dst, newErr(ErrMalformed, ErrTooLarge, nil, "%d-byte message exceeds %d", size, MaxEncodedMessage)
	}
	return peerwire.AppendFrame(dst, peerwire.ClassStickTable, typ, body)
}
