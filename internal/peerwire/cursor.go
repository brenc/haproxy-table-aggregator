package peerwire

import (
	"encoding/binary"
	"errors"
)

// Cursor reads fields from a message body with bounds checks. Every read
// either succeeds and advances, or fails and leaves the cursor where it
// was. Errors are *Error values whose Offset is the position within the
// body where the failed field starts.
//
// Slices returned by Bytes and Rest alias the body, so they follow the
// body's validity (see Decoder). A Cursor is not safe for concurrent use.
type Cursor struct {
	b   []byte
	pos int
}

// NewCursor returns a cursor at the start of body.
func NewCursor(body []byte) *Cursor { return &Cursor{b: body} }

// Offset returns the number of body bytes consumed.
func (c *Cursor) Offset() int { return c.pos }

// Remaining returns the number of unread body bytes.
func (c *Cursor) Remaining() int { return len(c.b) - c.pos }

// Rest returns the unread bytes without consuming them.
func (c *Cursor) Rest() []byte { return c.b[c.pos:len(c.b):len(c.b)] }

// Uint reads one encoded integer. It fails with ErrShortBody if the body
// ends inside the encoding and ErrIntOverflow if it exceeds 64 bits.
func (c *Cursor) Uint() (uint64, error) {
	v, n, err := DecodeUint(c.b[c.pos:])
	if err != nil {
		return 0, c.fieldError(err, "encoded integer")
	}
	c.pos += n
	return v, nil
}

// Uint32 reads one encoded integer that must fit in 32 bits; a larger
// value fails with ErrIntRange.
func (c *Cursor) Uint32() (uint32, error) {
	v, n, err := DecodeUint32(c.b[c.pos:])
	if err != nil {
		return 0, c.fieldError(err, "encoded 32-bit integer")
	}
	c.pos += n
	return v, nil
}

// Fixed32 reads a 4-byte big-endian (network order) unsigned integer, the
// form of update identifiers.
func (c *Cursor) Fixed32() (uint32, error) {
	if c.Remaining() < 4 {
		return 0, c.short(4)
	}
	v := binary.BigEndian.Uint32(c.b[c.pos:])
	c.pos += 4
	return v, nil
}

// Bytes reads the next n bytes, as for a length-prefixed or fixed-size
// field. n is unsigned so that a decoded length can be passed without a
// narrowing conversion; it fails with ErrShortBody unless n bytes remain.
func (c *Cursor) Bytes(n uint64) ([]byte, error) {
	if n > uint64(c.Remaining()) { //nolint:gosec // G115: Remaining is never negative.
		return nil, c.short(n)
	}
	end := c.pos + int(n) //nolint:gosec // G115: n <= Remaining, an int.
	b := c.b[c.pos:end:end]
	c.pos = end
	return b, nil
}

func (c *Cursor) short(want uint64) error {
	return newError(ErrShortBody, uint64(c.pos), //nolint:gosec // G115: pos is never negative.
		"field needs %d bytes, %d remain", want, c.Remaining())
}

func (c *Cursor) fieldError(err error, what string) error {
	if errors.Is(err, ErrIntTruncated) {
		return newError(ErrShortBody, uint64(c.pos), //nolint:gosec // G115: pos is never negative.
			"%s truncated at end of body", what)
	}
	return newError(err, uint64(c.pos), "%s", what) //nolint:gosec // G115: pos is never negative.
}
