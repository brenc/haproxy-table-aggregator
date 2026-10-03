package peerwire

import (
	"errors"
	"fmt"
)

// Sentinel errors. Their identities are stable; test for them with
// errors.Is. Decoder methods wrap them in *Error to add the stream offset.
var (
	// ErrNeedMore reports that the buffered input does not yet hold a
	// complete line or frame. It is not a failure: feed more input and
	// call again.
	ErrNeedMore = errors.New("peerwire: need more input")

	// ErrTruncated reports that input ended (CloseInput) inside a line or
	// frame.
	ErrTruncated = errors.New("peerwire: input ended inside a line or frame")

	// ErrIntTruncated reports that a byte slice ended inside an encoded
	// integer.
	ErrIntTruncated = errors.New("peerwire: encoded integer is truncated")

	// ErrIntOverflow reports an encoded integer whose value does not fit
	// in 64 bits. HAProxy never produces one.
	ErrIntOverflow = errors.New("peerwire: encoded integer exceeds 64 bits")

	// ErrIntRange reports a well-formed encoded integer that is too large
	// for the field being read, such as a 32-bit field.
	ErrIntRange = errors.New("peerwire: integer out of range for field")

	// ErrLengthEncoding reports a message length whose encoding does not
	// end within MaxLengthLen bytes. HAProxy answers this with a protocol
	// error.
	ErrLengthEncoding = errors.New("peerwire: message length encoding too long")

	// ErrMessageTooLarge reports a message body longer than
	// Limits.MaxMessage. HAProxy answers this with a size-limit error.
	ErrMessageTooLarge = errors.New("peerwire: message too large")

	// ErrHandshakeTooLarge reports handshake lines that exceed
	// Limits.MaxHandshake bytes in total.
	ErrHandshakeTooLarge = errors.New("peerwire: handshake too large")

	// ErrInputLimit reports that Feed was offered more bytes than the
	// decoder may buffer (see Decoder.Free).
	ErrInputLimit = errors.New("peerwire: buffered input limit exceeded")

	// ErrInputClosed reports Feed after CloseInput.
	ErrInputClosed = errors.New("peerwire: input already closed")

	// ErrReservedClass reports a message of the reserved class 255.
	ErrReservedClass = errors.New("peerwire: reserved message class")

	// ErrHandshakeDone reports NextLine after the decoder switched to
	// binary framing.
	ErrHandshakeDone = errors.New("peerwire: handshake framing already finished")

	// ErrBadHandshake reports a handshake line that does not have the
	// required form.
	ErrBadHandshake = errors.New("peerwire: malformed handshake line")

	// ErrShortBody reports a read past the end of a message body.
	ErrShortBody = errors.New("peerwire: read past end of message body")

	// ErrFixedBody reports a body supplied for a fixed-size message type.
	ErrFixedBody = errors.New("peerwire: fixed-size message cannot carry a body")

	// ErrInvalidLimits reports a Limits value that cannot frame every
	// message it admits.
	ErrInvalidLimits = errors.New("peerwire: invalid limits")
)

// maxDetail bounds Error.Detail so that peer-controlled content can never
// produce an unbounded log line.
const maxDetail = 160

// Error is a failure at a known position in a stream or message body,
// returned for Decoder framing failures, ErrInputLimit, and every Cursor
// failure. Err is one of the package's sentinel errors and is returned by
// Unwrap. Decoder misuse (ErrHandshakeDone, ErrInputClosed) and the
// non-failures ErrNeedMore and io.EOF are returned as bare sentinels.
// Failures without a position (handshake line parsing and encoding, Limits
// validation) are plain errors wrapping a sentinel.
type Error struct {
	// Err is the sentinel describing the failure class.
	Err error
	// Offset is the stream offset, in bytes from the first byte fed to the
	// decoder (or from the start of a message body for Cursor), of the
	// line, frame, or field that failed.
	Offset uint64
	// Detail is a bounded human-readable description. It never contains
	// more than a few bytes of peer-supplied data.
	Detail string
}

func newError(sentinel error, offset uint64, format string, args ...any) *Error {
	return &Error{Err: sentinel, Offset: offset, Detail: boundedDetail(format, args...)}
}

func boundedDetail(format string, args ...any) string {
	detail := fmt.Sprintf(format, args...)
	if len(detail) > maxDetail {
		detail = detail[:maxDetail] + "..."
	}
	return detail
}

// detailError wraps sentinel with a bounded detail for failures that have
// no stream or body position.
func detailError(sentinel error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", sentinel, boundedDetail(format, args...))
}

// Error formats the sentinel, offset, and detail.
func (e *Error) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("%v at offset %d", e.Err, e.Offset)
	}
	return fmt.Sprintf("%v at offset %d: %s", e.Err, e.Offset, e.Detail)
}

// Unwrap returns the sentinel error.
func (e *Error) Unwrap() error { return e.Err }
