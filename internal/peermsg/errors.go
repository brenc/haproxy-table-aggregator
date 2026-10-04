package peermsg

import (
	"errors"
	"fmt"
)

// Error classes. Every error returned by this package wraps exactly one
// class sentinel and, where one applies, one specific sentinel below, so
// callers can branch with errors.Is on either. One exception carries two
// specific sentinels under its single class: a definition both over the
// table limit and rejected wraps ErrState with ErrTooManyTables and the
// rejection's specific sentinel (see ErrTooManyTables). Cursor failures from
// package peerwire (such as peerwire.ErrShortBody or peerwire.ErrIntRange)
// stay in the chain as well.
var (
	// ErrMalformed reports a message that cannot be parsed at all. Stock
	// HAProxy answers the same input with a protocol error, or (for the
	// stricter checks documented on each decoder) would have accepted it
	// only by truncating or ignoring bytes.
	ErrMalformed = errors.New("peermsg: malformed message")

	// ErrSchema reports a well-formed table definition, or values for one,
	// outside the supported subset. Stock HAProxy ignores definitions it
	// cannot use and keeps the session; whether this service does is the
	// session layer's decision.
	ErrSchema = errors.New("peermsg: unsupported table schema")

	// ErrState reports a message that is well-formed but cannot be
	// applied to the session's table namespace, such as an update with no
	// table selected.
	ErrState = errors.New("peermsg: message does not fit session table state")

	// ErrUnknownMessage reports a message class or type this package does
	// not interpret. Stock HAProxy skips such messages.
	ErrUnknownMessage = errors.New("peermsg: unknown message")
)

// Specific failures, each wrapped together with its class.
var (
	// ErrTrailingData (ErrMalformed) reports bytes after the last field a
	// message's schema defines.
	ErrTrailingData = errors.New("peermsg: trailing data after last field")

	// ErrTableID (ErrMalformed) reports table ID 0, which HAProxy uses to
	// mean "no table" and never assigns.
	ErrTableID = errors.New("peermsg: invalid table ID")

	// ErrTableName (ErrMalformed for decoding, ErrSchema for encoding)
	// reports an empty table name or one with bytes outside printable
	// ASCII.
	ErrTableName = errors.New("peermsg: invalid table name")

	// ErrKeyType (ErrSchema) reports an unsupported key type.
	ErrKeyType = errors.New("peermsg: unsupported key type")

	// ErrKeyLength (ErrSchema) reports a key length that does not match
	// the key type.
	ErrKeyLength = errors.New("peermsg: key length does not match key type")

	// ErrExpiry (ErrSchema) reports a table expiry of 0, which HAProxy
	// announces for a table whose entries never age out. Lifetimes and
	// rate decay need a finite expiry, and 0 would read as "expired".
	ErrExpiry = errors.New("peermsg: table has no expiry")

	// ErrDataType (ErrSchema) reports a stored data type outside the
	// supported subset.
	ErrDataType = errors.New("peermsg: unsupported data type")

	// ErrFieldOrder (ErrSchema) reports fields, or definition parameters,
	// that are not in strictly ascending data-type order or do not belong
	// to the declared data types.
	ErrFieldOrder = errors.New("peermsg: fields out of order")

	// ErrArrayLength (ErrSchema) reports an array element count outside
	// 1..MaxArrayLen, or one given for a scalar type.
	ErrArrayLength = errors.New("peermsg: invalid array length")

	// ErrPeriod (ErrSchema) reports a frequency-counter period of zero, or
	// a period given for a type that has none.
	ErrPeriod = errors.New("peermsg: invalid counter period")

	// ErrValues (ErrSchema) reports update values that do not cover the
	// schema exactly: a missing or extra field, a field of the wrong type
	// or shape, or an array of the wrong length. Partial entries are never
	// encoded or completed.
	ErrValues = errors.New("peermsg: values do not match schema")

	// ErrRemaining (ErrMalformed) reports a timed update whose remaining
	// lifetime exceeds MaxRemaining, which HAProxy would read as negative.
	ErrRemaining = errors.New("peermsg: remaining lifetime out of range")

	// ErrTooLarge (ErrMalformed) reports a message to encode whose wire
	// size exceeds MaxEncodedMessage.
	ErrTooLarge = errors.New("peermsg: encoded message too large")

	// ErrNoTable (ErrState) reports an update received while no table is
	// selected.
	ErrNoTable = errors.New("peermsg: no table selected")

	// ErrRejectedTable (ErrState) reports an update for a table whose
	// definition was rejected with ErrSchema. The update is not decoded.
	ErrRejectedTable = errors.New("peermsg: table definition was rejected")

	// ErrUnknownTable (ErrState) reports a table ID that is not bound in
	// the namespace it refers to.
	ErrUnknownTable = errors.New("peermsg: unknown table ID")

	// ErrImplicitID (ErrState) reports an update without an explicit
	// update ID when the table has no previous update in this session to
	// derive one from.
	ErrImplicitID = errors.New("peermsg: implicit update ID without a previous update")

	// ErrTooManyTables (ErrState) reports a namespace that would exceed
	// its configured table limit. If the definition that hit the limit
	// was also outside the supported subset, the error carries that
	// rejection's specific sentinel (such as ErrDataType) but not the
	// ErrSchema class, since nothing was bound.
	ErrTooManyTables = errors.New("peermsg: too many tables")

	// ErrDuplicateTable (ErrState) reports a second local registration of
	// one table name.
	ErrDuplicateTable = errors.New("peermsg: table already registered")
)

// maxDetail bounds error details so that peer-controlled content cannot
// produce an unbounded log line.
const maxDetail = 160

// newErr wraps class and, when non-nil, specific and cause with a bounded
// detail. The cause is typically a *peerwire.Error from a Cursor read.
func newErr(class, specific, cause error, format string, args ...any) error {
	detail := fmt.Sprintf(format, args...)
	if len(detail) > maxDetail {
		detail = detail[:maxDetail] + "..."
	}
	switch {
	case specific != nil && cause != nil:
		return fmt.Errorf("%w: %w: %s: %w", class, specific, detail, cause)
	case specific != nil:
		return fmt.Errorf("%w: %w: %s", class, specific, detail)
	case cause != nil:
		return fmt.Errorf("%w: %s: %w", class, detail, cause)
	default:
		return fmt.Errorf("%w: %s", class, detail)
	}
}

// schemaReasons are the specific sentinels an ErrSchema error can carry.
var schemaReasons = []error{
	ErrTableName, ErrKeyType, ErrKeyLength, ErrExpiry, ErrDataType,
	ErrFieldOrder, ErrArrayLength, ErrPeriod, ErrValues,
}

// schemaReason returns the specific sentinel of an ErrSchema error, or
// nil, so that another class can report it without the ErrSchema class.
func schemaReason(err error) error {
	for _, r := range schemaReasons {
		if errors.Is(err, r) {
			return r
		}
	}
	return nil
}

func malformed(cause error, format string, args ...any) error {
	return newErr(ErrMalformed, nil, cause, format, args...)
}

func schemaErr(specific error, format string, args ...any) error {
	return newErr(ErrSchema, specific, nil, format, args...)
}

func stateErr(specific error, format string, args ...any) error {
	return newErr(ErrState, specific, nil, format, args...)
}
