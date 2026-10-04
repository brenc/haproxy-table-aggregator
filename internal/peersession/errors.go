package peersession

import (
	"errors"
	"fmt"

	"github.com/brenc/haproxy-table-aggregator/internal/peerwire"
)

// Session failure classes. Every error Run or a handshake method returns
// wraps exactly one of them (or the context's error on cancellation), so
// callers can branch with errors.Is.
var (
	// ErrHandshake: the hello exchange failed. A *HandshakeError carries
	// the status code sent or received, when there was one.
	ErrHandshake = errors.New("peersession: handshake failed")

	// ErrProtocol: the source sent something the protocol or the
	// session's state does not allow. The session sent a protocol or
	// size-limit error message before ending.
	ErrProtocol = errors.New("peersession: protocol violation")

	// ErrSchema: an input or output table's definition is outside the
	// supported subset or differs from its configured schema.
	ErrSchema = errors.New("peersession: table schema mismatch")

	// ErrLimit: the source exceeded a configured resource bound, such as
	// the number of tables one session may bind.
	ErrLimit = errors.New("peersession: session limit exceeded")

	// ErrIdle: the session waited the idle timeout without receiving a
	// complete message; time spent processing received messages does not
	// count.
	ErrIdle = errors.New("peersession: source idle too long")

	// ErrClosed: the source closed the connection.
	ErrClosed = errors.New("peersession: source closed the connection")

	// ErrPeerError: the source sent an error message (class 1), which
	// HAProxy sends just before closing.
	ErrPeerError = errors.New("peersession: source reported an error")

	// ErrEventRejected: the event sink refused an event; the update it
	// carried was not acknowledged.
	ErrEventRejected = errors.New("peersession: event sink rejected an event")

	// ErrIO: reading or writing the connection failed.
	ErrIO = errors.New("peersession: connection I/O failed")
)

// HandshakeError describes a failed hello exchange. It wraps ErrHandshake.
type HandshakeError struct {
	// Code is the status code this side sent (as acceptor) or received
	// (as initiator), or 0 if the exchange failed before one.
	Code peerwire.StatusCode
	// Sent is true when this side sent Code to reject the peer.
	Sent bool
	// Reason describes the failure.
	Reason string
	// Err is an underlying cause, if any.
	Err error
}

func (e *HandshakeError) Error() string {
	msg := ErrHandshake.Error() + ": " + e.Reason
	switch {
	case e.Code != 0 && e.Sent:
		msg += fmt.Sprintf(" (sent status %d)", e.Code)
	case e.Code != 0:
		msg += fmt.Sprintf(" (received status %d)", e.Code)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

// Unwrap returns ErrHandshake and the cause.
func (e *HandshakeError) Unwrap() []error {
	if e.Err != nil {
		return []error{ErrHandshake, e.Err}
	}
	return []error{ErrHandshake}
}

func wrap(class error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", class, fmt.Sprintf(format, args...))
}

func wrapCause(class, cause error, format string, args ...any) error {
	return fmt.Errorf("%w: %s: %w", class, fmt.Sprintf(format, args...), cause)
}
