// Package peersession runs one HAProxy peers-protocol session over one
// connection: the hello/status handshake in either direction, then the
// binary message stream with heartbeats, an idle timeout, the
// resynchronization controls, update acknowledgements, and validated table
// events handed to the caller.
//
// It knows nothing about other sessions. Choosing which connection
// represents a source, reconnecting, and owning the application event queue
// belong to the caller (package sources).
//
// # States
//
// A Conn moves through explicit phases (see Phase): PhaseHello (acceptor
// reading a hello) or PhaseStatus (initiator awaiting a status line), then
// PhaseEstablished, then PhaseClosed. While established it tracks two
// resynchronization state machines independently:
//
//   - Learning (LearnState): with Options.RequestResync the session sends a
//     resync request as soon as it is established and expects exactly one
//     "finished" or "partial" control in reply, which it confirms. Either
//     reply without a request is a protocol error.
//   - Teaching (pending confirms): a resync request from the source is
//     answered with "finished" at once, because this phase announces no
//     tables of its own. Each "finished" sent must be confirmed once; a
//     confirm without one outstanding is a protocol error.
//
// # Failure handling
//
// The session fails closed. Unknown message classes, unknown control or
// stick-table types, malformed frames or bodies, and messages that do not
// fit the session's state (an update with no table selected, an
// acknowledgement for a table this side never announced, an unexpected
// resync control) end the session after a best-effort protocol-error
// message (01 00), or a size-limit error (01 01) for an oversized frame,
// as HAProxy itself does. An input table whose definition is outside the
// supported subset or differs from its configured schema ends the session
// without an error message. Tables that are not configured as inputs are
// ignored, whatever their schema: their updates are neither decoded nor
// acknowledged, as HAProxy ignores tables it does not share.
//
// # Acknowledgements
//
// An update of an input table is acknowledged only after the caller's
// event sink accepted it. A sink refusal ends the session immediately, so
// neither that update nor any later one is acknowledged. Acknowledgements
// are cumulative per table, as in HAProxy: after the frames buffered by one
// read are processed, each table with newly accepted updates gets one
// acknowledgement naming its last accepted update ID.
package peersession
