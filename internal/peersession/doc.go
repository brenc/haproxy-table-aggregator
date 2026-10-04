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
//     answered with a full teach of the output (if any; see below) and
//     then "finished". Each "finished" sent must be confirmed once; a
//     confirm without one outstanding is a protocol error.
//
// # Output
//
// With Options.Output, the session publishes the output store's tables to
// the source as an ordinary peer: it announces each output table under
// its own local ID (1, 2, ... in store order), and never announces an
// input table. It sends nothing for a table until the source has
// announced its own definition of it and that definition matches:
// stock HAProxy applies an update to any table of that name whose key
// type matches, whatever it stores, so writing first and checking later
// could corrupt another table. Once a table is announced, the session
// teaches it (its definition and every entry), teaches every announced
// table again for each resync request, and afterwards sends each entry
// that changes, re-sending a table's definition to switch to it as
// HAProxy does. Stock HAProxy announces a table that it never updates
// itself, as an output table, only in a teach, so a session with output
// must request a resync (Options.RequestResync). Every update carries an
// explicit update ID, counted per table from 1 in each session, and is
// ordinary (untimed), so the source's table expire applies from
// reception. The source acknowledges with this side's table IDs, which
// are a namespace separate from the source's own IDs.
//
// The source's own copy of an output table (which HAProxy replays when it
// teaches) must have exactly the definition this side announces; its
// updates are acknowledged and counted (Stats.EchoedUpdates), but never
// delivered to the sink, so output can never become an input.
//
// # Failure handling
//
// The session fails closed. Unknown message classes, unknown control or
// stick-table types, malformed frames or bodies, and messages that do not
// fit the session's state (an update with no table selected, an
// acknowledgement for a table this side never announced, an unexpected
// resync control) end the session after a best-effort protocol-error
// message (01 00), or a size-limit error (01 01) for an oversized frame,
// as HAProxy itself does. So do an acknowledgement for a table this side
// never announced or for an update it did not send in this session. An
// input or output table whose definition is outside the supported subset
// or differs from its configured schema ends the session without an error
// message. Tables that are neither inputs nor outputs are ignored,
// whatever their schema: their updates are neither decoded nor
// acknowledged, as HAProxy ignores tables it does not share.
//
// # Acknowledgements
//
// An update of an input table is acknowledged only after the caller's
// event sink accepted it; an update of the source's copy of an output
// table is acknowledged without reaching the sink. A sink refusal ends the session immediately, so
// neither that update nor any later one is acknowledged. Acknowledgements
// are cumulative per table, as in HAProxy: after the frames buffered by one
// read are processed, each table with newly accepted updates gets one
// acknowledgement naming its last accepted update ID.
package peersession
