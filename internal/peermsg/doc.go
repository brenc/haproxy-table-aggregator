// Package peermsg encodes and decodes the stick-table messages of the
// HAProxy peers protocol for the narrow schema this service uses, on top
// of the framing in package peerwire. It holds no connection or session
// lifecycle state; Inbound and LocalTables are the per-session table-ID
// namespaces that decoding and acknowledging need.
//
// # Supported subset
//
//   - Table definitions (0x82) with the IPv6 key type (wire key type 5,
//     16-byte keys) and any strictly ascending combination of
//     http_req_cnt (data type 9, unsigned 32-bit), http_req_rate(period)
//     (type 10, frequency counter), and gpt(n) (type 22, array of n
//     unsigned 32-bit values, the selected output form). Every other key
//     type, key length, or data type is rejected with an ErrSchema error.
//   - Table switch (0x83), entry updates with and without an explicit
//     update ID (0x80, 0x81), timed updates carrying a remaining lifetime
//     (0x85, 0x86), and update acknowledgements (0x84).
//   - Control (class 0) and error (class 1) messages, which have no body.
//
// The pinned releases' doc/peers.txt (see package peerwire) describes the
// definition and update layouts but numbers the acknowledgement 133 and
// omits timed updates and definition parameters. The layouts here were
// confirmed against src/peers.c in the 3.4.6 and 3.2.25 release tarballs
// (read, not copied) and against fresh lab captures.
//
// # Namespaces
//
// Table IDs are local to the peer that announces them and to the session.
// RemoteTableID and LocalTableID are distinct types: updates and switches
// received refer to remote IDs, acknowledgements received refer to local
// IDs, and acknowledgements sent carry remote IDs. Keep one Inbound and
// one LocalTables per session.
//
// # Values and time
//
// Values decode to fixed-width unsigned Go integers. A wire value too
// large for its 32-bit field is rejected rather than truncated as HAProxy
// would. Expiry, counter periods, remaining lifetimes, and counter ages
// are Millis durations, never timestamps; a decoded Update records its
// reception time, against which those durations are measured.
//
// # Errors
//
// Every error wraps one class sentinel (ErrMalformed, ErrSchema,
// ErrState, ErrUnknownMessage) and, where one applies, a specific
// sentinel, so callers can test either with errors.Is. The one exception
// is a definition both over the table limit and rejected: still the
// single class ErrState, but with two specific sentinels,
// ErrTooManyTables and the rejection's own (see ErrTooManyTables).
// Details are bounded and quote little peer-supplied data.
package peermsg
