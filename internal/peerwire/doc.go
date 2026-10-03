// Package peerwire frames the HAProxy peers protocol: the text handshake,
// the binary message stream that follows it, and HAProxy's own
// variable-length integer encoding. It holds no session state and assigns
// no meaning to stick-table messages beyond what framing needs; session
// handling and table/entry decoding belong to later layers.
//
// # Protocol reference
//
// The authority is doc/peers.txt ("Peers protocol 2.1") as shipped in the
// HAProxy 3.4.6 and 3.2.25 release tarballs. Both copies are byte-identical
// (SHA-256 1378d18de6293dae098c5c474c641f1873c1d68749d6dcb6cd5b7ed6a1d0ba71).
// Where that text and stock HAProxy disagree, this package follows what
// the pinned releases put on the wire, as recorded in fresh lab captures
// under testdata/captures:
//
//   - The update acknowledgement is type 0x84, not 133 as the document's
//     table says, and types 0x85 and 0x86 are timed updates.
//   - A message is variable-length exactly when bit 7 of its type byte is
//     set, whatever its class. Unknown classes and types are framed and
//     returned; class 255 is reserved and rejected.
//   - A message length is encoded in at most 5 bytes; a longer encoding
//     draws a protocol error.
//   - HAProxy refuses a declared body longer than its tune.bufsize (16384
//     by default) with a size-limit error, and consumes a message only once
//     all of it fits in that buffer: a 16384-byte message is processed,
//     while a 16385-byte one stalls the session until HAProxy times it out.
//     Longer messages with a body of at most 16384 bytes (up to 16389
//     bytes, since such a length encodes in at most 3 bytes) are expected
//     to stall the same way; only 16385 bytes was captured. DefaultLimits
//     accepts bodies up to 16384 bytes, a superset of what HAProxy sends;
//     senders must keep whole messages within the receiver's
//     tune.bufsize.
//   - Handshake lines end in "\n"; a "\r" just before it is ignored.
//
// # Incremental decoding
//
// Decoder accepts input in arbitrary chunks through Feed and yields
// handshake lines (NextLine) and then binary frames (NextFrame). Input that
// does not yet hold a complete line or frame yields ErrNeedMore and is kept
// for the next call. Every limit is explicit (Limits) so that a peer cannot
// make the decoder buffer or allocate without bound.
//
// # Errors
//
// Every failure wraps one of the package's sentinel errors, so callers can
// test it with errors.Is. Framing failures from Decoder, ErrInputLimit from
// Feed, and every Cursor failure are positional *Error values carrying the
// stream or body offset and a bounded description. Decoder misuse is
// reported with the bare sentinels ErrHandshakeDone (NextLine after
// NextFrame) and ErrInputClosed (Feed after CloseInput), as are the
// non-failures ErrNeedMore and io.EOF. Handshake parse/encode failures and
// Limits/NewDecoder failures have no position and are plain errors with a
// bounded description. Decoder errors that mean the stream can no longer
// be framed are sticky: the decoder returns the same error from then on.
package peerwire
