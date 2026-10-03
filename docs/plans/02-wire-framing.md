# 02 — Wire framing and integers

Status: complete

Depends on: [01](01-local-lab.md).

## One-turn outcome

A bounded, incremental Go framing layer for the selected HAProxy peers protocol,
with independently justified integer and fragmentation fixtures.

## Work

- Pin the protocol documentation to the selected upstream releases and record
  provenance. Implement HAProxy's integer encoding, not a generic varint codec
  assumed to be equivalent. Use unsigned fixed-width types and checked lengths.
- Separate text handshake framing from binary message framing. Support partial
  reads, several messages in one read, and binary bytes following the handshake.
- Set explicit maximum handshake, message, and buffered-input sizes. Reject
  overflow, invalid lengths, truncation at EOF, and unsupported mandatory forms.
- Define stable errors and document buffer ownership and parser side effects.
- Author boundary fixtures from the specification and fresh lab captures.
  Round trips supplement independent fixtures rather than replacing them.
- Add native Go fuzz targets for integer decoding and incremental framing.

## Acceptance

- [x] Boundary integers, including 32-bit limits, have checked expected bytes.
- [x] A frame parses identically across all tested chunk boundaries.
- [x] Multiple frames and handshake-plus-binary input preserve every byte.
- [x] Truncated and oversized input produces bounded, actionable errors.
- [x] Fuzz seeds contain valid and malformed cases; a bounded fuzz run completes
      without a panic or unbounded allocation.
- [x] Unit tests pass with the race detector where applicable.
- [x] Fixtures have recorded origins and no copied LGPL implementation code.

## Stop and handoff

No network state machine or aggregation. Define the framing interfaces that
phase 03 will consume. Record any version-specific framing differences here.

## Execution record

- Commands and versions (2026-10-03, Debian 13 amd64): Go `go1.27.1`
  (repo-pinned); stock HAProxy `3.4.6-56332c5` (binary SHA-256
  `81df335a…d1c4`) and `3.2.25-70469d3` (`5f89a725…8869`) from phase 01's
  `make haproxy`.
  - `make check`: pass (0 lint issues, `go test -race ./...` ok, no
    vulnerabilities). Lab tests skip without `HTA_HAPROXY`; the committed
    capture fixtures are replayed without HAProxy.
  - `make fuzz-smoke` (10s per target, minimization capped at 1s):
    `FuzzDecodeUint`, `FuzzUintRoundTrip`, `FuzzDecoder`, `FuzzHandshake`,
    and `FuzzCursor` all pass.
  - `make lab-test` (now `./internal/...`, race detector on): both versions
    pass, including `TestLiveCapture` (fresh capture checked like the
    fixtures) and the new `TestCustomCloseStops`.
  - `make peerwire-captures` regenerated
    `internal/peerwire/testdata/captures/{3.4.6,3.2.25}/*.txt` (12 cases
    each).
- Protocol documentation provenance: `doc/peers.txt` ("Peers protocol
  2.1") extracted from the verified release tarballs
  `haproxy-3.4.6.tar.gz` (`791e1815…e68b`, VERDATE 2026-09-28) and
  `haproxy-3.2.25.tar.gz` (`d59a68d0…866e`, VERDATE 2026-09-28). The two
  copies are byte-identical, SHA-256
  `1378d18de6293dae098c5c474c641f1873c1d68749d6dcb6cd5b7ed6a1d0ba71`; the
  package doc (`internal/peerwire/doc.go`) records the same digest.
- Framing interface for phase 03 (`internal/peerwire`, no I/O, no session
  or table state):
  - `Decoder`: `NewDecoder(Limits)`, `Feed` (copies; all-or-nothing within
    `Free`), `CloseInput`, `NextLine` (handshake; strips `\n` and a
    preceding `\r`), `NextFrame` (binary; first call switches mode for
    good). `ErrNeedMore` consumes nothing; framing failures are sticky
    `*Error` values with a stream offset. Handshake parse/encode and
    `Limits` failures are plain errors; all wrap a stable sentinel. Returned lines and `Frame.Body`/`Raw`
    alias the buffer until the next `Feed`.
  - `Limits{MaxHandshake, MaxMessage, MaxBuffered}`, defaults 4096 /
    16384 / 32782; `Validate` requires the buffer to hold the largest frame
    and the whole handshake, so `Free > 0` whenever `ErrNeedMore` is
    returned.
  - `Frame{Class, Type, Body, Raw, Offset}`, `MessageType.Variable()`,
    class/type constants, `AppendFrame`.
  - Integers: `AppendUint`, `EncodedLen`, `DecodeUint`, `DecodeUint32`;
    `Cursor` (`Uint`, `Uint32`, `Fixed32`, `Bytes(uint64)`, `Rest`) for
    checked body reads.
  - Handshake: `Hello`, `AppendHello`, `AppendStatus`, `ParseHello`,
    `ParseVersionLine`, `ParsePeerNameLine`, `ParseSenderLine`,
    `ParseStatusLine`, `StatusCode` constants.
  - Stable sentinels: `ErrNeedMore`, `ErrTruncated`, `ErrIntTruncated`,
    `ErrIntOverflow`, `ErrIntRange`, `ErrLengthEncoding`,
    `ErrMessageTooLarge`, `ErrHandshakeTooLarge`, `ErrInputLimit`,
    `ErrInputClosed`, `ErrReservedClass`, `ErrHandshakeDone`,
    `ErrBadHandshake`, `ErrShortBody`, `ErrFixedBody`, `ErrInvalidLimits`.
- Acceptance evidence (boxes left for the orchestrator after review):
  - Boundary integers: `specEncodings` (`varint_test.go`) gives expected
    bytes for 28 values, including every 1–10-byte minimum and maximum,
    2^31±1, 2^32−1, 2^32, 2^63−1, 2^63, and 2^64−1, worked from the
    specification's pseudo-code and worked example, and checked by an
    arbitrary-precision evaluation of the spec's decode sum. The live
    capture stores each value through the runtime API (`gpc0` for 32-bit,
    `bytes_in_cnt` for 64-bit), and `TestCaptureFixtures` requires HAProxy's
    own bytes to equal the table for every value, in both versions.
  - Chunk boundaries: every capture stream (both directions, 24 fixtures)
    and a synthetic stream are framed whole, byte by byte, and split in two
    at every offset; results must be identical.
  - Multiple frames and handshake-plus-binary: `checkPreserved` requires
    lines and frames to tile the input with no gap. In the acceptor
    fixtures HAProxy's table push directly follows its `200\n`, so the
    decoder must keep the bytes after the status line for binary framing.
  - Truncation and size: each proper prefix of the synthetic stream ends in
    `io.EOF` at a boundary or a bounded `ErrTruncated` naming the offset.
    `TestDecoderLimits` covers body at/over limit (detected from the header
    alone), 5- and 6-byte lengths, reserved class, handshake at/over budget,
    and unterminated lines. The `size-limit`, `length-encoding`, and
    `reserved-class` fixtures show HAProxy answering the same inputs with
    size-limit/protocol errors, and the decoder reports the matching
    sentinel on the test peer's stream.
  - Fuzzing: seeds include all capture streams, the spec table, and
    malformed cases (overflow, truncation, long length, reserved class,
    oversized handshake). `FuzzDecoder` uses small and default limits and
    checks the buffer capacity never exceeds `MaxBuffered`. `FuzzCursor`
    runs fuzzed sequences of `Cursor` reads over fuzzed bodies (seeded with
    every capture frame body and the spec encodings). Each read must stay
    in bounds and consume exactly what it returns, or nothing with a
    documented sentinel. `Bytes` with a length near 2^64 must not allocate
    in proportion to it.
  - Race: `make check` and `make lab-test` run with `-race`.
  - Origins: fixtures are fresh captures written only by `TestLiveCapture`,
    each with HAProxy version, binary SHA-256, PID, Go version, capture
    time, and the HAProxy configuration in its header. No WoltLab code was
    consulted. The encoder implements the specification's pseudo-code.
- Decisions or deviations:
  - The specification and stock HAProxy disagree: the acknowledgement is
    `0x84`, not 133, and `0x85`/`0x86` are timed (incremental) updates. The
    names follow the wire, with the discrepancy documented. HAProxy frames by
    bit 7 of the type byte regardless of class, ignores unknown classes and
    types (`unknown-messages` fixture), and rejects class 255. The decoder
    does the same: it rejects only class 255 and returns other unknown
    messages.
  - HAProxy limits (observed live, both versions): a declared body over
    `tune.bufsize` (16384) draws `01 01` at once; a message is processed
    only if its whole wire size fits in 16384 bytes. A 16384-byte message is
    processed (`frame-at-bufsize`); 16385 bytes stall the session until
    HAProxy's 5-second timeout, with no error (`frame-over-bufsize`).
    Messages of 16386 to 16389 bytes (a body of at most 16384 bytes takes
    at most a 3-byte length) should stall the same way but were not
    captured. HAProxy never sends a larger message. `DefaultMaxMessage` is 16384
    (body), a superset of what HAProxy sends. Phase 04 senders must keep
    whole messages within the peer's `tune.bufsize`.
  - Integer decoding is stricter than HAProxy's in one place. Encodings past
    64 bits (`ErrIntOverflow`) are rejected; HAProxy's C decoder would
    shift past 64 bits, but its encoder never produces them. The encoding
    is bijective, so there are no overlong forms to reject. Message lengths
    are limited to 5 encoded bytes, as HAProxy enforces. A 5-byte length
    above 2^32 − 1 is rejected as `ErrMessageTooLarge`; HAProxy would
    truncate it to 32 bits and then apply its size limit.
  - Handshake parsing is stricter than HAProxy's: single spaces, decimal
    32-bit PIDs, printable-ASCII peer names, and exactly three status
    digits. HAProxy always sends that form. HAProxy accepts `\r\n`; the
    decoder does too. Version support (HAProxy accepts 2.0 and 2.1) is left
    to the session layer.
  - The C source (`src/peers.c` in the pinned tarballs) was read only to
    confirm facts: constants, the 5-byte length cap, `\r` stripping, and
    the receive path. No code was copied or translated. Every such fact is
    also pinned by a capture.
  - `internal/lab` gained `StartCustom` (one owned HAProxy with a
    caller-supplied config and inherited listeners; same cleanup guarantees)
    and `labtest.StartCustom`. `startNode` now shares `Node.spawn`.
    `Node.Addrs` includes custom listeners and omits empty fields.
- Version differences: none in framing. `src/peers.c` framing functions
  (`intencode`, `intdecode`, message receive, line reading, version parsing)
  differ only in internal buffer APIs and tracing between 3.2.25 and 3.4.6.
  Capture fixtures differ only in PIDs, rate-counter tick ages, and whether
  HAProxy's table push lands before or after its error reply.
- Observations for phase 03 (not acted on here): when teaching on
  connect, HAProxy sends a definition, one `0x80` update with an explicit
  ID, then `0x81` incremental updates (key and data only).
  `http_req_rate` set through the runtime API encodes a tick age near
  2^32 (an unsigned wrap of `now - curr_tick`). Timed updates
  (`0x85`/`0x86`) did not appear in these captures.
- Review: independent review plus a cross-model (codex) pass on the first
  and final diffs. Four low-severity findings were fixed in two batches:
  a spent handshake budget now yields `ErrNeedMore`/`io.EOF` instead of
  `ErrHandshakeTooLarge`; the stall range above was corrected; positionless
  errors no longer print "offset 0"; the integer fuzz oracle stops once
  overflow is certain. Final verdict clean; the reviewer re-ran
  `make check`, `FUZZTIME=20s make fuzz-smoke`, and the lab tests against
  both versions on the fixed code.
- Follow-up (after closeout): `make fuzz-smoke` used to spend much of
  `FuzzDecoder`'s budget minimizing inputs grown from the large capture
  seeds. It now passes `-fuzzminimizetime=$(FUZZMINIMIZETIME)` (default
  1s, overridable). With a cleared fuzz cache, `FuzzDecoder` sustained
  about 12k–17k execs/s at every 3-second report of a 10s smoke run, and
  13k–40k/s over 20s, with no 0/s interval before the final summary line.
  `FuzzCursor` was added (about 17k–33k execs/s). The package doc and
  `Error` doc now say that misuse returns (`ErrHandshakeDone`,
  `ErrInputClosed`) are bare sentinels.
- Remaining work / next action: phase 03 (table and entry messages)
  consumes `internal/peerwire`.
