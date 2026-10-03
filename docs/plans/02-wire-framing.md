# 02 — Wire framing and integers

Status: not started

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

- [ ] Boundary integers, including 32-bit limits, have checked expected bytes.
- [ ] A frame parses identically across all tested chunk boundaries.
- [ ] Multiple frames and handshake-plus-binary input preserve every byte.
- [ ] Truncated and oversized input produces bounded, actionable errors.
- [ ] Fuzz seeds contain valid and malformed cases; a bounded fuzz run completes
      without a panic or unbounded allocation.
- [ ] Unit tests pass with the race detector where applicable.
- [ ] Fixtures have recorded origins and no copied LGPL implementation code.

## Stop and handoff

No network state machine or aggregation. Define the framing interfaces that
phase 03 will consume. Record any version-specific framing differences here.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 01.
