# 03 — Table and entry messages

Status: not started

Depends on: [02](02-wire-framing.md).

## One-turn outcome

Encode/decode the narrow table schema needed for request snapshots and ordinary
integer output, with stock HAProxy wire evidence.

## Work

- Support table definitions, table selection, entry updates, timed updates,
  acknowledgments, and required control messages for the chosen wire version.
- Support IPv6 keys, request counts, frequency counters, and the general-purpose
  integer field/array form selected for output. Reject unsupported schemas
  explicitly; do not attempt every HAProxy stored type in this phase.
- Preserve field ordering, array counts, periods, and complete entry values.
  Keep local and remote table-ID namespaces separate and scoped to a session.
- Record reception time with decoded updates. Keep wire counter age and
  remaining TTL as durations; do not mislabel either as an absolute timestamp.
- Generate fresh captures from the phase-01 lab and compare selected fields to
  small runtime-table reads. Include a table-ID collision between two sources.
- Document explicit versus implicit update IDs, serial-number wrap behavior,
  and definition-switch behavior from upstream sources.

## Acceptance

- [ ] Fresh HAProxy messages decode into expected keys, counts, periods, and TTLs.
- [ ] IPv4-mapped and native IPv6 bytes round-trip without key collisions.
- [ ] Timed and ordinary updates preserve their distinct expiration semantics.
- [ ] Independent sources can use the same numeric table ID safely.
- [ ] Partial stored values are rejected or completed only according to a
      documented schema rule; they cannot be serialized with missing fields.
- [ ] Wrong key length, field order, array count, and unsupported schema fail.
- [ ] Update-ID boundary and wrap fixtures pass without inventing ordering from
      numeric magnitude alone.

## Stop and handoff

Do not implement the whole HAProxy type catalog. Record the supported wire
subset and its mapping to Go values here for the following phases.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 02.
