# 03 — Table and entry messages

Status: complete

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

- [x] Fresh HAProxy messages decode into expected keys, counts, periods,
      and TTLs.
- [x] IPv4-mapped and native IPv6 bytes round-trip without key collisions.
- [x] Timed and ordinary updates preserve their distinct expiration semantics.
- [x] Independent sources can use the same numeric table ID safely.
- [x] Partial stored values are rejected or completed only according to a
      documented schema rule; they cannot be serialized with missing fields.
- [x] Wrong key length, field order, array count, and unsupported schema fail.
- [x] Update-ID boundary and wrap fixtures pass without inventing ordering from
      numeric magnitude alone.

## Stop and handoff

Do not implement the whole HAProxy type catalog. Record the supported wire
subset and its mapping to Go values here for the following phases.

### Supported wire subset (`internal/peermsg`)

Integers use HAProxy's encoding (`peerwire.AppendUint`) unless marked
"4-byte", which is big-endian. IDs are typed by namespace:
`RemoteTableID` (announced by the peer; carried by received definitions,
switches, and updates, and by acks this side sends) and `LocalTableID`
(announced by this side; carried by acks received). Keep one
`peermsg.Inbound` and one `peermsg.LocalTables` per session.

| Wire                                 | Layout                                                                                                           | Go                                                           |
| ------------------------------------ | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| Definition `0a 82`                   | table ID, name len, name, key type, key len, data-type bitfield, expiry ms, then per counter/array: type, params | `DecodeDefinition` → `RemoteTableID`, `Definition`           |
| Switch `0a 83`                       | table ID                                                                                                         | `DecodeSwitch`, `AppendSwitch(LocalTableID)`                 |
| Update `0a 80` / `0a 81`             | [4-byte update ID, 0x80 only], key, values                                                                       | `DecodeUpdate` → `Update{ID, ExplicitID, Key, Values}`       |
| Timed update `0a 85` / `0a 86`       | [4-byte update ID, 0x85 only], 4-byte remaining ms (at most 0x7fffffff), key, values                             | `Update{Timed: true, Remaining}`                             |
| Ack `0a 84`                          | table ID, 4-byte update ID                                                                                       | `DecodeAck` → `LocalTableID`, `UpdateID`; `AppendAck`        |
| Control `00 0x` (0–4), error `01 0x` | no body                                                                                                          | `Control{Type}`, `ErrorMessage{Type}` (via `Inbound.Decode`) |

| Schema element          | Wire                                     | Go value                                                                                   |
| ----------------------- | ---------------------------------------- | ------------------------------------------------------------------------------------------ |
| Key type IPv6           | key type 5, key length 16                | `KeyTypeIPv6`; `Key [16]byte` (byte equality; `KeyFromAddr` maps 4-byte IPv4 to `::ffff:`) |
| `http_req_cnt`          | bit 9, one integer                       | `Value{Type: DataHTTPReqCnt, Uint uint32}`                                                 |
| `http_req_rate(period)` | bit 10, param period ms; age, curr, prev | `Field{Period Millis}`; `Value{Freq: FreqCounter{Age Millis, Curr, Prev uint32}}`          |
| `gpt(n)` (output form)  | bit 22, param n (1..100); n integers     | `Field{ArrayLen}`; `Value{Array []uint32}` (len n)                                         |
| Expiry, remaining, age  | milliseconds                             | `Millis` (uint32 duration; `.Duration()`), never a timestamp                               |
| Reception time          | not on the wire                          | `Update.Received` (caller-supplied, keep the monotonic reading)                            |

Rules the following phases rely on:

- Everything else (other key types or lengths, expiry 0, any other
  data-type bit, parameters out of order) fails with `ErrSchema` plus a
  specific sentinel. HAProxy announces expiry 0 for a table without
  `expire`, whose entries never age out; it is rejected (`ErrExpiry`) so
  that a lifetime of 0 always means "expired". `Inbound` binds the
  rejected table's ID and name and selects it, so its updates fail with
  `ErrRejectedTable` instead of being attributed to another table or
  dropped silently; this deliberately differs from HAProxy, which selects
  nothing and leaves the name on its old ID (stock senders never
  redefine a table with another schema, so neither case arises with
  them). Whether a rejected table ends the session is phase 04's
  call (HAProxy ignores such definitions).
- Every 32-bit field must fit; HAProxy would truncate, `peermsg` returns
  `ErrMalformed` wrapping `peerwire.ErrIntRange`. Trailing bytes after the
  last field fail (`ErrTrailingData`).
- Partial values: none. Decoding requires every declared field; encoding
  requires `Values` to match the definition exactly (count, order, type,
  array length, unused members zero), else `ErrValues`. Nothing is
  completed with defaults.
- `Update.Lifetime(tableExpiry)`: ordinary updates restart the full table
  expiry; timed updates grant `min(Remaining, tableExpiry)`, as HAProxy
  caps them for remaining lifetimes up to `MaxRemaining` (0x7fffffff).
  Larger values are negative to HAProxy, which reads the field as a
  signed int, so its cap does not apply and the releases disagree; they
  fail with `ErrRemaining` (`ErrMalformed`) when decoding and encoding,
  and `Lifetime` returns 0 for one built by hand.
- Update IDs are 32-bit serial numbers: implicit = previous + 1 with wrap;
  explicit IDs are taken as sent, never ordered by magnitude. An implicit
  update with no previous update since the table's definition fails with
  `ErrImplicitID`.
- Senders must send explicit IDs to HAProxy 3.4.6 (see the defect below)
  and keep whole messages within 16384 bytes (`MaxEncodedMessage`).

## Execution record

- Commands and versions (2026-10-03, Debian 13 amd64): Go `go1.27.1`
  (repo-pinned); stock HAProxy `3.4.6-56332c5` (binary SHA-256
  `81df335a…d1c4`) and `3.2.25-70469d3` (`5f89a725…8869`) from phase 01's
  `make haproxy` (not rebuilt).
  - `make check`: pass (0 lint issues, `go test -race ./...` ok, no
    vulnerabilities; log `artifacts/check-03.log`). Lab tests skip
    without `HTA_HAPROXY`; committed fixtures replay without HAProxy.
  - `make peerwire-captures` (now runs `TestLiveCapture` in
    `internal/peerwire` and `TestLiveTableCapture` in `internal/peermsg`):
    both versions pass and wrote
    `push.txt`, `collision-a.txt`, and `collision-b.txt` under
    `internal/peermsg/testdata/captures/{3.4.6,3.2.25}/`.
    The regenerated phase 02 fixtures differed only in ports, PIDs,
    timestamps, rate-counter ages, and whether HAProxy's table push lands
    before or after its error reply (as phase 02 recorded), so they were
    restored to the committed copies. Their comment headers matched apart
    from the test peer's port, so the capture format refactor is
    compatible.
  - `make lab-test` (race detector on): both versions pass, including
    `TestLiveTableCapture` and phase 01/02 tests (log
    `artifacts/lab-test-03.log`).
  - `make fuzz-smoke` (10 s per target): `FuzzInbound` (about 253k
    executions) and the five phase 02 targets pass (log
    `artifacts/fuzz-smoke-03.log`).
- Upstream sources (read to confirm facts, nothing copied or translated):
  `src/peers.c` from the verified tarballs, 3.4.6 SHA-256
  `59a3e188…4402`, 3.2.25 `edc1838c…cd47`; `include/haproxy/stick_table-t.h`
  and `src/stick_table.c` (data-type numbering and catalog, identical in
  both); `doc/peers.txt` (phase 02 digest). WoltLab code was not consulted.
- Fixtures: three fresh captures per version, written only by
  `TestLiveTableCapture`, each with HAProxy version, binary SHA-256, PID,
  Go version, capture time, the full HAProxy configuration, every runtime
  command, and every runtime reply line (`note reply:`). `push` runs real
  HTTP traffic through a lab frontend
  (`req.hdr_ip(X-Lab-Client),ipmask(32,64)`), dials HAProxy's peers bind,
  records the push, requests a full resync, sends one more request,
  then sends output updates encoded by `peermsg`. `collision-a`/`-b` are
  two HAProxy processes whose table ID 1 names `t_in` and `t_out`
  respectively. The capture format and connection driver moved from
  `internal/peerwire` tests to the shared test package
  `internal/peerwire/peertest` (no format change).
- Acceptance evidence (boxes left for the orchestrator after review):
  - Fresh messages decode to expected keys, counts, periods, and TTLs:
    `TestTableCaptureFixtures` (and the same checks in the live test) on
    both versions. Definitions decode to `t_in` = ID 1, IPv6/16, expiry
    30000, `http_req_cnt`, `http_req_rate(10000)`, and `t_out` = ID 2,
    `gpt(3)`. Pushed and resynced entries `2001:db8:1:1::` (5),
    `::ffff:192.0.2.1` (4), and `2001:db8:1:2::` (1, then 2 live) match
    `show table`. The push matches the read taken right after the traffic,
    inside the counter's first period: `http_req_cnt`, and
    `http_req_rate(10000)` equal to the current-period count with previous
    0. The resync and replayed updates repeat the push's counts with a
    counter age that never shrinks, and agree with the read taken after
    them (`http_req_cnt` equal, events split between current and previous,
    printed estimate no larger), which holds however many periods later
    they arrive. The preset
    `t_out` entry decodes to `[7 0 4294967295]` (runtime `gpt0..2`). Timed
    remaining lifetimes are at most 30000 and at least the later runtime
    `exp`. Every HAProxy definition, update, and ack re-encodes to
    HAProxy's exact bytes (`checkReencode`), and hand-written bodies from
    the documented layout decode and encode (`TestDecodeDefinitionFixtures`,
    `TestUpdateFixtures`).
  - IPv4-mapped and native IPv6 without collisions: `TestKeysDoNotCollide`
    round-trips nine keys including `::ffff:192.0.2.1`, `::192.0.2.1`,
    `2001:db8::c000:201`, `64:ff9b::c000:201`, `2002:c000:201::`, and
    `::ffff:0:c000:201`; all stay distinct, `Addr()` never unmaps, and a
    4-byte IPv4 address keys as HAProxy stores it (IPv4-mapped). Live:
    the push carries mapped and native keys as distinct entries, and the
    output updates `::ffff:198.51.100.7` and `::198.51.100.7` read back
    (`show table t_out key ...`) as two entries with their own values.
  - Timed versus ordinary expiration: decoded ordinary updates have
    `Lifetime(30000) = 30000`; timed ones `Remaining` (capped:
    `TestLifetime`; signed-int boundary 0x7fffffff accepted, 0x80000000
    and 0xffffffff rejected both ways: `TestRemainingBoundary`). Live,
    HAProxy's resync uses `0x85`/`0x86` and its push
    and live updates `0x80`/`0x81`; in the other direction an encoded
    timed update with 4000 ms shows `exp=4000` in HAProxy while ordinary
    ones show `exp` near 30000 (`checkOutput`). Each output entry is read
    right after HAProxy acks it, so the 4-second entry cannot expire
    before its read.
  - Same numeric table ID from independent sources: `collision-a`/`-b`
    on both versions. Each stream decodes in its own `Inbound`: source A's
    ID 1 is `t_in` (count 10 for `2001:db8:1:1::`), source B's ID 1 is
    `t_out` (`gpt[1]` = 20) and its ID 2 is `t_in` (count 20); every
    update is attributed to its own source's binding.
    `TestNamespacesAreIndependent` shows the same with synthetic bodies,
    and that B's body fails in A's namespace. Our definition of `t_out`
    as local ID 1 (HAProxy's `t_in` number in the other direction) is
    accepted, and HAProxy's acks name local ID 1.
  - Partial values: `TestAppendUpdateRejectsPartialValues` (missing,
    extra, swapped, short/long/nil arrays, members of the wrong shape) all
    fail with `ErrValues` and leave `dst` unchanged; decoding a short or
    long array fails (`TestUpdateErrors`). Documented rule: never
    completed.
  - Wrong key length, field order, array count, unsupported schema:
    `TestDecodeDefinitionErrors` (25 cases: key length 4/17, integer and
    string key types, gpc0, conn_rate, gpc array, bit 63, parameter
    prefixes out of order, zero/101 array length, zero period, truncation,
    trailing bytes, ID 0, bad names), `TestDefinitionValidate`, and
    `TestUpdateErrors` (4- and 15-byte keys, array of 2/4, values over 32
    bits, missing counter fields).
  - Update-ID boundary and wrap: `TestUpdateIDSequence` walks
    0xfffffffe (explicit) → 0xffffffff → 0 (timed implicit) → 1, back to
    explicit 5 → 6, to explicit 3 → 4, and a repeated explicit 4, all
    accepted without magnitude checks; an implicit update right after a
    definition fails. Live, the output updates cross the wrap (explicit
    0xfffffffe, timed 0xffffffff, implicit 0, implicit 1); 3.2.25 acks
    `fffffffe, ffffffff, 0, 1`. In the push fixture each entry's derived
    or explicit ID in the resync equals its ID in the push.
  - Fuzzing: `FuzzInbound` frames fuzzed traffic (seeded with every
    capture stream) and decodes every frame; failures must be classified
    and every accepted message must re-encode to its exact bytes. A 20 s
    run reached about 515k executions without failure.
- Upstream-documented behaviors (from `src/peers.c` in both releases):
  - Explicit versus implicit IDs. A sender sends an explicit ID unless
    the ID is exactly the last pushed one plus one
    (`!last_pushed || updateid < last_pushed || updateid - last_pushed != 1`),
    and always for the first update of each teach pass, so every update
    after a definition is explicit. The receiver derives an implicit ID
    from its `last_get`, reset to 0 per session.
  - Serial-number wrap. Update IDs are 32-bit and wrap. The sender's rule
    above forces an explicit ID after a wrap. Teaching walks the update
    tree with modular distances (`eb->key - last_pushed`), and a stale ack
    is detected with `(int)(localupdate - update) < 0` (half-range serial
    arithmetic). A resync replays IDs from an arbitrary origin and wraps
    round to the tree start, so magnitude carries no order.
  - Definition switch. HAProxy announces a table, and switches to it, by
    sending its definition again (`peer_prepare_switchmsg` builds `0x82`);
    it never sends `0x83` but accepts it, selecting the table whose remote
    ID matches, or none. A received definition first unbinds every table
    holding that ID, then binds the named table, which also moves the
    name off its previous ID. Definitions for unknown tables, other key
    types or sizes, or mismatched parameters are ignored, not errors, and
    leave no table selected, so the updates that follow are silently
    dropped. Updates are sent as timed (`0x85`/`0x86`) during a resync the
    peer requested, and untimed otherwise.
- Decisions or deviations:
  - Selected output form: `gpt(n)` (data type 22), an array of unsigned
    32-bit tags. It is set-only (`sc-set-gpt`, the runtime `data.gpt[i]`),
    unlike `gpc`, which request rules can increment; it is learned from
    peers without `recv-only` (not `is_local`) and holds several fields in
    one entry. Phases 05–06 fix the field layout. `gpt0`, `gpc`, and native
    rate output are not supported.
  - Key form: the lab and production rules (`ipmask(32,64)` into an IPv6
    table, with the owner-accepted v6tov4 folding) always produce 16-byte
    IPv6 keys, IPv4 sources as `::ffff:a.b.c.d`. Only that key type is
    supported.
  - Stricter than HAProxy, failing closed: mandatory expiry and
    parameters, no truncation of 32-bit fields, no trailing bytes, no table
    ID 0 (HAProxy's "unbound" value), printable non-empty names, and no
    implicit ID without a base.
  - Reception time is an argument of `Inbound.Decode`/`DecodeUpdate`;
    `peermsg` never reads a clock.
  - `Inbound` keeps the last update ID per binding (kept across switches,
    reset by a definition) and bounds bindings (`NewInbound(max)`, clamped
    to `MaxTables` = 65536); `LocalTables` assigns IDs from 1 in
    registration order, as HAProxy does.
  - The capture format and driver moved to `internal/peerwire/peertest`;
    `make peerwire-captures` now regenerates both packages' fixtures.
- Findings for later phases:
  - Upstream defect (of the pinned builds, 3.4.6 only; by source every
    release since 3.3.0, from upstream `f12252c7a`; minimal reproduction
    in the `push` fixture; draft upstream report in
    [`docs/upstream/peers-implicit-update-id.md`](../upstream/peers-implicit-update-id.md)):
    every implicit update ID is recorded as
    `htonl(last_get + 1)` (`peer_treat_updatemsg`), so on a little-endian
    host each one is recorded and acknowledged byte-swapped, and the next
    implicit ID is derived from the swapped value. In the fixture the
    first implicit ID is 0, identical in both byte orders; the second (1)
    is recorded and acknowledged as `0x01000000`, and `show peers` reports
    `last_get=16777216`. 3.2.25 increments `last_get` and acks 1. 3.4.6
    also skips recording IDs while it is itself learning a resync (source
    only; not exercised). The
    values are still applied. Phase 04/05 senders must send explicit IDs
    (`0x80`/`0x85`) to stay correct on both versions; `AppendUpdate`
    documents this.
  - Confirming a resync makes HAProxy re-push its tables from the resync
    origin: unchanged entries are replayed with their original IDs and
    counts (asserted in `checkLive`). Phase 07 must treat this as replay,
    not new contribution.
  - HAProxy acks only updates it applied to a matching table; a
    definition it ignores (wrong name, key type, or size) produces no
    error and no acks. Phase 05 should use acks plus a runtime read to
    confirm an output table actually accepted our schema. HAProxy parses
    and drops values for data types its own table does not store, and
    leaves values it stores but we do not send unchanged.
  - A counter whose period never started has `curr_tick` 0, so its wire
    age is the sender's `now_ms` (source only: `clock.c`). HAProxy starts
    that clock about 20 s before it wraps, so the age is near 2^32 ms
    only in roughly the first 20 s of the process (as in the lab
    captures) and arbitrary afterward. Phase 09 must not use age
    magnitude to detect an unstarted counter; only `Curr` and `Prev`
    both zero show it holds no events.
  - Remaining lifetimes of 0x80000000 and above (review finding R1,
    reviewer's live probe with explicit timed `gpt(3)` updates on both
    pinned builds): `peer_treat_updatemsg` reads the field into a signed
    int, so the `expire > table->expire` cap never applies and the tick
    arithmetic gets a negative delay. Remaining 4000, 0x7fffffff,
    0x80000000, and 0xffffffff gave `exp` 3999, 29999, 2147483647, and
    no entry on 3.4.6, and 3999, 30000, 0, and no entry on 3.2.25. Stock
    senders cannot emit such values (table expiry is a signed int), so
    `peermsg` rejects them both ways (`MaxRemaining`, `ErrRemaining`).
- Review fix batch 1: R1 (signed remaining lifetime, above); R2 (age
  documentation, above, and `FreqCounter.Age`); C1 (live waits now have
  a 15 s whole-wait deadline that names what was awaited; `peertest`
  frame reads return read errors instead of failing); C2 (the 3.4.6
  byte-swapped ack expectation depends on HAProxy's host byte order: the
  committed fixtures assume their little-endian capture host, live runs
  use the running host's order). No fixture was regenerated.
- Review fix batch 2: C3 (each output entry is now read with
  `show table t_out key <k>` right after its ack, replacing one read after
  all four, which a slow run could reach after the 4 s timed entry
  expired); C4 (only the read taken right after the traffic is assumed
  to be in the counter's first period; resync and later checks no
  longer assume it). C3 changes the capture procedure, so the six
  `internal/peermsg` fixtures were regenerated with
  `make peerwire-captures`; the phase 02 fixtures it also rewrote were
  restored, as before.
- Review: independent review plus cross-model (codex) passes on the
  first, r1, and r3 diffs. Two fix batches resolved six findings (R1
  Medium, R2, C1–C4 Low); the reviewer gave a clean verdict on r2. Batch 3
  (below) applied optional suggestions and changed production code
  (`ErrExpiry`, double-wrapped over-limit error). Its verification
  confirmed all items but O4 and raised R3 (Low): the over-limit error now
  matches both `ErrState` and `ErrSchema`, contradicting the one-class
  contract in `errors.go` and `doc.go`. On r3 the reviewer re-ran
  `make check`, `make lab-test` on both pinned builds, and `FuzzInbound`
  (20 s); all pass. Codex found nothing on r3. Batch 4 (owner-approved
  past the three-batch cap, no codex pass) fixed R3; the reviewer
  confirmed it on r4 with `make check`, `FuzzInbound` (15 s), and an
  error-class probe, and gave a clean verdict. Batch 5 (two optional
  wording nits) was confirmed clean on r5 with `make check` and a probe.
  `make lab-test` was not re-run after r3: no live path or fixture
  changed.
- Review fix batch 3 (the reviewer's optional suggestions, at the
  owner's request): O1 `Inbound` doc labels its two deliberate
  deviations for rejected definitions; O2 the 3.4.6 defect affects every
  implicit ID (the fixture's first one is 0, identical in both byte
  orders); O3 expiry 0 is rejected with `ErrExpiry` rather than given a
  special lifetime meaning; O4 a rejected definition over the table limit
  reports `ErrTooManyTables` and its schema reason (revised in batch 4);
  O5
  `Key.String` documents that IPv4-compatible keys print in hex, unlike
  HAProxy; O6 long lines wrapped; O7 a live wait ended by HAProxy closing
  the connection says so. No fixture was regenerated.
- Review fix batch 4: R3 (owner decision D6). A definition both
  rejected and over the table limit binds nothing, so its error keeps the
  single class `ErrState` with `ErrTooManyTables`, plus the rejection's
  specific sentinel (such as `ErrDataType`) for `errors.Is` and the
  message, but not the `ErrSchema` class. `TestInboundLimitsAndClasses`
  asserts that `ErrState`, `ErrTooManyTables`, and `ErrDataType` match and
  `ErrSchema` does not.
- Review fix batch 5: N1 the error-contract headers in `errors.go` and
  `doc.go` name this one exception (single class, two specific
  sentinels); N2 its message now quotes the rejection's detail (for
  example "data type 2", within the usual bound) instead of a boolean,
  and an accepted definition over the limit has no rejection text.
- Remaining work / next action: phase 04 (peer sessions) consumes
  `internal/peermsg`.
