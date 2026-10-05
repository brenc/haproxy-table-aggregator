# 08 — Counter aggregation

Status: complete

Depends on: [07](07-source-snapshots.md).

## One-turn outcome

Correct diagnostic totals from current source snapshots, including repeated
updates and source entry replacement, without a claim of durable accounting.

## Work

- Sum the latest retained `http_req_cnt` contribution per source/key with checked
  wide arithmetic. Use these totals as test diagnostics, not a lifetime quota.
- Recompute or adjust the sum by replacement; never treat absolute counter
  snapshots as additive events. Source snapshot storage remains authoritative.
- Specify behavior when a count decreases: entry recreation, reset, or wrap
  cannot silently become a huge positive delta. Preserve observed state and
  expose uncertainty where continuity cannot be established.
- Build an independent trace oracle from accepted source snapshots and known
  lab requests. Do not call the production merge routine to compute expectations.
- Keep source expiry and membership changes visible in diagnostics and readiness.
  Removing a required source requires an explicit roster configuration change.
- Carried in from phase 07: `snapshot.Store.Contributions` and `Lookup` do
  not check source state, because snapshots are applied in place while a
  source syncs. Sum only contributions that `Store.Roster().Ready` (or the
  per-source state) certifies; never present a total built from a syncing,
  degraded, or missing source as complete.

## Acceptance

- [x] A=10, B=20, then A=11 yields 30 then 31; replaying A=11 stays 31.
- [x] A=100 and B=100 each increment once, producing 202 after convergence.
- [x] Repeated full snapshot records do not inflate totals.
- [x] Distinct keys, sources, and local table-ID collisions remain isolated.
- [x] Expired/replaced contributions disappear exactly once.
- [x] Counter reset and 32-bit boundary cases have explicit expected behavior.
- [x] Overflow in a sum or a proposed narrow output encoding is detected.
- [x] Known two-node HTTP traffic matches diagnostic counts before entry expiry.

## Stop and handoff

No rate calculation or new stored data types. Document the diagnostic total's
scope so later users cannot confuse it with billing or persistent request counts.

## Execution record

- Commands and versions (2026-10-04, Debian 13 amd64): Go `go1.27.1`
  (repo-pinned); stock HAProxy `3.4.6-56332c5` (binary SHA-256
  `81df335a…d1c4`) and `3.2.25-70469d3` (`5f89a725…8869`) from phase 01's
  `make haproxy` (not rebuilt).
  - `make check`: pass (0 lint issues, `go test -race ./...` ok, no
    vulnerabilities; log `artifacts/check-08.log`).
  - `make lab-test` (both versions, race detector on): pass, including
    the new `TestLiveCounterTotals` (30.5 s on each version) and every
    earlier live test (log `artifacts/lab-test-08.log`).
- Scope of the total (also the `internal/aggregate` package
  documentation): a diagnostic of snapshot correctness, the sum of the
  values the sources' entries hold now. It is not a billing count, quota,
  or durable/lifetime request total and must not be published or enforced
  as one: it forgets whatever an entry counted once the entry expires,
  is evicted, or is reset, is lost on restart, and inherits HAProxy's
  32-bit per-entry counter. Rates (phase 09) are evaluated separately.
- Acceptance evidence. Unit tests in `internal/aggregate` drive a real
  `snapshot.Store` with a fake clock; every total they read is compared
  with the independent trace oracle (`internal/aggregate/aggregatetest`),
  which replays the accepted/refused event trace and heartbeat
  observations with its own model and calls neither the store's merge
  nor `aggregate.Count`. The live test runs against both pinned builds.
  - A=10, B=20, A=11 gives 30, 31; replaying A=11 stays 31:
    `TestReplacementNotAdditive` (also the same record replayed by a
    resync in a new session); live `TestLiveCounterTotals` (10 requests
    on node a and 20 on node b give 30, one more on a gives 31).
  - A=100 and B=100 each increment once give 202: `TestEachIncrementsOnce`
    (both arrival orders, 200 → 201 → 202); live: one request sent on
    each node at the same time gives 33 = the responder's count, with
    each source equal to the requests its node forwarded.
  - Repeated full snapshot records do not inflate totals:
    `TestRepeatedFullSnapshots` (a teach repeated after three partial
    replies, then in three later sessions; the syncing source is visible
    but not counted); live: after `Disconnect("a")` the session-2 replay
    of node a's table leaves the total at 33, not uncertain.
  - Distinct keys, sources, and table-ID collisions stay isolated:
    `TestIsolation` (two tables and two keys; source b numbers the tables
    the other way round; an output table is `ErrUnknownTable`); live: node
    b declares the output tables first (`OutputFirst`), so the nodes
    announce `lab_in` as table ID 1 (a) and 3 (b); a key counted only on
    b (5 requests) stays 5 and separate throughout.
  - Expired/replaced contributions disappear exactly once:
    `TestExpiredAndReplacedOnce` (a replaced value leaves once; an entry
    leaves exactly at its deadline with no input, repeated reads and
    `Expire` change nothing, the source's `Expired` counts 1; a timed
    update with no lifetime left removes a value once); live: b's
    single-node key leaves the total at its deadline, `Expired` +1, and
    stays gone.
  - Counter reset and 32-bit boundary behavior: `TestDecreaseAndBoundary`,
    `TestDecreaseRecorded` (`internal/snapshot`). A value of
    `math.MaxUint32` is valid and sums exactly in 64 bits. Any accepted
    update that lowers a stored count (HAProxy's unsigned counter
    wrapping to 0, a runtime-API reset, or a recreation, possibly seen in
    a later session) replaces it: the total falls to the sum of the
    current values, never jumps by about 2^32, and the contribution is
    `Discontinuous` (`Entry.Decreases`, `Entry.LastDecrease` with
    from/to values and sessions) and the total `Uncertain` until the
    entry expires; a new entry after expiry starts clean. Live: `set
    table lab_in key 2001:db8:7:: data.http_req_cnt 4294967295` on node a
    gives a=4294967295 and the total 4294967316; one more request makes
    HAProxy report 0 (its own lookup header) and the total falls to 21,
    uncertain, last decrease 4294967295 → 0 (both versions).
  - Overflow detection: `TestAddChecked` (`bits.Add64` carry on the
    64-bit sum is an error wrapping `ErrOverflow`, never a wrap);
    `TestNarrowing` (`Total.Uint32`, the 32-bit gpt output slot, fits
    up to `math.MaxUint32` and refuses `MaxUint32+1`); live: narrowing
    the 4294967316 total returns `ErrOverflow`.
  - Known two-node HTTP traffic matches before entry expiry:
    `TestLiveCounterTotals` compares every traffic total with the lab
    responder's received-request count (per node and summed) and with the
    oracle replaying the trace the live store accepted, at the total's
    own time; all before the entries' 30 s expiry.
  - Randomized: `TestRandomTraces` (40 seeds × 500 steps since the S2
    follow-up below; originally 400 steps: random,
    decreasing, and boundary counts, timed replays, partial/finished
    resyncs, reconnects, heartbeats, clock advances across health and
    expiry bounds, reads skipped on most steps) agrees with the oracle
    on sum, completeness, uncertainty, and counted values at every read.
    With the store's purge of an expired entry on update (below)
    disabled, it fails.
- Decisions or deviations:
  - New package `internal/aggregate`: `Count(store, table, key)` returns
    a `Total` (`Sum` uint64, `Complete`, `Uncertain`, and a
    `Contribution` per roster source with its state, presence, count,
    session, `Counted`, `HeldOver`, and decreases); `Total.Uint32` is the
    checked narrowing; `ErrOverflow`, `ErrUnknownTable`. The sum is
    recomputed from the store's current values on every call (the store
    stays authoritative); there is no running sum to adjust.
  - Completeness: only Ready sources' entries are summed (the carried-in
    phase 07 note); a syncing, degraded, or disconnected source's
    retained entry is reported but not counted. `Complete` is roster
    readiness at the same instant. Membership is the configured roster:
    every source is listed whatever its state, so a missing source keeps
    every total incomplete until it is Ready or removed from the
    configuration.
  - A Ready source's entry from an earlier session (not in the current
    session's complete snapshot; reconciliation is phase 11) is counted
    but marked `HeldOver`, which makes the total `Uncertain`.
  - `snapshot.Store.KeyView(table, key)` returns the roster and the
    key's entries under one lock at one clock reading, so completeness
    and values cannot come from different moments.
  - `snapshot.Entry` gains `Decreases`/`LastDecrease` (type `Decrease`)
    and `SourceReport` a `Decreases` counter. No new stick-table data
    types.
  - Fix in `snapshot.Store` found by the oracle: an update meeting an
    expired entry that no read had purged yet compared itself with (and
    carried state from) that invisible entry; it is now purged first
    (counted in `Expired`), so a recreated entry starts clean.
  - Lab traffic ground truth is the responder's per-node request count
    (`lab.Responder`), not HAProxy's tables.
  - Review fix (R1): `SourceReport.Decreases` counts only lowering
    updates the store keeps; a lower count that arrives with no lifetime
    left is an expiry (`Expired`), not a decrease. Regression in
    `TestDecreaseRecorded`, which fails without the fix.
  - Review follow-up (S1/S2, test-only): the oracle now models a
    `TableRejected` report (fault, table undefined) and a refused
    `SessionUp` (no effect), and a refused event of the current session
    still counts as read. `TestRandomTraces` (40 seeds × 500 steps,
    capacity 4) updates both tables and produces every refusal the store
    makes (`ErrSession` from duplicate `SessionUp` and earlier-session
    events, `ErrSchema`, `ErrNotInput`, `ErrUndefined`, `ErrCapacity`)
    plus `TableRejected`, and requires each, degraded sources with
    retained entries, complete-but-uncertain totals, and non-zero `t_in2`
    totals to occur. Mutations caught: counting Degraded sources in
    `aggregate.Count`; the old oracle's fault on a refused `SessionUp`;
    an oracle ignoring `TableRejected`. No store defect found.
  - A recreation at or above the stored count is indistinguishable from
    continuous counting and is not flagged (package documentation).
  - htad is unchanged: no consumer of totals until phase 10.
- Remaining work / next action: none for this phase's acceptance.
  Phase 09 evaluates rates per source (the count total is not an input
  to it); phase 10 should read completeness the same way (`KeyView` or
  `Roster`). Phase 11 owns held-over keys; until then they are counted
  and flagged uncertain.
