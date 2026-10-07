# 10 — Publication and enforcement

Status: complete

Depends on: [09](09-rate-evaluation.md), using the scheme proven in
[06](06-freshness-gate.md).

## One-turn outcome

The complete one-table path receives real traffic, publishes evaluated rates,
and drives local HAProxy decisions without a per-request aggregator call.

## Work

- Connect the source store/evaluator to the ordinary output publisher. Coalesce
  superseded output updates per key without losing required zero/expiry effects.
- Maintain separate bounded destination queues, send-time encodings, update IDs,
  and progress. Keep heartbeats and recovery controls responsive under load.
- Apply the phase-06 freshness design to real results. Never renew authority
  while source completeness or destination data progress violates the contract.
- Enforce the local limit at all times and the global limit when authoritative.
  Both use the full configured threshold; fallback does not divide by node count.
- Produce complete validated lab/example configurations: isolated raw inputs,
  separately named aggregate outputs, metadata, consistent keys, and ACLs.
- Separate missing aggregate entries from incomplete publication. A missing key
  may be zero only when the proven readiness scheme permits that interpretation.
- Carried in from phase 07: drive `output.Store.SetLease`/`Revoke` from
  `snapshot.Store.Roster()`. Heartbeats reach the store only through polled
  `Observe`/`ObserveStatus` (htad polls every 250 ms), so call
  `ObserveStatus` immediately before deriving a lease from `Roster()`.
- Carried in from phase 08: publish nothing as authoritative unless roster
  readiness, read with the values, certifies every required source (as
  `aggregate.Total.Complete` does for counts). `Uncertain` (a count
  decrease or a held-over prior-session entry) is not an error, but keep
  it visible in diagnostics rather than dropping it.
- Carried in from phase 09: publish only `aggregate.RateTotal.Authoritative()`
  values; an error, including `ErrOverflow`, means no certified value.
  `output.RateValue` still saturates (phase 05 called that safe because it
  only restricts); reconcile the two, and never use a saturated value to
  certify authority. Reevaluate keys at `Cadence.Due` and also on source
  state changes, which `RateTotal.Next` does not include.

## Acceptance

- [x] Each node stays below a chosen local threshold while the combined rate
      crosses it; both nodes return 429 after measured propagation.
- [x] A single node exceeding its local limit is protected even before a global
      update arrives and while global data is unavailable.
- [x] Stopping traffic eventually removes the aggregate denial through decay.
- [x] No aggregate value is counted back into the input or incremented locally.
- [x] A missing/unready aggregate selects local protection with the full limit.
- [x] One slow destination does not stall a healthy destination or controls.
- [x] End-to-end timings include HAProxy observation, not just Go queue insertion.

## Stop and handoff

This is the first functional end-to-end milestone, not a deployment approval.
Record overshoot and convergence measurements; do not claim a strict global
quota. Keep the configurations alongside the implementation and cite them here.

## Execution record

- Commands and versions (2026-10-05, Debian 13 amd64): Go `go1.27.1`
  (repo-pinned); stock HAProxy `3.4.6-56332c5` (binary SHA-256
  `81df335a…d1c4`) and `3.2.25-70469d3` (`5f89a725…8869`) from phase 01's
  `make haproxy` (not rebuilt).
  - `make check`: pass (0 lint issues, race tests ok, no
    vulnerabilities; log `artifacts/check-10.log`).
  - `make lab-test` (both versions, race detector on): first run
    `artifacts/lab-test-10.log` passed everything on 3.4.6 and every new
    test on 3.2.25, but failed the phase 08 test `TestLiveCounterTotals`
    on 3.2.25 (see Decisions: a timing assumption in that test, fixed);
    final run on the final tree `artifacts/lab-test-10-r2.log`: pass on
    both versions (every package ok, exit 0).
  - `make lab-smoke HAPROXY_BIN=...` with each pinned build: `smoke ok`
    on both (`artifacts/lab-smoke-10.log`); the lab without an aggregator
    is unchanged.
  - `go test -fuzz '^FuzzRun$' -fuzztime 20s ./internal/peersession`:
    about 258k executions, pass (`artifacts/p10/fuzzrun.log`).
- Configurations (kept with the implementation):
  - `examples/two-node/haproxy-a.cfg`, `haproxy-b.cfg`, `htad.json`: a
    complete two-proxy example: isolated raw input `req_in` (the only
    tracked table, peered only with the aggregator), aggregate output
    `req_agg` and metadata `req_meta` (gpt(4), never tracked), one key
    expression `src,ipmask(32,64)` for tracking and every lookup, the
    local limit always and the same full limit on the aggregate under the
    frozen phase 06 authority rule. `TestExampleConfigs`
    (`internal/lab/example_test.go`) loads `htad.json`, checks that the
    HAProxy files declare its tables and addresses and track only the
    input, that their decision lines are exactly the lab's enforcement
    (normalized names, limit 100), and runs `haproxy -c -V` with
    zero-warning on both versions under `make lab-test`.
  - The lab (`internal/lab/config.go`, template `enforce`): with
    `lab.Aggregator.Limit` the lab listener enforces the limit locally
    on `sc_http_req_rate(0)` always, and on `lab_out` while authoritative
    (local rule first), and reports the decision in response headers
    (`X-Lab-Authority`, `X-Lab-Deny`, `X-Lab-Local-Rate`,
    `X-Lab-Agg-Rate`). `TestConfigValidates` now also checks the
    enforcing variant; `TestRenderEnforcement` checks it contains the
    frozen expressions. `htalab -aggregator ... -limit N` runs it by hand,
    and `-htad-config` writes the aggregate output's `input`.
- Acceptance evidence. All live tests are in `cmd/htad/live_enforce_test.go`
  and run the daemon exactly as htad does (`runWith`: snapshot store,
  publisher, output store, sessions) against a two-node lab whose lab
  listeners enforce 100 requests per 10 s period; the daemon dials both
  nodes. Numbers are single-run figures from `artifacts/lab-test-10.log`,
  3.4.6 / 3.2.25; timings vary between runs (an independent review run
  saw a propagation p99 of 28.4 ms on 3.4.6), so only the asserted bounds
  are claimed.
  - Each node below the local threshold while the combined rate crosses
    it; both return 429 after measured propagation: `TestLiveEnforcement`
    sends 6 requests/s to each node for one key for 20 s (about 60 per
    period each). No request was denied locally and no local rate reached
    100. The sum of the two nodes' own `http_req_rate` readings reached 100
    at +8.17 s; the next request on each node (166 ms later, the request
    interval) was the first 429, denied by the aggregate under authority
    (`X-Lab-Deny: aggregate`), and every later request was 429 on both
    nodes. Overshoot: 1 request admitted after the crossing, 100 admitted
    in total (50 per node), aggregate 100 / 101 at node a's first denial.
    Convergence is bounded here by the request interval; the propagation
    itself is measured below.
  - A single node over its local limit is protected before a global update
    arrives and while global data is unavailable:
    `TestLiveLocalProtection`. Under authority, a burst on node a was
    denied at exactly its 100th request by the local rule, while the
    aggregate it read was 98 / 97 (97 / 99 of the 100 responses saw the
    aggregate behind the local rate). With the roster unready (node b
    disconnected and kept from reconnecting) and with the daemon stopped,
    bursts were denied at exactly the 100th request, locally. (Before
    review batch 1 the "daemon gone" figures, 0.96 s / 1.00 s, measured
    marker expiry; shutdown now delivers the revocation, see Review
    batch 1.)
  - Stopping traffic removes the aggregate denial through decay:
    `TestLiveEnforcement` stops sending, then probes both nodes without
    tracking: the aggregate fell to 94 / 95 (below 100 with a margin)
    2.17 s / 2.07 s after the last request, with the entry still present
    (version 2) and authoritative; one more request on each node was then
    200 under aggregate authority.
  - No aggregate value is counted back into the input or incremented
    locally: in `TestLiveEnforcement` each node's `lab_in` count equals
    the requests sent to it (denials included: tracking precedes the
    decision), the daemon's snapshot holds the same counts per source, the
    responder saw exactly the admitted requests, each node's `lab_out`
    entry holds only gpt values, `show peers` shows `remote_id=0` and
    `last_get=0` for `lab_in` on both nodes (the daemon never announced
    or updated an input table), and the published aggregate equals the
    sum of the nodes' own rates (96 = 48 + 48; 97 = 49 + 48), not twice
    it. Configuration: the lab and the examples track only the input
    (`TestRenderAggregatorPeers`, `TestExampleConfigs`).
  - A missing or unready aggregate selects local protection with the full
    limit: `TestLiveLocalProtection` "roster unready": the revocation was
    visible on node a 8 ms / 10 ms after node b's session was cut; then 8
    requests/s on each node for 13 s (peak local rates 81 and 81, sum 162,
    above 100) were all 200 under local authority: no aggregate denial and
    no divided limit; a burst was denied at exactly 100. "Daemon gone"
    likewise. A key never published reads `local` (the probe in every
    test that waits for authority, and `TestLiveFreshnessLease`).
  - One slow destination does not stall a healthy destination or
    controls: `TestLiveSlowDestination` holds every write to node b for
    3.5 s (honoring write deadlines, as a full socket buffer would) while
    node b keeps receiving traffic. Node a stayed authoritative in every
    probe (156 / 154), its propagation from node b's traffic was
    unchanged (p99 7.1 ms / 6.4 ms during the stall, 5.9 / 6.0 ms
    before), node b's input was still read (its update was in the
    snapshot 1 ms after the stall began) so the roster stayed ready
    throughout, and node b alone fell back to local protection when its
    last marker ran out (867 ms / 860 ms after the stall began). After
    the stall node b was authoritative again within 2 ms on the same
    session. The session queue held 3.8 KB during the stall. Under bulk
    output, `TestOutputBacklog` (`internal/sources`, fake source that
    stops reading, 2000 keys republished every round): the session kept
    accepting input, deferred output once the queue passed 256 KiB, its
    queue stayed at about 264–288 KB over 15–16 rounds, and once the
    source read again it received each key's latest value (about 12 000
    updates for 30 000 published) followed by a marker, on the same
    session.
  - End-to-end timings include HAProxy observation: `TestLivePropagation`
    sends one request for a fresh key to one node and probes the other
    node's HAProxy until its own ACLs read the new aggregate as
    authoritative (rate at least 1): from the request's start, p50 5.1 ms,
    p90 5.7–5.8 ms, p99 7.2 ms / 6.7 ms, max 7.7 ms / 7.4 ms over 200
    keys; from the response, p99 6.0 / 5.6 ms; to the same node p99
    6.5 / 5.7 ms. Well within the 250 ms p99 starting target in this lab
    (loopback, light load; not a load qualification, phase 18).
- Design and decisions:
  - New package `internal/publish` (`Publisher`): evaluates each key's
    `aggregate.Rate` and writes only `RateTotal.Authoritative()` values.
    A key is reevaluated when its source update is applied
    (`Publisher.Apply`, which wraps `snapshot.Store.Apply` as
    `sources.Options.Apply`), at `aggregate.Cadence.Due`, and (every key)
    after any other session event or roster state change (detected by a
    per-source state/session signature at each pass). Each pass reads
    the wall clock W, calls `ObserveStatus(Manager.Status())`, then
    requires `Roster().Ready`; the lease is renewed every 250 ms (lease/4,
    lease 1 s = `output.MaxLeaseLength`) as `SetLease(W + 1 s)` after the
    pass evaluated every dirty and due key, so it certifies values current
    at W. A roster that is not ready, or a key view that turns out
    incomplete mid-pass, writes no value and revokes once; the next ready
    pass reevaluates every key before leasing again. Passes triggered by
    input are coalesced (at most one per 5 ms).
  - Overflow (phase 09 handoff; owner decision 2026-10-07): a complete
    rate above 2^32-1 is published as 2^32-1 under the lease and counted
    (`Stats.Overflows`, `Saturated`); it is published exactly again once
    it fits. This deliberately overrides the phase 09 handoff's "never
    use a saturated value to certify authority": the slot only feeds
    limit checks, and any limit below 2^32-1 decides the same for the
    bound as for the true sum, so every proxy keeps denying the key
    (fail closed). Withholding the key instead, as first implemented,
    would make every other proxy read it as local and admit it below its
    own local limit. The cost is that the slot is a lower bound, not a
    count, while saturated. `output.RateValue` stays removed; package
    publish owns the narrowing. Covered by `TestOverflowSaturated`.
  - Missing versus incomplete: a missing key reads local, which decides
    exactly as a zero aggregate would (the aggregate can only add
    denials), so no missing key is ever interpreted as zero; incomplete
    publication is a revoked or expired lease. Zero-rate keys that need
    no further evaluation are retired in batches every 10 s
    (`RetireInterval`), the phase 06 retirement hand-off.
  - `Uncertain` totals are published and counted (`Stats.Uncertain`).
  - Destination queues: each session now sends through its own ordered
    byte queue drained by a writer goroutine (`peersession/writer.go`),
    so a source slow to read never stops the session reading its input,
    acknowledging, or answering controls. Above `SoftBacklog` (256 KiB)
    queued, output changes, refreshes, and markers are deferred (the
    output store coalesces them per key; the writer wakes the session once
    drained); above `HardBacklog` (64 MiB) the session ends with
    `ErrLimit`; each write still waits at most the idle timeout. Update
    IDs, generations, and progress (`outSeq`, `taughtSeq`) stay per
    session as before. New stats: `OutQueued`, `MaxOutQueued`,
    `OutDeferred`.
  - Config: an aggregate output names its input (`"input": "lab_in"`),
    required, a configured input, at most one aggregate per input;
    metadata outputs take none.
  - htad now publishes: it builds the output store and publisher, passes
    `Publisher.Apply` to the sessions, runs the publisher, and on
    shutdown revokes the lease and waits at most 500 ms
    (`revocationDrain`) until no established session may still leave its
    source holding a live marker (`Stats.LiveMarker`, see Review batch
    3) before closing sessions; a source
    it misses falls back when its last marker expires (logged). Tests
    reach it
    through `runWith` hooks (dialer, listener, publisher options).
  - `snapshot.Store.Keys(table)` lists keys with any unexpired entry.
  - Deviation (test only): phase 08's `TestLiveCounterTotals` assumed
    node b's two keys' deadlines were more than 100 ms apart. When both
    nodes answer the first resync "finished" (no 1 s retry), they fall
    within 25 ms and both expire, failing "expired 2 entries, want 1"
    (2 of 3 runs on 3.2.25 on this tree; the same timeline is possible
    at baseline, where it passed 3 of 3). The test now sends one more
    request on node b for the other key first; 4 of 4 reruns pass.
- Review batch 1 (R1, Low): shutdown revocation was a race: `Revoke` only
  changed the store and `Manager.Close` cancelled sessions at once (their
  writers stop with no drain on cancellation), so delivery depended on
  scheduling (a reviewer run saw 1 ms on 3.4.6 but 968 ms, marker expiry,
  on 3.2.25). Fixed as above: htad compares each session's
  `Stats.Revocations` before and after stopping the publisher (which now
  counts its shutdown revocation) and waits, bounded, until every
  session that was up has written one and has `OutQueued == 0`.
  `TestLiveLocalProtection` "daemon gone" now asserts that both nodes read
  the revocation marker (version 2, generation 0) within 100 ms of
  `runWith` returning and that no "revocation not written" warning was
  logged: 3 runs per version, read 3–8 ms after shutdown began (1–5 ms
  after it returned) on both nodes.
- Review batch 2 (C1, Low): the batch 1 wait also covered sessions that
  can never write a revocation (no metadata table, or output not yet
  fully taught, so no marker was ever written), so such a shutdown waited
  the full 500 ms and logged a false warning. Sessions now report
  `Stats.LiveMarker` (the last marker written was a live lease), and
  shutdown waits only for established sessions with a live marker, and
  only while each keeps the same session number. Coverage:
  `TestRevocationWait` (`cmd/htad`, selection and completion) and
  `TestLiveMarkerStat` (`internal/sources`: false before any marker, true
  after a lease, false after a revocation, never true without a metadata
  table). The live "daemon gone" assertion still passes on both versions
  (revocation read 3–5 ms after shutdown began; `make check` log
  `artifacts/check-10-b2.log`, live log `artifacts/p10/htad-live-b2.log`).
- Review batch 3 (C2 Low, C3 Medium; reviewer suggestion S4):
  - C2/S4: `LiveMarker` was cleared when a revocation was queued, not
    written, and shutdown captured sessions before stopping the publisher,
    so a revocation queued behind pending writes, or a live marker written
    after the capture, could be cut off by `Manager.Close`. Now each
    session counts live markers from just before one is queued, and the
    send queue records the byte position at which each revocation ends;
    the writer marks the live markers it covers withdrawn only once those
    bytes are written. `Stats.LiveMarker` is "a live marker may be
    outstanding". Shutdown no longer pre-captures: after stopping the
    publisher it re-reads every session until none that is up has
    `LiveMarker` (at most 500 ms), so a session that never wrote a live
    marker, or has no metadata table, is still not waited for (C1).
    Coverage: `TestLiveMarkerAccounting` (`peersession`, deterministic:
    counted from before queueing, still live with the revocation queued or
    partly written, withdrawn once fully written, live again with a newer
    marker), `TestLiveMarkerStat` "revocation behind a backlog"
    (`internal/sources`: a source that stops reading keeps `LiveMarker`
    true after `Revoke` until it reads the revocation), and
    `TestRevocationWait` (`cmd/htad`).
  - C3: a lost wake: when the writer drained a backlog between Run's
    backlog check and its consuming of the output wake, Run kept the stale
    "blocked" decision and the deferred output and marker waited for the
    next read (up to a heartbeat interval). `deferOutput` now re-reads
    the writer's disarm flag after consuming the wake and resumes output
    at once. `TestDrainWakeNotLost` (`peersession`) replays that
    interleaving deterministically.
  - Checks: `make check` pass (`artifacts/check-10-b3.log`); peersession
    tests (3 runs) and the `internal/sources` output tests including
    `TestOutputBacklog` (3 runs) under `-race`; the htad live suite on
    both versions (`artifacts/p10/htad-live-b3.log`; revocation read
    3–5 ms after shutdown began); full `make lab-test` on the final tree:
    pass on both versions, exit 0 (`artifacts/lab-test-10-b3.log`).
- Limitations: one table and one category; nbthread 1; loopback lab with
  light load. Evaluation and due scheduling scan every key per pass
  (O(keys)); the output and queue bounds are not qualified at scale
  (phase 14). Convergence in `TestLiveEnforcement` is bounded by its
  request interval; the propagation measurement is separate. The slow
  destination is simulated by holding the daemon's writes (HAProxy's own
  peer timeout limits a stall to a few seconds); a destination stalled
  past the idle timeout loses its session. A source whose session cannot
  read its input still ages out of readiness after the health bound and
  revokes for everyone, as the contract requires.
- Review batch 4 (owner-authorized extra round; R2 Low, R3 Low, C4
  Low–Medium):
  - R2: a session mid-`publish()` that had read a live lease before
    `Revoke` counted its live marker only in `writeLease`, after queueing
    the values, so shutdown could see `LiveMarker` false in between and
    cut off the revocation. `publish` now sets a pending flag as soon as
    it reads a valid lease (cleared when it returns, after `writeLease`
    has counted any marker), and `LiveMarker` includes it.
    `TestLiveMarkerPendingDuringPublish` (`peersession`) reads the stat
    from a test hook between the values and the marker: true (false
    without the fix).
  - R3: `noteRevocation` recorded the revocation's end after `flushOut`
    had queued it, so a writer that had already written those bytes
    never withdrew the marker (`LiveMarker` stuck true, a 500 ms wait and
    a false warning). It now also compares the boundary with the bytes
    already written and withdraws at once; the boundary is set under the
    queue lock and the writer adds its count before reading it, so one of
    the two always sees the other. `TestRevocationWrittenFirst`
    (`peersession`) writes the bytes before the boundary is recorded
    (fails without the fix).
  - C4: `retireIdle` retired key by key, and each removal was its own
    store change, so a session could rotate and re-send its whole output
    several times in one retirement pass. New
    `output.Store.RetireBatch` removes every listed key of every table as
    one change (one notification, validated before removing anything);
    `Retire` is a one-key batch; `retireIdle` makes one batch per pass.
    The overflow withdrawal still retires its single key at once.
    `TestRetireBatch` (`output`: one notification for four keys, nothing
    removed on an invalid table, absent keys wake nothing),
    `TestOutputRetireBatch` (`sources`: 49 keys retired in one batch cost
    the session one rotation and one re-send), `TestRetireIdleBatch`
    (`publish`).
  - Checks: `make check` pass (`artifacts/check-10-b4.log`); peersession,
    sources, publish, and output tests under `-race`, 3 runs each, pass
    (`artifacts/p10/race-b4.log`); full `make lab-test` on the final tree,
    htad live suite included: pass on both versions, exit 0
    (`artifacts/lab-test-10-b4.log`; "daemon gone" revocation read 3–5 ms
    after shutdown began).
- Remaining work / next action: independent review, three fix batches,
  and one extra batch authorized by the owner are done; every acceptance
  item has live evidence on both pinned builds. The three findings that
  were open at the review cap (R2, R3, C4) are fixed in Review batch 4
  (above) and verified by the reviewer; a final cross-model pass raised
  no surviving defect. Next: phase 11.
