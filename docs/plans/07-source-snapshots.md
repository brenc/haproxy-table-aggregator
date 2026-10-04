# 07 — Source snapshots

Status: complete

Depends on: [06](06-freshness-gate.md).

## One-turn outcome

Maintain validated, expiring snapshots separately for each configured source,
and establish initial completeness without doing aggregate arithmetic yet.

## Work

- Key state by configured source, logical table, and canonical wire key. Store
  a replaceable session generation separately; it is never another contributor.
- Track each source through disconnected, syncing, ready, and degraded states.
  Request full synchronization from every required source on initial startup.
- Stage full snapshots and reconcile concurrent live updates without letting an
  older lesson overwrite newer accepted state. Commit completeness only when
  the appropriate controls and queued application work have been processed.
- Interpret timed updates using their remaining TTL and normal updates using
  the validated table definition. Store local deadlines with an injectable
  monotonic clock. Distinguish entry lifetime from rate-window age.
- Validate matching key types, lengths, periods, stored types, and arrays across
  contributors. Unknown output/metadata tables remain non-contributing.
- Acknowledge only accepted state. Basic capacities must already be finite;
  phase 14 qualifies resource exhaustion and retention at larger scale.

## Acceptance

- [x] Two sources reporting one key retain two independent snapshots.
- [x] The last accepted update replaces only its source's previous value.
- [x] Overlapping snapshot/live updates retain the newest valid source state.
- [x] One source completing sync cannot make the whole roster ready.
- [x] An empty but synchronized, healthy source is distinct from a missing one.
- [x] Expiration is deterministic under a fake clock and does not require input.
- [x] Schema mismatches degrade/reject the affected logical table explicitly.
- [x] No output or metadata replay is admitted into the snapshot store.

## Stop and handoff

Do not sum values yet. Record source/session identity, completion transitions,
and snapshot merge rules here. Reconnect reconciliation and process restart
qualification belong to phase 11, using this state model.

## Execution record

- Commands and versions (2026-10-04, Debian 13 amd64): Go `go1.27.1`
  (repo-pinned); stock HAProxy `3.4.6-56332c5` (binary SHA-256
  `81df335a…d1c4`) and `3.2.25-70469d3` (`5f89a725…8869`) from phase 01's
  `make haproxy` (not rebuilt).
  - `make check`: pass (0 lint issues, `go test -race ./...` ok, no
    vulnerabilities; log `artifacts/check-07.log`).
  - `make lab-test` (both versions, race detector on): pass, including
    the new `TestLiveSnapshots` and every phase 01–06 live test (log
    `artifacts/lab-test-07.log`). A later test-only variable rename
    (lint) was followed by `TestLiveSnapshots` alone on both versions:
    pass.
  - `make fuzz-smoke`: pass (log `artifacts/fuzz-smoke-07.log`).
- Acceptance evidence (independently reviewed: no defects).
  Unit tests in `internal/snapshot` use a fake monotonic clock that also
  stamps the events; `internal/sources/snapshot_test.go` drives the store
  through a real `sources.Manager` with scripted peers; the live test runs
  against both pinned builds.
  - Two sources, one key, two snapshots: `TestTwoSourcesOneKey`;
    `TestSnapshotThroughManager`; live `TestLiveSnapshots` (10 requests
    on node a and 20 on node b for `2001:db8:5::` give contributions
    a=10, b=20, one entry per source).
  - Last accepted update replaces only its source's value:
    `TestReplaceOwnSource` (a=10, b=20, a=11 gives a=11, b=20; a
    replayed identical update changes nothing); live: one more request
    on node a gives a=11, b=20.
  - Overlapping snapshot/live updates keep the newest valid state:
    `TestOverlappingSnapshotAndLive` (session 1 live values, then
    session 2's timed teach, a live update with a restarted ID, a
    partial reply, the retried teach, and "finished"; a late session-1
    event is refused with `ErrSession`; an update read before the stored
    one is ignored as stale; an invalid update is refused, keeping the
    newest valid value); live: after `Disconnect`, node a's resync
    re-delivers a=11 under session 2.
  - One source's sync cannot ready the roster:
    `TestOneSourceCannotReadyRoster` (ready, syncing, and disconnected
    sources; a partial reply does not complete; losing any source ends
    roster readiness); `TestSnapshotThroughManager` (a synchronized
    with b missing: roster not ready).
  - Empty synchronized source versus missing: `TestEmptySyncedVersusMissing`
    (Ready with zero entries versus Disconnected; a source that
    synchronizes without announcing the input table is Degraded, not an
    empty contributor); `TestSnapshotThroughManager` (empty b becomes
    Ready, completing the roster).
  - Deterministic expiry without input: `TestExpiryUnderFakeClock`
    (ordinary update: table expiry; timed: remaining lifetime, capped at
    the expiry; remaining 0 stores nothing and removes a stored value;
    an entry vanishes exactly at its deadline with no further event;
    `NextDeadline`, `Expire`; the rate counter's age does not shorten the
    lifetime). Health under the same clock: `TestHealthUnderFakeClock`
    (heartbeat reads keep a quiet source Ready; it is Degraded exactly
    at the health bound and Ready again on the next read). Live: both
    sources stayed Ready through a 7 s quiet period (health timeout 5 s)
    on heartbeats alone.
  - Schema mismatches degrade/reject the logical table explicitly:
    `TestSchemaMismatch` (period, missing field, extra array, key type:
    refused with `ErrSchema`, table marked rejected, source Degraded,
    its updates refused, the other source unaffected; the rejection
    survives reconnection until a later session completes a finished
    sync with the right schema; swapped values or another expiry in an
    update are refused; expiry may differ between sources);
    `TestSnapshotSchemaRejected` (the session's new `TableRejected`
    event reaches the store, which marks `t_in` rejected with
    `peersession.ErrSchema`).
  - No output or metadata replay admitted: `TestOutputNotAdmitted`
    (definitions and updates of `lab_out`, `lab_meta`, `prod_in` refused
    with `ErrNotInput`, nothing stored); `TestSnapshotOutputEchoNotAdmitted`
    (a scripted source replays `t_out` and `t_meta`: 2 echoed updates
    acknowledged by the session, 0 reach the store); live: after the
    reconnect node a replayed 2 output entries (`EchoedUpdates`), and the
    store holds one `lab_in` entry per source and nothing for the output
    tables.
  - Acknowledge only accepted state: `TestSnapshotRefusalNotAcked` (with
    capacity 1 the second key is refused: no acknowledgement, the session
    ends with `ErrEventRejected` wrapping `snapshot.ErrCapacity`, the
    event is not queued, the source degrades).
- Source and session identity: a contributor is a configured source name
  (also its HAProxy `localpeer`). `sources.Event.Session` numbers that
  source's sessions; the store keeps only the current number per source
  and the number that delivered each entry (`Entry.Session`), so a new
  session replaces, never adds, a contributor. Entries are keyed by
  source, configured input table name, and the 16-byte wire key
  (bytewise). Session-local table IDs play no part.
- Completion transitions (`snapshot.State`, evaluated at the store clock's
  now, in this precedence): Degraded while a fault is recorded (any
  refused event of the current session, or a `TableRejected` report;
  cleared only by a later session's finished resync) → Disconnected
  without a session → Degraded while unhealthy (now − last message read ≥
  `health_timeout`, from event times and `Observe` of
  `Stats.LastRx`, so heartbeats count and a backlog ages like silence) →
  Syncing until a "finished" resync reply (a "partial" reply keeps it
  syncing; the session asks again) → Degraded if synchronized without
  announcing a configured input table (HAProxy announces every shared
  table when it teaches, even empty) → Ready. A finished reply commits
  completeness only after every earlier event of the session was applied
  (Apply runs synchronously in wire order). `Roster.Ready` holds only
  when every configured source is Ready; phase 10 drives the lease from
  it (`Store.Roster`), which this phase does not wire.
- Snapshot merge rules: an accepted update replaces only its source's
  value for that table/key; wire order is recency within a session
  (HAProxy encodes current values when it sends), and a later session
  replaces an earlier one; events of any other session are refused;
  update IDs are never compared; an update read before the stored value
  is ignored (defensive). Lifetime: ordinary update = announced table
  expiry from reception; timed = remaining lifetime capped at the expiry
  (`peermsg.Update.Lifetime`); deadline = reception + lifetime on the
  monotonic clock; an update arriving with no lifetime left removes the
  value. Expired entries are invisible at once and purged by `Expire` or
  any report. Snapshots are applied in place rather than staged: the
  per-entry session tag and the source state are the staging boundary,
  and readers must gate on readiness.
- Decisions or deviations:
  - New package `internal/snapshot` (`Store`: `Apply`, `Observe`,
    `ObserveStatus`, `Expire`, `NextDeadline`, `Lookup`, `Contributions`,
    `Source`, `Roster`; `OptionsFrom(config)`). No arithmetic across
    sources.
  - `sources.Options.Apply`: the application sees every event
    synchronously in the session goroutine before it is queued and can
    refuse a table event, so an update is acknowledged only once the
    store accepted it (the queue alone accepted it before). SessionUp is
    applied once queued, SessionDown always.
  - Resync retry (`peersession.Options.ResyncRetry`,
    `sources.Options.ResyncRetry`, default `sources.DefaultResyncRetry`
    1 s): after a "partial" reply the session requests again until a
    "finished" one. Needed in practice: in `TestLiveSnapshots` and
    `TestLiveSessions` stock HAProxy answered the first request "partial"
    (both versions) while its own startup resync was unfinished, then
    "finished" on a retry. HAProxy sends "partial" whenever its peers
    section's resync state is not FINISHED (`peer_send_resync_finishedmsg`,
    `src/peers.c` of both releases). Existing live tests now wait for a
    finished reply per session (`resyncedSessions`) instead of counting
    one reply per session.
  - `peersession.TableRejected` reports an input table whose definition
    is rejected (session still ends with `ErrSchema`, as in phase 04);
    htad prints it as `table_rejected`.
  - `peersession.Stats.LastRx` now carries a monotonic reading
    (`monoBase` offset), so health ages are immune to wall-clock steps;
    `Stats.ResyncRequests` counts requests.
  - Config: `health_timeout` (default 5 s, 4–8 s: above HAProxy's 3 s idle
    heartbeat, and low enough for fallback within 10 s) and
    `max_source_entries` (default 131072, 1–4194304; a new key beyond it
    is refused and degrades the source). Phase 14 qualifies the bound.
  - htad applies events to a store and every 250 ms observes session
    liveness, expires entries, and logs source state changes and roster
    readiness (`TestRunEvents` waits for "roster ready").
  - `request_resync: false` is refused by config (owner decision after
    review): without a resync no source can ever become Ready.
- Handoff to later phases: keys absent from a later session's complete
  snapshot keep the earlier session's value until expiry (tagged with its
  session); reconciling them, and reconnect/restart qualification, are
  phase 11. Expiry and `NextDeadline` scan all entries (O(n)); phase 14
  should bound or index them. The resync retry is a fixed 1 s; each retry
  is a full teach, which phase 14 should measure at scale.
- Review handoff notes (non-blocking): `Contributions` and `Lookup` do
  not check source state, so phases 08 and 10 must gate on
  `Roster().Ready` or the source state. Heartbeats reach the store only
  through polled `Observe`/`ObserveStatus` (htad: every 250 ms), so phase
  10 should call `ObserveStatus` immediately before deriving a lease from
  `Roster()`. Stale keys from an earlier session count against
  `max_source_entries` until they expire (phases 11/14).
- Remaining work / next action: phase 08 sums `Store.Contributions`.
