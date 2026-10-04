# 04 — Peer sessions

Status: complete

Depends on: [03](03-table-messages.md).

## One-turn outcome

A minimal Go daemon establishes ordinary peers-protocol sessions with the two
lab proxies, exchanges controls, and exposes validated table events.

## Work

- Add a small CLI and strict configuration for local identity, listen address,
  explicit source names/addresses, and table mapping. Use a narrow documented
  configuration surface; environment discovery is outside scope.
- Implement handshake/version negotiation for the selected protocol, both
  connection directions, heartbeats, timeouts, acknowledgment events, and the
  required resynchronization controls. Track protocol states explicitly.
- Resolve duplicate/colliding connections deterministically so one logical
  source is never treated as two contributors. Bound reconnection backoff.
- Separate wire/session handling from source storage. Do not acknowledge an
  update that was rejected by the bounded application event path.
- Permit plaintext only through an explicit isolated-lab setting. No supported
  non-loopback deployment until phase 13 adds mutual TLS.
- Handle cancellation and SIGTERM without leaking goroutines or sockets.

## Acceptance

- [x] Two stock HAProxy instances establish and retain sessions while idle.
- [x] Handshakes reject unexpected identities and unsupported protocol versions.
- [x] Heartbeat-only traffic keeps a quiet source live.
- [x] Both connection directions and simultaneous connection attempts work.
- [x] Closing/reopening a connection produces one logical source identity.
- [x] Unknown controls or malformed frames fail the affected session cleanly.
- [x] Shutdown and a bounded race-detector integration run pass.

## Stop and handoff

No aggregation, persistent state, or production listener. Session control may
request/observe synchronization, but phase 07 owns snapshot completeness.

### Session interface for later phases

- `cmd/htad -config FILE`: the daemon. It writes one JSON object per
  event to stdout and logs to stderr; SIGINT/SIGTERM closes every session
  and exits 0 (1 if stdout stayed blocked for 5 s; a second signal ends
  it at once). `htalab -aggregator NAME@ADDR -htad-config FILE` runs the
  two-node lab with a peers section per node (the node and the aggregator
  only, attached to all three tables) and writes a matching configuration.
- `internal/config`: strict JSON (unknown fields, trailing data, and
  oversized files fail). `local_peer`; optional `listen`; `sources` (1 to
  64; `name` is both the logical source name and the HAProxy `localpeer`
  it must present; optional `address` the daemon dials); `tables` (input
  tables: `name` and required `period`); timing (`heartbeat` default 3s,
  at most 4s; `idle_timeout` default 10s, at least 4s; `handshake_timeout`
  5s; `reconnect_min`/`reconnect_max` 100ms/5s; `event_timeout` 1s, below
  `heartbeat`); `event_queue` 1024; `max_session_tables` 32;
  `request_resync` true. Duplicate keys and keys differing only in case
  are rejected, and `max_session_tables` must be at least the number of
  input tables. It is accepted only with
  `"insecure_plaintext_loopback_lab": true` and loopback IP literals for
  every address; phase 13 replaces this with mutual TLS.
- `internal/sources.Manager` (`Start`, `Events`, `Status`, `Disconnect`,
  `Close`): one bounded queue of `Event{Source, Session, Body}`. Per
  source, `Session` increases with every session, and all events of a
  session (`SessionUp` ... `SessionDown`) precede the next session's
  `SessionUp`, so a consumer never sees two concurrent sessions of one
  source. Bodies are `peersession.SessionUp`, `SessionDown`,
  `TableDefined` (an input table accepted, once per binding),
  `EntryUpdated` (the decoded `peermsg.Update` with reception time, plus
  the table expiry for `Lifetime`), and `SyncFinished` (the source's
  "finished"/"partial" reply to the resync request each session sends).
- Acknowledgement contract: an update is acknowledged only after the
  queue accepted its event; a table event that finds the queue full for
  `event_timeout` ends the session unacknowledged (`ErrEventRejected`
  wrapping `sources.ErrQueueFull`), and the resync of the next session
  replays the source's table. Acks are cumulative per table and sent
  after each read.
- `internal/peersession.Conn` is the single-connection state machine
  (phases hello/status/established/closed, learn state, pending
  confirms); failures wrap one of `ErrHandshake` (`*HandshakeError` with
  the status code), `ErrProtocol`, `ErrSchema`, `ErrLimit`, `ErrIdle`,
  `ErrClosed`, `ErrPeerError`, `ErrEventRejected`, `ErrIO`, or the
  context's error.
- For phase 05: output tables will appear in the same peers sections.
  Today every table not configured as input is ignored (never decoded,
  never acknowledged) and HAProxy's resync requests are answered
  "finished" at once because this side announces no tables; phase 05
  must revisit both when it announces output tables. For phase 07: the
  resync reply arrives as `SyncFinished`; HAProxy answered "partial"
  while it had not finished its own startup resync (observed live), and
  every new session replays the table (see phase 03).

## Execution record

- Commands and versions (2026-10-03, Debian 13 amd64): Go `go1.27.1`
  (repo-pinned); stock HAProxy `3.4.6-56332c5` (binary SHA-256
  `81df335a…d1c4`) and `3.2.25-70469d3` (`5f89a725…8869`) from phase 01's
  `make haproxy` (not rebuilt).
  - `make check`: pass (0 lint issues, `go test -race ./...` ok, no
    vulnerabilities; log `artifacts/check-04.log`).
  - `make lab-test` (now `./internal/... ./cmd/...`, race detector on,
    `-timeout 15m`): both versions pass, including every phase 01–03
    live test and the new `TestLiveSessions`, `TestLiveSimultaneous`,
    `TestLiveHandshakeRejects`, `TestLiveFaultIsolation`,
    `TestLiveAbandonedAttempt` (`internal/sources`), and
    `TestLiveSIGTERM` (`cmd/htad`); 1 min 44 s for both versions (log
    `artifacts/lab-test-04.log`).
  - `make fuzz-smoke` (10 s per target): the new `FuzzRun` (about 129k
    executions) and the six earlier targets pass (log
    `artifacts/fuzz-smoke-04.log`); a separate 20 s `FuzzRun` reached
    about 313k executions.
  - Manual: `htalab -aggregator agg@127.0.0.1:PORT -htad-config F` with
    each pinned build, then `htad -config F`, three requests to node a's
    lab listener, SIGTERM to htad (exit 0) and SIGINT to htalab (exit 0):
    both sessions up, three `entry_updated` lines with
    `http_req_cnt` 1, 2, 3 for `2001:db8:77::`.
- Upstream sources (read to confirm facts, nothing copied or translated):
  `src/peers.c` and `include/haproxy/peers-t.h` of both verified release
  tarballs (session states, hello checks, collision handling,
  `peer_session_forceshutdown`, `peer_session_release`, the sync task's
  heartbeat/reconnect logic, resync controls), and the 3.4.6 `peers`
  section of `doc/configuration.txt`. The session state machine and
  collision handling are identical in both releases apart from tracing
  and buffer APIs.
- Acceptance evidence (boxes left for the orchestrator after review):
  - Two stock instances establish and retain sessions while idle:
    `TestLiveSessions` on both versions. Node a dials the daemon
    (inbound), the daemon dials node b (outbound); both reach `ESTA` in
    `show peers`, the daemon's resync request is answered (both
    "finished" and "partial" observed), and only `lab_in` produces
    events (one `TableDefined` per source; `prod_in` traffic is received
    and skipped, `SkippedUpdates > 0`). 10 and 20 requests arrive as
    updates with `http_req_cnt` 10 and 20, and HAProxy's per-peer cursor
    for `lab_in` shows `update=` equal to the last update ID the daemon
    accepted (10 and 20), so acknowledgements land. Both sessions then
    stay up through a 12 s quiet period (three idle timeouts).
  - Handshakes reject unexpected identities and unsupported versions:
    `TestHandshakeRejections` (no HAProxy) answers 501 (not a peers
    hello, malformed sender line, oversized line), 502 (2.0, 2.2, 3.1,
    unparsable), 503 (addressed to another peer), and 504 (unknown
    sender, or the daemon's own name), each followed by a close and no
    events; the status follows the first bad line at once;
    `TestHandshakeTimeout` closes an incomplete hello after the handshake
    timeout. Live (`TestLiveHandshakeRejects`, both versions): the
    daemon's 504 and 503 are recorded by HAProxy as `last_status=UNKN`
    and `NAME`, and HAProxy's 503 (wrong peer name in the daemon's hello)
    and 504 (unknown local name) are recorded by the daemon as
    `HandshakeError` codes; no events. `TestOutboundStatus` covers a
    refused outbound hello followed by a successful retry.
  - Heartbeat-only traffic keeps a quiet source live: in
    `TestLiveSessions`, with `idle_timeout` 4 s, both sessions survive
    12 s with no table traffic; the daemon received and sent 4 heartbeats
    per session, every message received in that window was a heartbeat,
    and HAProxy's `rx_hbt`/`tx_hbt` rose with `no_hbt=0`.
    `TestHeartbeatsAndIdle` (fake peers): a heartbeating source stays up
    for four idle timeouts while a silent one ends with `ErrIdle`.
  - Both directions and simultaneous attempts: both directions in
    `TestLiveSessions`. `TestLiveSimultaneous` (both versions) holds
    node a's connection, accepted first, while the daemon's outbound
    hello is accepted (HAProxy `coll=1`); the held connection is then
    refused without a status line and the outbound session is the only
    one. Five close-and-reopen cycles with both sides redialing each
    converge to one session that HAProxy shows `ESTA` (typically an
    inbound session replaced by the daemon's outbound one).
    `TestLiveAbandonedAttempt` (both versions; 30 consecutive passes
    on the final code) reproduces HAProxy delivering a
    hello from an attempt it had already abandoned; it is refused and the
    outbound session stays up. `TestStaleInbound` and `TestReplacement`
    cover the rules with fake peers.
  - Closing and reopening gives one logical source: in
    `TestLiveSessions` both sessions are closed (`Manager.Disconnect`);
    node a redials and the daemon redials node b; each source has
    exactly sessions 1 and 2 with one `SessionDown` between, the resync
    replays the counts, and new traffic is attributed to session 2.
    `checkLifecycle` asserts on every manager test that per source the
    session numbers increase and no two sessions are ever up at once.
  - Unknown controls or malformed frames fail the affected session
    cleanly: `TestProtocolFailures` (12 cases: unknown control, unknown
    class, unknown stick-table type, reserved class, 6-byte length,
    oversized body, malformed and trailing-byte definitions, update with
    no table, switch to an unknown table, ack for an unannounced table,
    unsolicited confirm) each end with exactly one error message (`01 00`,
    or `01 01` for the oversized body) and a close, `ErrProtocol`, while
    another source's session continues. `TestResyncControls`,
    `TestPeerErrorMessage`, `TestSchemaMismatch` (period, fields, output
    form, unsupported key type: closed without an error message,
    `ErrSchema`), and `TestQueueFullNoAck` (no acknowledgement for an
    update the queue refused) complete the set. Live
    (`TestLiveFaultIsolation`, both versions): scripted sources sending
    `00 09` and a malformed definition next to node a are closed after
    `01 00`; node a's session continues (`proto_err=0`) and delivers new
    updates. `FuzzRun` checks that arbitrary input after a handshake
    always ends `Run` with a classified error and never yields an
    acknowledgement for an update the sink did not accept.
  - Shutdown and a bounded race-detector integration run:
    `TestLiveSIGTERM` (both versions) runs htad as a process with one
    session in each direction; SIGTERM yields both `session_down` lines
    and exit 0 in about 1.0 s under `-race` (3 ms with
    `GORACE=atexit_sleep_ms=0`: the race runtime sleeps 1 s at exit),
    and both HAProxy nodes leave `ESTA`. `TestCloseReleasesEverything`
    closes a manager with an inbound, an outbound, and a half-finished
    connection: Close returns in well under 1 s, every peer sees the
    close, the listener refuses connections, and the goroutine count
    returns to its baseline. The bounded race run is `make lab-test`
    above (`-race`, `-timeout 15m`, both versions).
- Decisions or deviations:
  - Packages: `internal/config`, `internal/peersession` (one connection,
    no I/O beyond it), `internal/sources` (listener, dialers,
    arbitration, queue), `cmd/htad`. Wire and session handling are
    separate from any storage; the daemon stores nothing.
  - Collision rule (deviates from plain "newest wins"): an outbound
    session HAProxy accepted always becomes current; an inbound session
    replaces an earlier inbound one but never a current outbound one,
    and such an inbound connection is closed without a status line.
    The first design compared accept times; a live run showed HAProxy
    delivering a hello from an attempt it had abandoned about 0.9 s
    after the outbound session started, which displaced a live
    session. Inferred, not verified (see the upstream draft): the
    abandoned attempt was already past `CONNECT` (status `HSHK` at the
    collision), so `peer_session_forceshutdown` did not skip it; its
    stream presumably kept retrying the refused connect and flushed the
    queued hello when it connected. Closing without a
    status keeps HAProxy retrying (it does not redial after a 300 or
    50x status).
  - HAProxy upstream behavior found (both versions, source plus live):
    `peer_session_release` clears the peer's `PEER_F_ALIVE` flag for any
    released connection to that peer, including an abandoned attempt,
    and after a collision HAProxy rechecks liveness within 50–2050 ms,
    closing the current session as dead if no message arrived since. In
    `TestLiveAbandonedAttempt` this closed the daemon's live session in
    16 of 20 runs (8 per version) with the mitigation disabled. Mitigation: the daemon sends extra heartbeats
    10 ms and 250 ms after each session starts and after each refused
    connection (`Conn.Nudge`); 30 of 30 runs (15 per version) then
    passed on the final code. A residual window remains (a liveness check falling between the release and the
    first extra heartbeat); its effect is one reconnect. Draft upstream
    report with a stock-only reproduction:
    [`docs/upstream/peers-alive-flag-release.md`](../upstream/peers-alive-flag-release.md).
  - Protocol version: exactly 2.1. HAProxy also accepts 2.0 (downgrade,
    no timed updates); the daemon answers 502, since lifetimes need timed
    updates and stock HAProxy announces 2.1.
  - Fail closed: unknown classes and types, which HAProxy skips, end the
    session with a protocol error; so do unsolicited resync replies and
    confirms, and acks (this side announces no tables). An input table
    with an unsupported or different schema ends the session without an
    error message (phase 03 left this choice to phase 04); a table not
    configured as input is ignored whatever its schema, as HAProxy
    ignores unshared tables.
  - Resync: each session requests one resync and confirms the reply;
    the source's requests are answered "finished" at once (nothing to
    teach until phase 05 announces tables).
  - Timing defaults follow HAProxy: 3 s heartbeat (HAProxy kills a peer
    silent for a whole 5 s window), 10 s idle timeout (HAProxy heartbeats
    every 3 s), bounded jittered reconnect backoff (100 ms to 5 s,
    uniform in [d/2, d], `crypto/rand`), at most 64 connections in
    handshake. `SessionUp`/`SessionDown` wait for queue room until the
    manager closes, then at most `event_timeout`.
  - Plaintext: only with `insecure_plaintext_loopback_lab` and loopback
    IP literals (listen and sources); no non-loopback listener exists.
  - Lab: `lab.Options.Aggregator` adds a peers section per node
    (`localpeer` = node name, a peers bind exposed as `Node.PeersAddr`,
    the aggregator as a remote peer with a per-node address); without it
    the generated configuration is unchanged. `make lab-test` also runs
    `./cmd/...` and has a 15-minute timeout.
- Review fix batch 1 (independent review R-1..R-4, codex C-1..C-3; C-2
  duplicates R-1):
  - R-1: htad shutdown no longer waits forever on a blocked stdout. After
    the sessions close it waits at most 5 s (`outputDrainTimeout`) for
    the event writer, then exits 1 ("event output blocked"); the first
    signal also restores the default disposition, so a second SIGTERM
    ends the process at once. `TestRunBlockedOutput` (in-process) and
    `TestSIGTERMBlockedStdout` (process with an unread stdout pipe and
    2000 updates: exit 1 in about 6 s under `-race`; a second SIGTERM
    kills it 0.2 s after the first).
  - R-2: the idle timeout counts from when the last message was
    processed, not from the read that delivered it, so a burst behind a
    slow sink cannot end a live source as idle. `TestSlowSinkIsNotIdle`
    (10 updates in one write, 300 ms per event, idle 1 s) fails with the
    old accounting ("no message for 1.201s" after 4 updates) and passes
    now.
  - R-3: htad's comment now states that updates are acknowledged when
    the queue accepts them, so events queued when a write fails may be
    acknowledged and lost from the output (nothing is stored).
  - R-4: `config.Parse` rejects duplicate keys and keys that match a
    field only case-insensitively, at any depth (`TestInvalid` cases).
  - C-1: the lab accepts only loopback ip:port aggregator addresses,
    including per-node overrides (`TestValidateAggregator`).
  - C-3: `max_session_tables` counts every table a source shares, input
    or not, and must be at least the number of input tables.
  - Checks: `make check` pass (`artifacts/check-04-b1.log`); `make
    lab-test` both versions pass in 1 min 45 s
    (`artifacts/lab-test-04-b1.log`), run before three test-only
    variable renames for lint.
- Review fix batch 2 (verification R-5, R-6):
  - R-5 (regression from the R-4 fix): the key check swallowed JSON
    syntax errors and looped forever on malformed arrays or objects. It
    now returns them, so `config.Parse` fails with `ErrInvalid` at once.
    htad also returns right after loading its configuration if a signal
    arrived meanwhile. Tests: six malformed `TestInvalid` cases (trailing
    and missing commas, scalars in an array, a truncated file, a trailing
    comma in an object, 20000 nested `[`), each bounded by 5 s; four of
    them hang with the batch 1 code. `TestMalformedConfigExits` (process
    exits 1 in under 0.1 s) and `TestSignalledDuringStartup`.
  - R-6: removed a stray `htad` binary from the repository root, built
    by `go build ./cmd/htad` during batch 1.
  - Checks: `make check` pass (`artifacts/check-04-b2.log`); `make
    lab-test` both versions pass in 1 min 44 s
    (`artifacts/lab-test-04-b2.log`).
- Independent review: one full review plus two verification rounds by a
  fresh reviewer, and a cross-model (codex) pass on the first and final
  diffs. Findings R-1 to R-6 and C-1 to C-3 fixed and confirmed. A
  follow-up verification of S-1 and S-2 found the code clean. The
  upstream draft's review findings R-7 to R-9 are recorded in the draft
  as open items; its corrections were not re-verified.
- Accepted limitation (codex C-4): when the event queue stays full, a
  session that failed with `ErrQueueFull` closes its socket but keeps its
  source slot until its `SessionDown` is queued, so `Status` reports it up
  and no new session starts until the consumer drains. This is the
  backpressure that preserves per-source lifecycle ordering; phases 14
  and 15 should bound it and surface it in health reporting.
- Possible upstream report: in both pinned builds HAProxy clears a peer's
  liveness flag when any connection to it is released, including an
  abandoned attempt, and can then drop a live session. Mitigated here by
  extra heartbeats (`Conn.Nudge`); a residual window costs one reconnect.
- Follow-up (owner-approved suggestions S-1, S-2 and the upstream draft):
  - S-1: `sources.Start` re-applies the plaintext gate
    (`config.Config.CheckPlaintextGate`: the new
    `InsecurePlaintextLoopbackLab` field, which `Validate` sets, plus
    loopback IP literals for listen and source addresses) and requires
    an injected listener to be on loopback, before anything is bound or
    dialed. Tests: `TestCheckPlaintextGate`, `TestStartRefusesOffLoopback`.
  - S-2: `FuzzRun` keys accepted updates by (table ID, update ID).
    Sending every ack with table ID + 1 made seed 6 fail ("acknowledged
    table 2 update 6"), which the previous update-only check would have
    accepted; reverted.
  - Upstream draft `docs/upstream/peers-alive-flag-release.md`: a
    stdlib-only Python remote peer reproduces the liveness kill with
    stock HAProxy (12/20 on 3.4.6, 11/20 on 3.2.25 within a 4 s watch;
    0/20 and 1/20 with two heartbeats after the close), with source
    references and a 3.4.6 trace. The heartbeat-first result was
    withdrawn in review (the session dies at 5.0 s instead), and an 8 s
    watch closed 10/10, so the 4 s rates understate it. Open items
    R-7 to R-9 are listed in the draft; not checked against master.
    Evidence in `artifacts/upstream-peers-alive/`.
  - Checks: `make check` pass (`artifacts/check-04-f1.log`); `make
    lab-test` both versions pass (`artifacts/lab-test-04-f1.log`).
- Remaining work / next action: phase 05 (output tables and isolation).
