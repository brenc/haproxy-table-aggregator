# 06 — Freshness feasibility gate

Status: complete

Depends on: [05](05-output-isolation.md).

## One-turn outcome

A small, experimentally verified output/readiness scheme that stock HAProxy can
invalidate locally even when the aggregator is dead, paused, or replayed.

## Work

- Use known output values and controllable simulated readiness; a complete
  aggregation engine is not needed. Define authoritative versus local-fallback
  ACL behavior before implementing the arithmetic.
- Test locally expiring metadata and, if necessary, generation or absolute-age
  fields. Ordinary `table_idle`/`table_expire` lookups are possible primitives,
  not proof that a relative TTL alone survives delayed teaching or reload.
- Freeze the chosen layout and its HAProxy expressions here. If it needs clock
  synchronization, define and test the skew bound and failure behavior explicitly.
- Stop publication, pause the process, queue a marker behind a slow receiver,
  reconnect/resync, and reload HAProxy with old metadata still in its tables.
- Couple readiness to data publication progress. Define how a marker cannot
  overtake unsent output or certify entries whose age exceeds the allowed bound.
  Transport acknowledgment alone is not proof of application-level freshness.
- Prove how missing keys, partially populated snapshots, and already expired
  keys select the correct policy. Never track metadata with request rules.

## Acceptance

- [x] With the process killed or paused, aggregate authority ends locally within
      2 seconds of the last valid publication, measured by request decisions.
- [x] Buffered or replayed markers cannot extend that authority indefinitely or
      grant a fresh full lease to data already outside the allowed age bound.
- [x] HAProxy reload/resync during an outage does not revive stale authority.
- [x] A live marker cannot conceal a backlog of stale aggregate entries.
- [x] Missing metadata and incomplete output select local protection.
- [x] Healthy repeated requests do not refresh the readiness lease themselves.
- [x] The scheme runs on unmodified upstream binaries with documented clocks,
      timers, field widths, generation behavior, and configuration assumptions.

## Stop and handoff

This phase may end with a **blocked feasibility finding**, not a finished
implementation. Keep the smallest failing reproduction and explain whether
stock configuration can meet the contract. Do not weaken the 2-second target,
invent a protocol extension, or require patches without an owner decision.
Proceed to the aggregation engine only after this gate has a supported design.

## Frozen scheme (output schema version 2)

Package `internal/output` owns the layout and the lease; the phase 05 schema
(version 1) is replaced. The bump is deliberate: version 1 promised zero
reserved slots and no authority, so an ACL written for it must not read
version 2 entries as version 1.

### Layout

Both tables are IPv6-keyed stock stick tables storing `gpt(4)` (unsigned
32-bit tags), attached only to the peers section shared with the aggregator,
and never tracked or incremented by a request rule.

| Table kind  | Key                                    | gpt[0] | gpt[1]                                         | gpt[2]                           | gpt[3] |
| ----------- | -------------------------------------- | ------ | ---------------------------------------------- | -------------------------------- | ------ |
| `aggregate` | input key expression (`ipmask(32,64)`) | 2      | rate: estimated requests per input period      | session generation (never 0)     | 0      |
| `metadata`  | `::` only (`ipv6(::)` in HAProxy)      | 2      | lease deadline: Unix ms mod 2^32 (0 = revoked) | session generation (0 = revoked) | 0      |

- Session generation: a random non-zero 32-bit value per peers session
  (`output.NewGeneration`; `peersession.Options.Generation` fixes it for
  tests), written by the session into every entry it sends, values and
  markers alike. A marker therefore certifies only values its own session
  wrote, in order, ahead of it. This is what makes the scheme safe with
  more than one writer: on a soft reload the old HAProxy process teaches
  the new one whatever it held, and that teach can land after the
  session's newer values (review finding R1-F1). Entries written by any
  earlier session, of this or a previous aggregator incarnation, carry
  another generation and are never certified by the current session's
  markers; an earlier session's marker taught the same way certifies only
  its own session's values, and only until its own deadline. Such an
  overwritten key reads as local until the session's next periodic refresh
  (see Publication and lease) re-sends it under the current generation:
  within the refresh interval, a third of the aggregate table's expire by
  default, even if its value never changed. Each teach and each refresh
  rewrites every key currently in the store, that is, every key published
  and not yet retired (see Retirement).
- Lease deadline: the low 32 bits of the Unix time in milliseconds on the
  aggregator's wall clock until which the output is authoritative. HAProxy
  compares it modulo 2^32, so the 49.7-day wrap of the value is harmless; a
  marker would have to survive about 49.7 days to alias, and HAProxy removes
  it within 2 s.
- The aggregate table's `expire` is the operator's (lab: 3 input periods).
  The metadata table's `expire` is at most `output.MaxLease` (2 s; config
  and store refuse more); the lab uses exactly 2 s.

### Authority rule

An aggregate entry is authoritative for a request only while all of these
hold, evaluated by HAProxy alone: the metadata entry `::` exists with
version 2; `(deadline - date(0,ms)) mod 2^32` is at most 2000 (the deadline
is not past and at most 2 s ahead); and the aggregate entry has version 2
and the same generation as the metadata entry (a revocation's generation 0
matches nothing, because no aggregate entry carries 0). Otherwise the request falls
back to local protection: the local limit, which is always active and never
divided by cluster size, is the only limit. Under authority, either limit
can reject. Missing keys, missing or revoked metadata, an expired or
too-distant deadline, and entries written by any other session all select local
protection.

The HAProxy expressions, validated by `TestFrozenExampleValidates`
(`haproxy -c -V`, both pinned versions) and used verbatim (with
`txn.lab_key` for `txn.agg_key`) by the lab probe that every phase 06 live
test decides with:

```haproxy
global
    # The local peer name must match the "server" line without an
    # address; otherwise HAProxy drops the whole peers section.
    localpeer a

defaults
    mode http
    timeout connect 2s
    timeout client 10s
    timeout server 10s

peers agg
    bind 127.0.0.1:10001
    server a
    server agg 127.0.0.1:10000

backend lab_in
    stick-table type ipv6 size 1m expire 30s store http_req_cnt,http_req_rate(10s) peers agg
backend lab_out
    stick-table type ipv6 size 1m expire 30s store gpt(4) peers agg
backend lab_meta
    stick-table type ipv6 size 16 expire 2s store gpt(4) peers agg

frontend fe
    bind 127.0.0.1:8080
    http-request set-var(txn.agg_key) src,ipmask(32,64)
    http-request track-sc0 var(txn.agg_key) table lab_in
    # Local protection: always active, with the full per-proxy limit.
    http-request deny deny_status 429 if { sc_http_req_rate(0) ge 100 }
    # Aggregate protection: only while the output is authoritative.
    http-request set-var(txn.agg_now) date(0,ms),and(4294967295)
    http-request set-var(txn.agg_left) ipv6(::),table_gpt(1,lab_meta),sub(txn.agg_now),and(4294967295)
    http-request set-var(txn.agg_gen) ipv6(::),table_gpt(2,lab_meta)
    acl agg_meta  ipv6(::),table_gpt(0,lab_meta) eq 2
    acl agg_lease var(txn.agg_left) -m int le 2000
    acl agg_v2    var(txn.agg_key),table_gpt(0,lab_out) eq 2
    acl agg_gen   var(txn.agg_key),table_gpt(2,lab_out),sub(txn.agg_gen) eq 0
    acl agg_over  var(txn.agg_key),table_gpt(1,lab_out) ge 1000
    http-request deny deny_status 429 if agg_meta agg_lease agg_v2 agg_gen agg_over
    default_backend app

backend app
    server app1 127.0.0.1:8081
```

`var()` needs `-m int` for a numeric match; a missing `lab_meta` entry
reads 0 through `table_gpt`, which fails `agg_meta`.

### Publication and lease

- The publisher (the aggregation engine from phase 07 on; a simulated
  renewer here) sets values with `Store.Set` and declares readiness with
  `Store.SetLease(until)`: every value set so far is authoritative until
  `until`, at most `MaxLeaseLength` away: 1 s, which is `MaxLease` (2 s)
  minus the accepted clock budget (1 s), enforced by `SetLease` (review
  finding R3-F2; a requirement on phases 07–10, which must not lengthen
  the lease without shrinking the clock budget). It derives `until` from when its
  data was current, not from when it calls, and stops renewing (or calls
  `Revoke`, which writes a revocation at once) when any required source is
  missing, recovering, backlogged, or incomplete.
- `SetLease` reads the wall clock once and fixes the deadline every marker
  of that lease carries. A marker written later (after a suspend, a
  stalled session, or a wall-clock step) keeps that deadline, and is not
  written at all once the lease has run out by either the monotonic or the
  wall clock; its relative lifetime is the smaller of the two (review
  finding R1-F2: the deadline was previously re-derived from the current
  wall clock and the monotonic time left, so a suspend could hand an old
  lease a fresh deadline).
- Each session writes the lease into every metadata table as an explicit
  **timed** update (type 0x85) of `::` whose remaining lifetime is the time
  the lease has left. It reads the lease before the values, writes the
  values first, and writes the marker only once every output table has
  been taught in that session; so a marker never overtakes output it
  certifies, and every value it certifies is already applied when HAProxy
  applies it (one TCP stream, applied in order). Other writers into the
  same tables (the old process's teach on a reload) are excluded by the
  session generation. A lease that has run out
  is not written. Markers are never taught from the store, so a teach (on
  connect, reconnect, or resync request) never replays an old marker; the
  teach is followed by a fresh marker only if the lease is still live.
- Aggregate updates stay ordinary (untimed). Teaches restart their HAProxy
  lifetimes (the phase 05 finding), which grants nothing: lifetime is not
  authority. A key HAProxy expires reads as missing (local).
- Periodic refresh (review finding R2-F1): once every output table is
  taught, each session re-sends the whole output (each table's definition
  and every entry, unchanged ones included, under its generation) every
  refresh interval R, then writes the lease again after those values, as
  a teach does (`peersession.Options.Refresh`; default R = a third of the
  shortest aggregate table expire, 10 s in the lab's 30 s tables). Bound:
  a key another writer overwrote (the old process's reload teach) regains
  aggregate authority within R of the overwrite, and an unchanged
  published key never expires in HAProxy while its session runs. The
  marker still certifies only values its session wrote ahead of it. Cost:
  every R, each session sends every key currently in the store once
  (roughly 30–45 bytes per IPv6 key plus framing).
- Retirement (review finding R3-F1): a key stays in the store, and so in
  every refresh and every proxy's table, until the publisher retires it
  with `Store.Retire` (`sources.Manager.Retire`). Retire removes it from
  the store; every session that sees a retirement switches to a new random
  generation and re-sends the remaining output under it before its next
  marker. HAProxy's copy of the retired key keeps the old generation, so
  no later marker certifies it, and since nothing sends it again HAProxy
  expires it after the aggregate table's expire. Until the re-send and
  marker reach the source, the copy is as authoritative as the marker
  already there, the same bound a value change has. Each wake-up that sees
  retirements costs a session one full re-send, and every remaining key
  reads as local from its re-send until the new marker arrives, a brief
  availability gap (never a safety one) that grows with output size.
  Frozen hand-offs: when to
  retire a key, and batching retirements, is phase 10's publisher policy;
  bounding the store's size, the refresh bandwidth (keys in the store ×
  sessions per R), and the re-send cost of retirements is phase 14's.
- Transport acknowledgements play no part (3.4.6 does not acknowledge while
  learning; acks never proved freshness).
- Renewal: at most every lease/4 (the lab renews a 1 s lease every 250 ms).
  The lease is at most 1 s (`MaxLeaseLength`, enforced by `SetLease`).

### Clocks, timers, and bounds

- Deadlines are absolute and compared with HAProxy's own wall clock
  (`date`, sampled per event-loop iteration), so the guarantee against
  delayed or replayed markers needs the aggregator's wall clock to agree
  with every proxy's. Let δ = aggregator clock − HAProxy clock and L the
  lease. A marker written at W (data current at C ≤ W) gives authority
  until C + L + δ by HAProxy's clock, however late it is delivered.
  - Safety bound: δ ≤ 2 s − L (1 s with the recommended lease) keeps every
    authoritative decision within 2 s of the data it certifies, whatever
    the delivery delay.
  - Availability bound: δ ≥ −(L − renewal interval − delivery delay)
    (about −0.75 s with a 1 s lease renewed every 250 ms) keeps authority
    continuous while healthy; further behind, authority flaps and below
    −L it is never granted (fail-safe local). This lies inside the
    accepted ±1 s budget: with the aggregator 0.75–1 s behind a proxy,
    that proxy's aggregate authority flaps or is never granted. That is
    an availability effect within the bound (local protection stays in
    force), not a safety failure (review finding R3-F2).
  - Beyond the safety bound (δ > 2 s − L): the ACL's 2 s window refuses a
    marker while it reads more than 2 s ahead. Just past the bound (δ <
    2 s − L + renewal interval + delivery delay) each marker becomes live
    shortly before the next renewal replaces it, so authority flaps while
    published (measured at +1.1 s: about 90 of 150 decisions aggregate);
    further past it, steady renewal never grants authority (measured at
    +1.5 s: none). A marker delayed by D can grant authority until
    min(C + L + δ, the marker's relative expiry): at most δ − (2 s − L)
    past the bound. This is the residual risk of bad clocks.
  - The aggregator cannot measure δ through the peers protocol (no message
    carries the proxy's time). Operators must keep clocks synchronized
    (NTP/chrony; milliseconds in practice) and alert well before 1 s of
    offset; phase 15 should expose the offset if a probe is added.
- Independent of clocks, the marker's timed lifetime (at most the lease
  left, capped by the 2 s table expire) makes HAProxy drop it: a dead or
  paused aggregator loses authority at most 2 s (the metadata table's
  expire) after its last delivered marker even with skewed clocks, and
  within L when marker lifetimes never shrink (measured: 0.99 s at +1.5 s
  skew). HAProxy removes an entry when its expiry-tree position comes due,
  and a later, shorter lifetime does not move that position earlier
  (`stktable_touch_with_exp`, `process_tables_expire`). So a revocation
  (sent with 2 s) followed within 2 s by a new lease, or a renewal with a
  shorter lease than the one before, leaves the marker present until the
  longer position: up to 2 s, not L (review finding R1-F3; source reading,
  not measured). With nbthread > 1 a lookup holding the entry at the
  instant the expiry task visits it postpones removal by the table expire
  (2 s). In every case the deadline still bounds authority when clocks are
  within the safety bound.
- Reload: HAProxy's internal old-to-new teach uses timed updates, so the
  new process keeps both the deadline and the remaining lifetime (measured:
  `exp` 0.84–0.87 s of a 1 s lease about 0.14 s after the last renewal,
  on both versions).
- Source health (carried in from phase 04): the session idle timer counts
  from when a message is processed, so a dead source behind a backlog is
  detected late by up to the backlog × `event_timeout`. Readiness must not
  use the idle timer: a source counts as healthy only while
  `now − Stats.LastRx` is below a health bound H, and `LastRx` is the time
  the last processed message was _read_, so its age grows with both
  silence and backlog (`TestSlowSinkIsNotIdle` now asserts this). H must
  exceed HAProxy's idle heartbeat (about 3 s); fallback after a silent
  source failure then takes at most H plus a revocation's delivery, within
  the 10 s target. Phase 07/11 implement the check.

## Execution record

- Commands and versions (2026-10-04, Debian 13 amd64): Go `go1.27.1`
  (repo-pinned); stock HAProxy `3.4.6-56332c5` (binary SHA-256
  `81df335a…d1c4`) and `3.2.25-70469d3` (`5f89a725…8869`) from phase 01's
  `make haproxy` (not rebuilt).
  - `make check`: pass (0 lint issues, `go test -race ./...` ok, no
    vulnerabilities; log `artifacts/check-06.log`, and after review batch 1
    `artifacts/check-06-b1.log`, after batch 2 `artifacts/check-06-b2.log`, after batch 3
    `artifacts/check-06-b3.log`).
  - `make lab-test` (both versions, race detector on): pass (log
    `artifacts/lab-test-06.log`; after review batch 1
    `artifacts/lab-test-06-b1.log`; after batch 2
    `artifacts/lab-test-06-b2.log`; after batch 3 `artifacts/lab-test-06-b3.log`), including every new
    `TestLiveFreshness*` test (23 passing tests and subtests per version
    after batch 3),
    `TestFrozenExampleValidates`, `TestConfigValidates`, and every phase
    01–05 live test. Per-test runs while developing are under
    `artifacts/p06/`.
  - `make lab-smoke HAPROXY_BIN=...` with each pinned build: `smoke ok`
    on both (`artifacts/lab-smoke-06.log`, `artifacts/lab-smoke-06-b1.log`,
    `artifacts/lab-smoke-06-b2.log`, `artifacts/lab-smoke-06-b3.log`); the lab without an aggregator
    is unchanged.
  - `go test -fuzz '^FuzzRun$' -fuzztime 20s ./internal/peersession`:
    about 132k executions, pass (`artifacts/p06/fuzzrun.log`; rerun
    after batch 1, `artifacts/p06/fuzzrun-b1.log`, and after batch 2,
    `artifacts/p06/fuzzrun-b2.log`, and after batch 3,
    `artifacts/p06/fuzzrun-b3.log`).
- Upstream sources (read to confirm facts, nothing copied): `src/peers.c`
  (`peer_treat_updatemsg`: a timed update's expire, capped at the table
  expire, sets the entry's expiry from reception; `peer_send_teachmsgs`:
  full teaches, including the old-to-new reload teach, send timed updates
  with the remaining lifetime, incremental pushes do not),
  `src/stick_table.c` (`stktable_touch_with_exp`, `process_tables_expire`:
  an entry whose expiry moved earlier is removed only when its old
  expiry-tree position comes due, and an entry referenced at that instant
  is postponed by the table expire; `sample_conv_table_gpt`: lookups take
  and release a reference, they never touch), `src/haproxy.c` (soft reload
  with `-sf`), and the `date`, `sub`, `and`, and peers `bind` entries of
  `doc/configuration.txt`, all from both verified release tarballs.
- Acceptance evidence (all live tests in `internal/sources`
  `live_freshness_test.go` under `make lab-test`, both versions, unless
  noted; one node "a" dialing the aggregator; probes every 10 ms; "the
  last valid publication" is the publisher's last `SetLease`, the stricter
  reference since its marker is written just after it; numbers are from
  `artifacts/p06/` runs and match within a few ms on both versions):
  - Killed or paused: `TestLiveFreshnessStop` with a 1 s lease renewed
    every 250 ms. Killing the aggregator (manager closed) or pausing it
    (every read and write frozen, renewals stopped, the TCP session left
    `ESTA` on the node) leaves the last aggregate decision 0.99 s after the
    last renewal and every one of about 100 decisions after 2 s local, on
    both versions. Thawed, the same session (1) regains authority.
  - Buffered or replayed markers: `TestLiveFreshnessDelayedDelivery`
    ("publisher stops") holds every byte the aggregator writes, changes
    values behind the hold, renews 300 ms more, then releases 2.8 s after
    the hold (2.5 s after the last renewal). The held values and markers
    arrive (rate 50 visible, lease left reads as past, 4294965793 mod
    2^32), and every decision stays local; about 100 decisions after the
    release would have been aggregate under the relative-lifetime rule the
    probe also reports (`X-Lab-Relative-Authority`): that is the smallest
    reproduction of why a relative TTL alone is insufficient. Replay of
    markers: never taught from the store (`TestOutputTeachAndLive`: a
    resync teach carries values only, then "finished", then a fresh
    marker); HAProxy's replay of its own copy to the aggregator is
    acknowledged and discarded (phase 05); its reload teach keeps the
    deadline (below).
  - Reload/resync during an outage: `TestLiveFreshnessReload` (a
    `Reloadable` lab; lease 1 s, the maximum). The aggregator is killed
    and the node soft-reloaded with `-sf` about 0.15 s later. The new
    process holds the learned marker with its remaining lifetime (exp
    0.84–0.87 s when read 0.13–0.16 s after the last renewal, never
    restarted to 1 s or 2 s) and the same deadline and generation; it
    answers about 86 aggregate decisions from that marker (the probe
    reports the answering PID), the last 0.99 s after the last renewal,
    then only local ones, and every decision after 2 s is local. A new aggregator incarnation with no lease
    then reconnects: the node resynchronizes and replays its output copy,
    values are taught again, and a second reload follows; no decision is
    aggregate until a lease is set again.
  - Reload of a healthy node (review finding R1-F1 regression):
    `TestLiveFreshnessReloadOverwrite` (Linux; `live_reload_test.go`)
    stops the old process (SIGSTOP), publishes 1500 → 50, reloads, waits
    until the new process is authoritative on 50, and resumes the old
    process, whose own teach then overwrites the new value with 1500. On
    both versions the old copy (1500, old generation) answered every
    decision until the refresh. Resumed 2.5 s after it stopped, none of
    those was aggregate; resumed at once, 14–15 were, all under the old
    session's own marker and within 2 s of the stop; all others are local
    under the new session's marker. A third case keeps the store at 1500
    (unchanged key, review finding R2-F1): the old copy is told apart only
    by its generation and is local likewise. In all three the periodic
    refresh (R = 10 s) re-sent the key and it was authoritative again
    7.6–10.0 s after the old process resumed, within R. Against the code
    before batch 1 (one generation per aggregator incarnation) the
    reviewer's sequence gave 382 of 382 decisions aggregate on the stale
    1500 with fresh markers; against batch 1 (no refresh) an unchanged key
    stayed local for at least 40 s (reviewer R2).
  - Live marker and backlog: `TestLiveFreshnessDelayedDelivery`
    ("publisher keeps publishing") renews throughout a 2.8 s hold with
    values changed behind it: authority ends within the bound although the
    aggregator never stopped, no decision during the hold after 2 s is
    aggregate, and after the release authority returns only with the
    delivered values (rate 50 aggregate 200, new key 5000 aggregate 429;
    never aggregate on the pre-hold 1500). `TestOutputTeachAndLive` (fake
    peer) checks the stream order: a value then a lease send the update,
    then the marker; a marker is withheld while any output table is
    untaught.
  - Missing metadata and incomplete output: `TestLiveFreshnessLease`:
    taught values without a lease are local (`lab_meta` missing); under a
    live lease an unpublished key is local; after `Revoke` the last
    aggregate decision was sent 0 ms after it.
    `TestLiveFreshnessGeneration`: a new aggregator incarnation
    republishing only one of two keys makes the other (left by the earlier
    session, version 2, rate 1500, its generation) local, and the two
    sessions' generations differ.
    `TestLiveFreshnessExpiredKey` (1 s period, 3 s expire): with the
    refresh disabled, a key HAProxy expired 3.0 s after publication is
    local under a live lease while the store still holds it; with the
    default refresh (1 s) the unchanged key stays authoritative in all 900
    decisions over 9 s (8–9 refreshes). Fake peer: no marker until every
    output table is taught; `TestOutputRefresh` checks that each refresh
    re-sends every entry under the session's generation before the
    marker. `TestLiveFreshnessRetire` (1 s period, 3 s expire): after
    `Retire` of one of two keys under a live lease, the retired key's
    copy (old generation) answered about 290–300 decisions and none was
    aggregate after the Retire; HAProxy expired it 2.99–3.02 s after the
    Retire; the kept key stayed aggregate in every decision (0 of about
    400 local) under the new generation (1 rotation). `TestOutputRetire`
    (fake peer): after `Retire` the session sends the remaining key under
    a new generation, then the marker, never the retired key.
  - Requests do not renew: `TestLiveFreshnessLease` stops renewal while
    probing every 10 ms: authority ends 0.99 s after the last renewal, the
    `lab_meta` `::` expiry keeps falling (947 → 243 ms over 0.7 s) and the
    entry is gone after 2 s. `table_gpt` only takes and releases a
    reference.
  - Unmodified binaries and documented assumptions: every result above is
    from the pinned stock builds; `TestConfigValidates` (lab
    configurations) and `TestFrozenExampleValidates` (the block above) pass
    `haproxy -c -V` with zero-warning on both. Clocks, timers, widths,
    generation behavior, and configuration are frozen above.
    `TestLiveFreshnessSkew` (1 s lease, kill after 1.5 s of publication):
    +0.9 s and −0.5 s keep every decision aggregate while published and end
    authority within 2 s; +1.1 s (just past the bound) flaps, about 90 of
    150 decisions aggregate while published; +1.5 s grants no authority
    while published (the 2 s window refuses the marker; 0 of about 150
    decisions); in all three the last marker's relative lifetime ends
    authority 0.99 s after the last renewal; −1.5 s never grants
    authority. `TestLeaseDeadlineIsFixed` (unit): an hour's wall-clock jump
    without the monotonic clock leaves the lease unwritable, and a step
    back leaves its marker unchanged.
- Decisions or deviations:
  - Schema version 2 (above). `Store.Set` refuses metadata tables; the
    session writes the marker from `Store.Lease`. New `output` API:
    `StoreOptions` (wall clock), `NewStore(tables, opts)`, `SetLease`,
    `Revoke`, `Lease` (with the fixed `WallUntil`), `Marker(lease, gen)`,
    `NewGeneration`, `MarkerValues`, `RevocationValues`, `DeadlineValue`,
    `LeaseLeft`, `MaxLease`, `LeaseWindowMillis`; `SlotGeneration` now
    holds the session generation; `MetadataValues` is gone.
    `sources.Manager` gains `SetLease` and `Revoke`.
  - `peersession`: markers are explicit timed updates; every entry carries
    the session generation (`Options.Generation`, random when 0); `Stats`
    gains `Markers`, `Revocations`, `LastMarker`, `LastDeadline`,
    `Generation`, and `OutputUpdates` now counts aggregate updates only.
  - Review batch 1: R1-F1 fixed by per-session generations (replacing the
    per-incarnation epoch); R1-F2 fixed by fixing the deadline at
    `SetLease`; R1-F3 fixed in the Clocks section (flapping just past the
    bound; the clock-independent bound is the 2 s metadata expire, L only
    while marker lifetimes never shrink).
  - Config and store refuse a metadata `expire` above 2 s; the lab's
    `lab_meta` expires after 2 s (`lab.MetaExpire`).
  - Lab: the probe listener applies the frozen rule and reports each slot,
    the lease left, the decision (`X-Lab-Authority`), the relative-only
    decision, and the answering PID. `Options.Reloadable` binds the peers section on a Unix socket
    (the old process must connect to the local peer's address, which an
    inherited descriptor lacks) and keeps listeners for `Node.Reload`,
    which starts a new process with `-sf` on the same configuration and
    inherited listeners and waits for its own PID on the runtime socket.
  - The pause is simulated in-process (the publisher stopped, then every
    later read and write frozen), which HAProxy cannot tell from a stopped
    process: the TCP session stays established and no byte flows. As
    modelled the pause measures the same bound as a stopped publisher; a
    read already blocked when the freeze starts is not held. Stalled delivery
    is simulated by queueing the aggregator's writes and releasing them in
    order.
  - Clock synchronization is required for the delayed/replayed-marker
    guarantee (see Clocks). Without a shared clock no scheme can meet
    bullets 2 and 4: a receiver sees a delayed stream exactly as a fresh
    one, and no peers message carries the proxy's time.
  - **Owner decision (2026-10-04):** the clock-synchronization requirement
    is accepted. Aggregator and HAProxy wall clocks must agree within 1 s
    via NTP, which runs on every host. That budget meets the safety bound
    δ ≤ 2 s − L only for leases of at most 1 s, so the code enforces it
    (`output.ClockBudget`, `MaxLeaseLength` = 1 s; batch 3). A negative
    skew inside the budget (aggregator 0.75–1 s behind) can make authority
    flap or never be granted: fail-safe, an availability effect within
    the bound. The bound and the failure behavior outside it stay as
    documented under Clocks.
  - Review batch 2 (R2-F1): sessions refresh the whole output every R (a
    third of the aggregate expire by default), so overwritten or unchanged
    keys regain authority within R and published keys do not expire in
    HAProxy; `Stats.Refreshes`, `sources.Options.OutputRefresh` (tests
    only, negative disables).
  - Review batch 3: R3-F1, `Store.Retire`/`Manager.Retire` with a
    generation rotation per session (`Stats.Rotations`); R3-F2, leases
    capped at `MaxLeaseLength` (1 s) and the within-budget negative-skew
    effect documented.
  - Final verification (R4) confirmed R3-F1 and R3-F2 with no blocking
    defects; its one Low finding, R4-F1 (stale comments and doc text
    after batches 2–3), was corrected at closeout.
- Limitations: one node per experiment (the mechanism is per session and
  per proxy); nbthread 1 in the lab; the skew outcomes beyond the bound
  under delayed delivery, and the expiry-tree effect of a shrinking marker
  lifetime, are derived, not measured; a key overwritten by an old
  process's reload teach is local for up to the refresh interval R (10 s
  with 30 s tables) before the refresh restores it; the refresh's
  bandwidth and the retirement re-send cost are unmeasured and left to
  phase 14, retirement policy to phase 10; a 49.7-day alias of the deadline is excluded by the 2 s marker
  lifetime, not tested.
- Carried in from phase 04 review: addressed in Clocks above (readiness
  uses `LastRx`, the read time, not the idle timer).
- Remaining work / next action: phase 07. Phases 07–10 must drive
  `SetLease`/`Revoke` from source health and data currency with leases of
  at most 1 s, and retire keys they no longer publish (`Retire`); the
  session refresh already keeps published keys present and re-stamped.
