# 05 — Output tables and isolation

Status: complete

Depends on: [04](04-peer-sessions.md).

## One-turn outcome

Prove that a normal Go peer can write a known integer into a distinct stock
HAProxy output table, and cannot accidentally feed it back as an input.

## Work

- Give each proxy its own input table and a separately named output table in
  its dedicated relationship with the aggregator. Keep other peer sections out
  of this experiment. Add metadata-table scaffolding if needed for phase 06.
- Publish known integer values through the ordinary peers protocol, including
  a full teach and subsequent live updates. No Runtime API writes as the output
  transport; runtime reads are test observations only.
- Specify the initial field layout, units, schema version, and numeric limits
  here. General-purpose tags/arrays are candidates; validate the exact form on
  both upstream versions. Reserve metadata without claiming freshness yet.
- Add an HAProxy ACL/test response that proves the ordinary fetch sees the value.
- Treat incoming output/metadata definitions and replay as non-contributing
  state. Never send input-table updates from the aggregator.

## Acceptance

- [x] Go-published values are visible through stock HAProxy ACLs on both nodes.
- [x] A live change appears without a reconnect or full synchronization.
- [x] Publishing output leaves each node's input count unchanged.
- [x] Received/replayed output records never change source contributions.
- [x] Source table-ID collisions do not misroute output or acknowledgments.
- [x] HAProxy configuration validates without a patch, module, or protocol change.
- [x] The supported output schema and example lookup are recorded here.

## Stop and handoff

This is the first feasibility gate. If stock HAProxy cannot receive the chosen
ordinary fields, investigate the mapping and wire behavior before continuing.
Do not substitute a custom HAProxy build or an external per-request service.

## Output schema (version 1)

Package `internal/output` owns the layout; the aggregator announces exactly
this definition for every output table and requires each source's own
definition of the table to match (else the session ends with `ErrSchema`).

Every output table is an IPv6-keyed stock stick table storing `gpt(4)`
(four unsigned 32-bit general-purpose tags, data type 22), attached only to
the peers section shared with the aggregator, and never tracked or
incremented by a request rule. Its name differs from every input table's
(configuration rejects a collision).

| Table kind  | Key                                    | gpt[0]           | gpt[1]                                    | gpt[2]                          | gpt[3]   |
| ----------- | -------------------------------------- | ---------------- | ----------------------------------------- | ------------------------------- | -------- |
| `aggregate` | input key expression (`ipmask(32,64)`) | schema version 1 | rate: estimated requests per input period | reserved for phase 06, always 0 | reserved |
| `metadata`  | `::` only (`ipv6(::)` in HAProxy)      | schema version 1 | reserved for phase 06, always 0           | reserved for phase 06, always 0 | reserved |

- Units: gpt[1] is estimated requests per the input table's configured
  `http_req_rate` period (not per second), the contract's published unit.
- Numeric limits: every slot is an unsigned 32-bit integer, 0 to
  4294967295. A rate above that saturates at 4294967295
  (`output.RateValue`), which compares at or above every limit an ACL can
  write against the slot, so saturation can only restrict, never wrap.
  Phase 08/09 own the checked arithmetic that produces the rate.
- Schema version: `table_gpt` returns 0 for a missing key, so gpt[0] is a
  non-zero version. A lookup that does not see 1 there has no usable
  output, whatever the other slots read. The store refuses any other
  version and any non-zero reserved slot.
- Lifetimes: updates are ordinary (untimed, message `0x80` with an
  explicit ID), so each entry gets the receiving table's own `expire` from
  reception. Version 1 claims no freshness or authority; ACLs must not
  derive authority from either table until phase 06 defines it.
- Configuration (`htad`): `"outputs": [{"name": "lab_out", "kind":
  "aggregate", "expire": "30s"}, {"name": "lab_meta", "kind": "metadata",
  "expire": "30s"}]`; `expire` must equal the HAProxy table's.

Example HAProxy configuration and lookup (the lab's probe frontend uses
exactly these expressions):

```haproxy
global
    # The local peer name must match the "server" line without an
    # address; otherwise HAProxy drops the whole peers section (with only
    # a warning) and every published value with it.
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
    stick-table type ipv6 size 1k expire 30s store http_req_cnt,http_req_rate(10s) peers agg
backend lab_out
    stick-table type ipv6 size 1k expire 30s store gpt(4) peers agg
backend lab_meta
    stick-table type ipv6 size 16 expire 30s store gpt(4) peers agg

frontend fe
    bind 127.0.0.1:8080
    http-request set-var(txn.key) src,ipmask(32,64)
    http-request track-sc0 var(txn.key) table lab_in
    acl agg_v1   var(txn.key),table_gpt(0,lab_out) eq 1
    acl agg_over var(txn.key),table_gpt(1,lab_out) ge 1000
    http-request deny deny_status 429 if agg_v1 agg_over
    # metadata lookup (reserved in version 1): ipv6(::),table_gpt(0,lab_meta)
    default_backend app

backend app
    server app1 127.0.0.1:8081
```

## Execution record

- Commands and versions (2026-10-04, Debian 13 amd64): Go `go1.27.1`
  (repo-pinned); stock HAProxy `3.4.6-56332c5` (binary SHA-256
  `81df335a…d1c4`) and `3.2.25-70469d3` (`5f89a725…8869`) from phase 01's
  `make haproxy` (not rebuilt).
  - `make check`: pass (0 lint issues, `go test -race ./...` ok, no
    vulnerabilities; log `artifacts/check-05.log`).
  - `make lab-test` (both versions, race detector on): pass on the second
    run (log `artifacts/lab-test-05-r2.log`), including the new
    `TestLiveOutput` and `TestConfigValidates` and every phase 01–04 live
    test. The first run (`artifacts/lab-test-05.log`) failed once on
    3.4.6 in the phase 04 test `TestLiveSimultaneous`, which configures
    no outputs: in its fifth close-and-reopen cycle neither side
    re-established a session within 20 s (no daemon log was captured).
    Six immediate reruns of that test and the full second run passed; it
    is recorded as an unexplained intermittent failure for review, not
    attributed. The lab nodes there now carry the two output tables too.
  - `make lab-smoke` with each pinned build (`HAPROXY_BIN=...`):
    `smoke ok` on both (counts 10 and 20; log
    `artifacts/lab-smoke-05.log`). The lab without an aggregator is
    unchanged.
  - `go test -fuzz '^FuzzRun$' -fuzztime 20s ./internal/peersession`
    (now with an output table): about 87k executions, pass.
- Upstream sources (read to confirm facts, nothing copied):
  `src/peers.c` and `src/stick_table.c` of both verified release tarballs
  (`peer_treat_definemsg`, `peer_treat_updatemsg`, `peer_treat_ackmsg`,
  the ack/push loop, resync controls, `stktable_touch_with_exp`,
  `stktable_add_pend_updates`), and the `table_gpt` and `ipv6()` entries of
  both releases' `doc/configuration.txt`.
- Acceptance evidence:
  - Go-published values visible through stock ACLs on both nodes:
    `TestLiveOutput` (`internal/sources`, both versions). The lab (with an
    aggregator) now has `lab_out` and `lab_meta` (`gpt(4)`, `peers agg`)
    and a `probe` frontend that tracks nothing and answers with
    `table_gpt` lookups plus the ACL `table_gpt(0,lab_out) eq 1` and
    `table_gpt(1,lab_out) ge 1000` (429 when both match, else 200). Values
    published before any session existed (rates 400, 1500, and 7 for
    `2001:db8:a::`, `2001:db8:b::`, and IPv4 `192.0.2.7`, plus the
    metadata entry) read back on both nodes as version 1 with those rates
    and statuses 200/429/200; an unpublished key reads version 0, rate 0,
    status 200 (missing output never matches the ACL).
  - Full teach: those values could only arrive by a teach. Since review
    batch 1 a session teaches each output table once the node announces a
    matching definition of it, which stock HAProxy does for every shared
    table (empty ones included) in the teach that answers the daemon's
    resync request. Each session reported 3 teaches (`lab_out`,
    `lab_meta`, and the node's own startup resync request, answered
    before it had announced anything, so with no entries) carrying the 4
    entries, both tables announced (`SourceID` non-zero), and no other
    output update. After a reconnect each new session taught the full
    store again (2 teaches, 5 entries).
  - A live change appears without reconnect or full sync: publishing
    `2001:db8:a::` 400→1200 (200→429), `2001:db8:b::` 1500→50 (429→200),
    and a new key `2001:db8:c::` = 5 was visible through both nodes' ACL
    within about 6 ms (polling at 20 ms). Each session sent exactly 3
    updates and no teach, stayed session 1 with no `SessionDown`, and
    HAProxy's `new_conn` stayed 1 with status `ESTA`. HAProxy then
    acknowledged `lab_out` up to the last update ID sent (9) under the
    aggregator's table ID 1.
  - Publishing output leaves each node's input count unchanged: `lab_in`
    on node a holds only `2001:db8:a::` = 10 and on node b only
    `2001:db8:b::` = 20 before the aggregator connected, after the teach,
    after the live change, and after the replay; the responder saw no
    request during the output checks (probes never reach a backend). In
    `show peers`, every input table (`lab_in`, `prod_in`, `proxy_in`) has
    `remote_id=0` and `last_get=0` on both nodes: HAProxy never received a
    definition or update for an input table from the aggregator.
  - Received/replayed output never changes source contributions: after
    disconnecting both sessions, each node's resync replayed its copy of
    the output (5 `lab_out`/`lab_meta` entries per source, counted in
    `Stats.EchoedUpdates`, acknowledged, never queued). The only table
    events in the whole run name `lab_in`, and each source's contribution
    (latest `lab_in` count per key) is exactly `{2001:db8:a:: 10}` for a
    and `{2001:db8:b:: 20}` for b. `TestOutputTeachAndLive` (fake peer)
    feeds an echoed `t_out` update and checks it is acknowledged under the
    source's ID with no event; `FuzzRun` now runs with an output table and
    allows acknowledgements only for sink-accepted input updates or
    updates of the source's copy of the output table.
  - Table-ID collisions do not misroute output or acknowledgements:
    `TestLiveOutput` declares node b's output tables first
    (`lab.Aggregator.OutputFirst`), so HAProxy numbers node a's tables
    `lab_in`=1, `prod_in`=2, `proxy_in`=3, `lab_out`=4, `lab_meta`=5 and
    node b's `lab_out`=1, `lab_meta`=2, `lab_in`=3, ... The aggregator
    numbers `lab_out`=1, `lab_meta`=2 for both. On node a its ID 1
    (`lab_out`) is HAProxy's `lab_in` and its ID 2 HAProxy's `prod_in`;
    across sources, remote ID 1 is `lab_in` (input) from a and `lab_out`
    (output copy) from b. `show peers` on each node shows `remote_id` 1
    and 2 for `lab_out`/`lab_meta` and the aggregator records each
    source's own `local_id` for them (`OutputStats.SourceID`); acks of
    `lab_out` land on the aggregator's `lab_out` (last acked 9 = last
    sent), the aggregator's acks of `lab_in` carry HAProxy's IDs (cursor
    `update=1` on `lab_in`), and only a's ID 1 produced input events.
    `TestOutputTeachAndLive` repeats this with a fake source whose input
    is its ID 1 and its output copy its ID 2 (the aggregator's
    `t_out`/`t_meta` IDs), and `TestOutputFailures` ends sessions on an
    ack for an unannounced table, for an update never sent, or for a
    table with nothing sent (`ErrProtocol`).
  - HAProxy configuration validates without patch, module, or protocol
    change: `TestConfigValidates` (`internal/lab`, under `make lab-test`)
    runs `haproxy -c -V` on the plain, aggregator, and output-first lab
    configurations (with `zero-warning`): "Configuration file is valid"
    on both versions. The same configurations run every live test. The
    output path uses only ordinary definition (`0x82`), update (`0x80`),
    and ack (`0x84`) messages and stock `table_gpt`/`ipv6()`.
  - Supported output schema and example lookup: recorded above.
- Decisions or deviations:
  - Output form: `gpt(4)` with a version slot, because `table_gpt` reads
    0 for missing keys; two slots reserved per table kind so phase 06 can
    add generation/readiness without changing the array length. Metadata
    scaffolding: a `metadata` table with one entry under `::`, published
    as version 1 with zero reserved slots; it claims nothing yet.
  - New package `internal/output`: schema constants, `Definition`,
    `AggregateValues`/`MetadataValues`/`RateValue`, and `Store` (latest
    values per table/key with a change sequence; `Since(seq)` for teach
    and live deltas, `Changed()` for wake-ups; unchanged values are a
    no-op). Entries are never deleted (the protocol has none; HAProxy
    expires them); the store has no size bound yet (phase 14).
  - `peersession`: `Options.Output` makes a session announce the output
    tables under local IDs 1..n (never an input table; `NewConn` refuses
    overlapping names), teach each table (definition and entries) only
    once the source has announced a matching definition of it, teach
    every such table per resync request (before "finished"), then send
    each change to those tables after a watcher goroutine interrupts the
    blocked read. Nothing is sent for a table the source has not
    announced, so a mismatched or foreign table of the same name is never
    written (review batch 1, R1-F1). Outputs therefore require
    `request_resync` (config, `sources.Start`, and `NewConn` refuse
    otherwise): stock HAProxy announces a table it never updates itself
    only while teaching. Updates always carry
    explicit IDs (3.4.6 implicit-ID defect) counted per table from 1 per
    session. Received acks are routed by the aggregator's own table IDs
    and must name an update sent in this session. The source's copy of an
    output table must match the announced definition exactly (fields and
    expiry) or the session ends with `ErrSchema` before anything is
    written to it; its updates are counted,
    acknowledged (so its cursor advances), and never delivered. New stats:
    `EchoedUpdates`, `Teaches`, `OutputUpdates`, `TaughtUpdates`,
    `AcksReceived`, per-table `Outputs`.
  - `sources`: `Manager.Publish`; `Options.Output` injects a pre-filled
    store (must match the configured outputs). `config`: optional
    `outputs` (name, kind, expire); `max_session_tables` must cover inputs
    plus outputs. htad accepts outputs but publishes nothing yet (no
    aggregation); `htalab -htad-config` writes both lab outputs.
  - Lab: with an aggregator, nodes get `lab_out`/`lab_meta`, a probe
    listener (`Node.ProbeAddr`, `Client.Probe`), and
    `Aggregator.OutputFirst`. Without an aggregator the configuration is
    unchanged, so `lab-smoke` is unaffected.
- Findings for later phases:
  - HAProxy replays remote-learned entries: `stktable_touch_remote` puts
    them in the update tree at an offset of 2^31, which incremental pushes
    skip but a teach walks. So every aggregator resync request brings the
    output back (observed: 5 of 5 entries on both versions). Phase 07 must
    keep treating it as non-input; phase 06 must not read it as progress.
  - 3.4.6 does not acknowledge updates received while it is itself
    learning (`last_get` is not recorded in `PEER_LR_ST_PROCESSING`).
    Before review batch 1, both teaches sent during its startup resync
    stayed unacknowledged (`lab_meta` never was); 3.2.25 acknowledged
    every taught update. With the gate, the tables are taught after the
    node's learning ended and both versions acknowledged everything in
    the run, but any update sent while a node learns stays
    unacknowledged on 3.4.6. Transport acks are therefore not a uniform
    delivery signal across versions, in addition to not proving
    freshness.
  - Teaches send ordinary updates, so each teach (every reconnect and
    resync request) restarts every output entry's HAProxy lifetime.
    Phase 06 must decide whether teaches use timed updates or another
    mechanism so that replay cannot renew stale output.
  - A source table without the aggregator's peers section is never
    announced, so the daemon sends it nothing; visible as
    `OutputStats.SourceID == 0` and no ack. Phase 15 should report it.
- Review fix batch 1 (independent review R1-F1, R1-F2):
  - R1-F1 (Medium): the session taught every output table at start,
    before the source's definition could be checked, and stock HAProxy
    applies an update to any table of that name with IPv6 keys whatever
    it stores. An output named like a shared non-input table (`prod_in`)
    was written into it before `ErrSchema` ended the session, on every
    retry. Now a table is sent nothing until the source announces a
    matching definition (see Decisions). Source check (both releases):
    `peer_send_teachmsgs` sends a table's definition before walking its
    entries, so a teach announces empty tables too; outside a teach a
    table is announced only when it has local updates, which an output
    table never has, hence the `request_resync` requirement. Live
    regression `TestLiveOutputNeedsMatchingTable` (both versions):
    outputs `prod_in` (input schema) and `lab_out` with a 60 s expire
    (node: 30 s) each end three or more sessions with `ErrSchema` and
    leave no entry for the published key in the node's table. Against
    the reviewed code the same test fails on 3.4.6 (`prod_in` held
    `2001:db8:e::` with `http_req_cnt:0`, `lab_out` held `gpt0:1 gpt1:5`).
    `TestLiveOutput` still delivers into empty `lab_out`/`lab_meta` on
    both versions. Fake-peer tests: nothing is sent before the
    announcement however much is published; each announcement teaches
    that table only; a re-announcement teaches nothing; mismatching
    definitions end the session with no stick-table message sent
    (`TestOutputTeachAndLive`, `TestOutputFailures`); outputs without
    `RequestResync` are refused (`TestInvalid`,
    `TestStartRejectsMismatchedStore`).
  - R1-F2 (Low): the recorded example lacked `localpeer a`, so HAProxy
    removed the peers section with only a warning. The example now sets
    `global` / `localpeer a` (with a comment on why), defaults, a bind,
    and a backend; `haproxy -c -V -dW` (zero-warning) on the exact block:
    "Configuration file is valid" on 3.4.6 and 3.2.25.
  - Follow-on fix found by stressing the new fake test: a wake-up still
    pending when a table became ready resent entries its teach had just
    sent (harmless duplicates; `TestOutputTeachAndLive` failed 25 of 200
    parallel `-race` runs). Each table now records the store sequence its
    last teach covered (`output.Entry.Seq`), and live sends skip entries
    no newer: 200 of 200 runs pass.
  - Checks: `make check` pass (`artifacts/check-05-b1.log`); `make
    lab-test` pass on 3.4.6 and 3.2.25 (`artifacts/lab-test-05-b1r2.log`);
    `make lab-smoke` ok on both (`artifacts/lab-smoke-05-b1.log`; lab
    configuration unchanged in this batch); `FuzzRun` 20 s pass.
  - `TestLiveSimultaneous` (phase 04, no outputs) failed once more on
    3.4.6 in a full `make lab-test` before the follow-on fix
    (`artifacts/lab-test-05-b1.log`, cycle 2: no session back within
    20 s), so 2 failures in 5 full runs on this branch. In isolation it
    passed 10 of 10 at baseline HEAD and 10 of 10 on this tree, and in
    six alternating full-suite runs (3 baseline, 3 this tree, 3.4.6) it
    never failed. Not reproduced on demand and not attributed to this
    phase.
- Independent review: R1 found R1-F1 and R1-F2; R2 confirmed both fixed
  with no new findings (`make check` and `make lab-test` on 3.4.6 and
  3.2.25 pass on the final tree). `TestLiveSimultaneous` passed 9 of 9
  further full-suite runs on 3.4.6 on a quiet host (including three
  `make lab-test HAPROXY_VERSIONS=3.4.6` and four `go test -race
  -count=4` runs). Both recorded failures fall in windows when unrelated
  HAProxy work was running on the same host, so host contention is the
  likeliest cause; it configures no outputs and runs none of this
  phase's code. Optional follow-up: dump the daemon log and `show peers`
  when it times out.
- Remaining work / next action: none for this phase. Phase 06 starts
  from the replay, acknowledgement, and lifetime findings above.
