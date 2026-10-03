# 01 — Reproducible local lab

Status: not started

Depends on: [the agreed contract](../../README.md).

## One-turn outcome

A Go test harness starts two isolated stock HAProxy processes, sends known
traffic, inspects their local tables, and reliably cleans up. No aggregator yet.

## Work

- Create the Go module and minimal testing layout. Resolve and pin a supported
  Go toolchain using current official documentation. Keep dependencies small.
- Accept an explicit HAProxy path. Add a reproducible acquisition/build recipe
  for unmodified 3.4.6 and 3.2.25, recording source/artifact checksums, compiler,
  build flags, and `haproxy -vv`. Do not vendor the existing patched binary.
- Run on unprivileged loopback listeners, temporary directories, Unix runtime
  sockets, and a local HTTP responder. Isolate and terminate only owned processes.
- Define an IPv6-keyed input table with `http_req_cnt,http_req_rate(10s)` and
  expiry longer than two rate periods for decay tests. Request rules must use
  consistent keys for tracking and lookup. Add a 60-second test variant.
- Generate deterministic IPv4 and IPv6 client cases. A synthetic test header
  may supply keys only on the isolated lab listener; never trust it in a
  deployment example. Verify the production-style /64 masking separately.
- Count requests actually observed by the responder/harness, with unique IDs
  and no implicit retries. Do not use offered traffic as the truth when requests
  fail. Poll only small local test tables, not production-sized runtime dumps.

## Acceptance

- [ ] One documented command creates and tears down the two-node lab.
- [ ] Sending 10 requests to A and 20 to B yields local counts 10 and 20.
- [ ] Identical client keys exist independently on A and B; neither is peered
      with the other for input counters.
- [ ] Same-/64 IPv6 cases coalesce; distinct /64 and IPv4 cases remain distinct.
- [ ] The 10-second and 60-second variants validate with stock HAProxy.
- [ ] Failure mid-test leaves no owned listeners or processes behind.
- [ ] A run records exact Go/HAProxy versions and artifact identity.
- [ ] No infrastructure credentials, private repository, or running Docker
      daemon is required for the explicit-binary route.

## Stop and handoff

Do not implement the peers protocol in this phase. If acquiring/building a
stock binary is blocked, record the exact missing prerequisite; do not pass the
gate using a patched build. Later integration phases reuse this harness.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: implement this phase.
