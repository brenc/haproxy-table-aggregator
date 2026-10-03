# 01 — Reproducible local lab

Status: complete

Depends on: [00](00-dev-tooling.md).

## One-turn outcome

A Go test harness starts two isolated stock HAProxy processes, sends known
traffic, inspects their local tables, and reliably cleans up. No aggregator yet.

## Work

- Create the minimal testing layout on the phase 00 module and toolchain.
  Keep dependencies small.
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

- [x] One documented command creates and tears down the two-node lab.
- [x] Sending 10 requests to A and 20 to B yields local counts 10 and 20.
- [x] Identical client keys exist independently on A and B; neither is peered
      with the other for input counters.
- [x] Same-/64 IPv6 cases coalesce; distinct /64 and IPv4 cases remain distinct.
- [x] The 10-second and 60-second variants validate with stock HAProxy.
- [x] Failure mid-test leaves no owned listeners or processes behind.
- [x] A run records exact Go/HAProxy versions and artifact identity.
- [x] No infrastructure credentials, private repository, or running Docker
      daemon is required for the explicit-binary route.

## Stop and handoff

Do not implement the peers protocol in this phase. If acquiring/building a
stock binary is blocked, record the exact missing prerequisite; do not pass the
gate using a patched build. Later integration phases reuse this harness.

## Execution record

- Commands and versions (2026-10-03, Debian 13 amd64, kernel 6.12.107):
  - Go `go1.27.1` (repo-pinned toolchain); gcc/g++ 14.2.0, cmake 3.31.6,
    perl 5.40.1.
  - `make haproxy` (`scripts/build-haproxy.sh`): stock, unpatched, no
    EXTRAVERSION. Sources verified against haproxy.org's `.sha256` files and
    the downloaded tarballs:
    - 3.4.6 `791e1815f8af6e8b850a227a9a0a190f3d3478c9e8d38a0f51c98b7f4bfe368b`,
      binary `81df335a2e181be99d5841637cdff048648c3d202ec84c1d6f6b6373dc47d1c4`
      (`3.4.6-56332c5`).
    - 3.2.25 `d59a68d0daef7b5c596b019b742089788ff1748513ef96e71fe7b3943577866e`,
      binary `5f89a725d9401d0fc3b13f9e490a3a2501f276fce39ad2038e9b63307b428869`
      (`3.2.25-70469d3`).
    - AWS-LC 5.11.0 (GitHub tag tarball
      `8cb24c6e6be1fa7ff05075c4560ca8b537a7ef48f9e6f465af4ea455794d74f4`),
      static, Release, PIC, no tests/tools; built once and shared.
    - Flags: `TARGET=linux-glibc USE_OPENSSL_AWSLC=1 USE_QUIC=1 USE_LUA=1
      USE_PCRE2=1 USE_PCRE2_JIT=1 USE_PROMEX=1 LUA_LIB_NAME=lua5.4`,
      `SSL_INC`/`SSL_LIB` at the AWS-LC prefix,
      `DEBUG='-DDEBUG_STRICT -DDEBUG_STRICT_ACTION'`,
      `CFLAGS='-fstack-protector-strong -D_FORTIFY_SOURCE=3 -fPIE'`,
      `LDFLAGS='-pie -Wl,-z,relro,-z,now'`. Both releases built with these
      flags unchanged; `-vv` reports `Running on SSL library version :
      AWS-LC 5.11.0` for both.
    - Per-version `build-info.txt`, `haproxy-vv.txt`, and `build.log` live in
      `artifacts/haproxy/<version>/`. Rebuilding 3.2.25 twice at the same
      path gave the same binary digest.
  - `make lab-test`: both versions, `go test -race -count=1 -v
    ./internal/lab/...`, all pass; 16 run records in `artifacts/lab-runs/`
    (after review fix batch 1).
  - `make lab-smoke` (3.4.6 and `HAPROXY_BIN=artifacts/haproxy/3.2.25/haproxy`)
    printed node a `http_req_cnt=10 responder_observed=10`, node b `20`/`20`,
    `smoke ok`, `lab torn down`. `htalab` with `-period 60s` stopped both
    nodes on SIGINT.
  - `make check` passes without `HTA_HAPROXY` (lab tests skip with a message
    naming the variable).
- Acceptance evidence:
  - One command: `make lab-smoke` creates the lab, checks the counts, and tears
    it down; `make lab` runs it until Ctrl-C. Both run `cmd/htalab`.
  - 10/20: `TestTwoNodeIndependentCounts` sends 10 requests to A and 20 to B
    with the same client key. The tables show 10 and 20, the responder saw 10
    and 20 distinct request IDs, and the post-track lookup header matches.
  - Independence: the same key exists separately on A and B with different
    counts. Configs contain no `peers`, `show peers` is empty, and
    `TestLabKeyCases` finds no entries on B after loading A only.
  - Keys: same-/64 IPv6 clients coalesce to `2001:db8:1:1::`; a different
    /64 stays separate; IPv4 is stored as `::ffff:a.b.c.d` and distinct
    IPv4 /32s stay distinct. The production key `src,ipmask(32,64)` is
    verified separately on two listeners:
    - `TestProductionMasking`: real loopback sources (127.0.0.2, 127.0.0.3,
      ::1); a spoofed synthetic header is ignored.
    - `TestProxyProtocolMasking`: a PROXY-protocol listener (`accept-proxy`,
      test-only trust confined to that loopback listener) running the same
      `src` fetch and key rule. Native IPv6 sources 2001:db8:1:1::5 and
      2001:db8:1:1:ffff:ffff:ffff:9 coalesce to `2001:db8:1:1::`;
      2001:db8:1:2::1 stays separate; IPv4 sources (TCP4 and IPv4-mapped
      TCP6) are stored as `::ffff:a.b.c.d`. 6to4 sources in two /64s of
      `2002:c000:20a::/48` both key as `192.0.2.10`. PROXY headers sent to
      the prod listener are rejected, and plain requests on the PROXY listener
      are refused; neither is tracked.
  - Variants: every integration test runs at 10s and 60s on both versions.
    It checks `http_req_rate(<ms>)` equals the count, and that `exp` falls in
    (2, 3] periods (expiry = 3 periods).
  - Cleanup: `TestCleanupAfterMidTestFailure` re-runs the test binary as a
    child that starts a lab and serves traffic. The child then either calls
    `t.Fatal` (cleanup runs) or is SIGKILLed (no cleanup). In `fatal` mode,
    a cleanup that runs after the lab's `Close` and before the child exits
    checks that the PIDs are gone and that listeners and sockets refuse
    connections. Without that ordering, the parent-death signal could hide a
    broken `Close`. The parent fails on a `close lab:` error or missing
    post-close confirmation. In both modes it then checks the PIDs, every
    TCP listener and runtime socket, and (for `fatal`) that the lab
    directory was removed. `TestCloseStopsNodes` checks the same thing in
    process immediately after `Close` returns. Negative controls: disabling
    the parent-death signal fails `hang`. A mutant whose `Node.stop` returns
    at once fails `TestCloseStopsNodes` and the `fatal` case.
  - Identity: each lab records Go version, GOOS/GOARCH, the resolved HAProxy
    path, its SHA-256, the `-vv` version and full text, and the adjacent
    `build-info.txt`. When `HTA_HAPROXY_VERSION` is set, the test fails on a
    version mismatch or if the binary's digest differs from `build-info.txt`.
  - Explicit-binary route: `HTA_HAPROXY`/`HAPROXY_BIN` accept any stock binary.
    It needs no Docker, credentials, Bitwarden, or Ansible repository.
- Decisions or deviations:
  - Listeners are sockets the harness opens and passes to HAProxy as
    `bind fd@N`, so no port is probed and then released. The parent closes
    its copies after the fork. HAProxy runs in the foreground (`-db`) in its
    own process group with an empty environment. On Linux it is started from
    a locked OS thread with `Pdeathsig: SIGKILL`. Teardown signals only owned
    PIDs: SIGTERM, then SIGKILL after 5s.
  - Lab listener key: `req.hdr_ip(X-Lab-Client),ipmask(32,64)`, honored only
    on the lab frontend; requests without a valid header get 400 and are not
    forwarded. Production-style key: `src,ipmask(32,64)`. Each frontend stores
    its key once in `txn.lab_key` and uses it for tracking, lookup, and the
    forwarded header. Separate tables (`lab_in`, `prod_in`, `proxy_in`) keep
    the three paths apart.
  - Traffic truth is the responder: each request carries a unique ID, the
    client disables keep-alive (so the Go transport never replays a request),
    HAProxy uses `retries 0`, and tests fail on duplicate or missing IDs.
  - HAProxy first binds a stats socket as `<path>.<pid>.tmp`, which must fit
    in the 108-byte `sun_path`. With Linux's 7-digit PID ceiling (4194304),
    the safe path limit is 95 bytes. HAProxy's own message says "usually
    97", which assumes shorter PIDs. The lab uses short `os.TempDir()`
    directories and rejects longer paths before starting anything.
  - The recipe builds against AWS-LC instead of system OpenSSL, mirroring
    the owner's production flags without patches. The flags are explicit,
    not auto-detected. AWS-LC's Go step uses the repo-pinned toolchain with
    `GOCACHE`/`GOMODCACHE` inside the build tree. The work directory is fixed
    so `__FILE__` strings, and therefore the binary digests, are stable.
  - Finding (both versions, source-confirmed in `sample_conv_ipmask` →
    `c_ipv62ip` → `v6tov4`): `ipmask` converts IPv4-mapped (`::ffff:0:0/96`),
    IPv4-compatible (`::/96`), and 6to4 (`2002::/16`) addresses to the
    embedded IPv4 before masking. 6to4 clients are therefore keyed per
    embedded IPv4 /32: a whole /48 coalesces, and it shares a key with that
    native IPv4 address. `::1` keys as `::ffff:0.0.0.1`, so the loopback
    production test cannot show /64 masking of a native IPv6 source; the
    PROXY-protocol listener does, through the real `src` fetch, and also
    shows the 6to4 behavior. The tests assert this behavior; whether it is
    acceptable for production keying is an owner decision.
- Review: independent review of the full change found two medium defects
  (cleanup test could not detect a no-op `Close`; production `src` masking
  never saw a native IPv6 source) and one low (socket path limit two bytes
  loose). All three were fixed in one batch and confirmed by the reviewer
  with mutants. `GOOS=darwin` and `GOOS=freebsd` `go vet ./...` pass.
- Rebuild check (2026-10-03): `make haproxy` rerun in place after review
  reproduced both binary digests exactly (3.4.6 `81df335a…d1c4`, 3.2.25
  `5f89a725…8869`). It reused the existing AWS-LC install, so it covers the
  HAProxy step, not a from-scratch AWS-LC build.
- Owner decision pending (does not block this phase): whether 6to4 and
  IPv4-compatible sources sharing a key with the embedded IPv4 address is
  acceptable for production keying with `src,ipmask(32,64)`.
- Remaining work / next action: start phase 02. Later phases reuse
  `internal/lab` and `internal/lab/labtest`.
