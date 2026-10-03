# 16 — Failure qualification

Status: not started

Depends on: [15](15-observability.md).

## One-turn outcome

One command runs the bounded correctness/failure scenarios against real HAProxy
and produces machine-readable results plus a concise human summary.

## Work

- Assemble existing phase tests into a deterministic qualification runner rather
  than rewriting their logic. Include a fixed random seed and exact manifests.
- Exercise both two-node and four-node topology for the relevant scenarios.
  Use a local TCP fault relay for loss of progress, half-open connections,
  fragmentation, and delayed delivery without root privileges.
- Cover counter replay, offset windows, idle decay, entry expiry, aggregator
  kill/pause/restart, source cold restart, graceful reload, interrupted lessons,
  TLS failures, and queue/capacity exhaustion.
- Include failure during failure: HAProxy reload with the aggregator down, and
  resynchronization while a destination is slow. Prioritize authority correctness.
- Measure decisions through HAProxy requests. Define the start/end of each
  detection interval and report measurement resolution and any uncertainty.
- Preserve small synthetic failing traces and phase-appropriate logs. Large
  generated reports go under ignored `artifacts/` with paths recorded below.

## Acceptance

- [ ] The runner exits nonzero on a violated invariant, timing bound, or missing
      required scenario; timeouts and skips cannot become passes.
- [ ] Publication stop invalidates authority within 2 seconds, including the
      queued/replayed readiness and reload scenarios from phase 06.
- [ ] Silent source failure causes fallback within 10 seconds.
- [ ] No replay/reload scenario doubles inherited contributions.
- [ ] Live-but-backlogged sessions cannot keep stale output authoritative.
- [ ] Healthy destinations continue when one destination stalls.
- [ ] Runs include exact binary hashes, configuration, seed, and request counts.
- [ ] Owned test processes and files are cleaned up on success and failure.

## Stop and handoff

This phase assembles and qualifies existing behavior. If a defect requires a
substantial redesign, record the failing case and a bounded repair handoff here;
do not combine an unbounded redesign and qualification into this turn.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 15.
