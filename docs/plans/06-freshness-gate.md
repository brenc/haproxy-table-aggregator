# 06 — Freshness feasibility gate

Status: not started

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

- [ ] With the process killed or paused, aggregate authority ends locally within
      2 seconds of the last valid publication, measured by request decisions.
- [ ] Buffered or replayed markers cannot extend that authority indefinitely or
      grant a fresh full lease to data already outside the allowed age bound.
- [ ] HAProxy reload/resync during an outage does not revive stale authority.
- [ ] A live marker cannot conceal a backlog of stale aggregate entries.
- [ ] Missing metadata and incomplete output select local protection.
- [ ] Healthy repeated requests do not refresh the readiness lease themselves.
- [ ] The scheme runs on unmodified upstream binaries with documented clocks,
      timers, field widths, generation behavior, and configuration assumptions.

## Stop and handoff

This phase may end with a **blocked feasibility finding**, not a finished
implementation. Keep the smallest failing reproduction and explain whether
stock configuration can meet the contract. Do not weaken the 2-second target,
invent a protocol extension, or require patches without an owner decision.
Proceed to the aggregation engine only after this gate has a supported design.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 05.
