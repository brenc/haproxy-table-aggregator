# 12 — Reload continuity gate

Status: not started

Depends on: [11](11-recovery.md).

## One-turn outcome

Establish the actual continuity and freshness guarantees across stock HAProxy
reloads using a bounded, repeatable experiment under traffic.

## Work

- Extend the local harness to perform real master-worker reloads while known
  traffic continues. Keep the logical source name stable across worker PIDs.
- Test one-node reload, repeated reloads, and simultaneous two-node reloads.
  Add a small unrelated peer section to catch cross-section lifecycle effects.
- Keep an old-worker connection open and send requests where the old worker
  still accepts them; account for its draining behavior explicitly.
- Pace a local handover long enough to cross normal peer timer intervals.
  Capture the earliest missing/duplicated request or stalled-control evidence.
- Reload while the aggregator is unavailable and while output is backlogged.
  Reapply the phase-06 tests against genuine worker handover.
- Run pinned unmodified upstream targets separately. A patched local binary
  may help isolate an upstream defect but cannot satisfy this gate.

## Acceptance

- [ ] Old/new PIDs are never summed as independent source contributions.
- [ ] In successful handovers, non-expired count fixtures and known traffic retain
      continuity without duplicated inherited state.
- [ ] Live rate/output behavior follows the defined recovery and freshness rules.
- [ ] An outage reload cannot revive a stale lease or stale aggregate authority.
- [ ] Slow handover and old-worker traffic have explicit measured outcomes.
- [ ] Each upstream target has a separate result and smallest useful reproduction
      for any failure; no silent skips are called support.

## Stop and handoff

Stop on an unresolved continuity defect. This phase is intentionally an
experiment-sized turn: an upstream bug report or a proposed contract/version
change may be the handoff. Do not invent a durable source epoch the protocol
does not carry, require custom patches, or waive the gate without an owner
decision. Local reproduction does not authorize publishing an upstream report.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 11.
