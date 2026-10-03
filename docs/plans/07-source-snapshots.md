# 07 — Source snapshots

Status: not started

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

- [ ] Two sources reporting one key retain two independent snapshots.
- [ ] The last accepted update replaces only its source's previous value.
- [ ] Overlapping snapshot/live updates retain the newest valid source state.
- [ ] One source completing sync cannot make the whole roster ready.
- [ ] An empty but synchronized, healthy source is distinct from a missing one.
- [ ] Expiration is deterministic under a fake clock and does not require input.
- [ ] Schema mismatches degrade/reject the affected logical table explicitly.
- [ ] No output or metadata replay is admitted into the snapshot store.

## Stop and handoff

Do not sum values yet. Record source/session identity, completion transitions,
and snapshot merge rules here. Reconnect reconciliation and process restart
qualification belong to phase 11, using this state model.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 06.
