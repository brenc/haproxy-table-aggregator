# 15 — Operational visibility

Status: not started

Depends on: [14](14-resource-bounds.md).

## One-turn outcome

An operator can distinguish a healthy idle service, recovery, missing sources,
stale publication, and overload without inspecting client-level table contents.

## Work

- Add structured lifecycle/error logs with source, logical table, session, and
  reason codes. Keep request keys and certificate/private-key contents out of
  routine logs. Avoid per-update logging on the hot path.
- Expose bounded metrics for source connectivity/synchronization, snapshot age,
  retained entries, rejected schemas, reconnects, queue depth, publication lag,
  authority state, lease expiry, capacity failures, and process resource use.
- Limit labels to configured source/table/destination identities and finite
  reason enums. Never use IP keys, update IDs, or session generations as labels.
- Distinguish process liveness, global source completeness, and per-destination
  publication readiness. Document how each affects HAProxy authority.
- Bind diagnostics to loopback by default. Add a bounded, useful status surface;
  avoid building a general table-query API or dashboard.
- Add a concise operational runbook in this phase file: startup, graceful stop,
  certificate replacement, interpreting degradation, and collecting a reproducer.

## Acceptance

- [ ] Idle healthy sources remain visibly healthy with zero entry updates.
- [ ] Restart, partition, bad schema, and slow destination have distinct reasons.
- [ ] Metrics/log volume does not scale with unique client-key cardinality.
- [ ] Queue and lease measurements explain why a destination lost authority.
- [ ] Diagnostic endpoints reveal no credentials or unbounded table contents.
- [ ] Tests verify representative status transitions and metric-label bounds.
- [ ] An operator can follow the runbook without this conversation or private tools.

## Stop and handoff

No hosted monitoring, alert delivery, UI, or production deployment. Export enough
evidence to support the qualification phases and later monitoring integration.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 14.
