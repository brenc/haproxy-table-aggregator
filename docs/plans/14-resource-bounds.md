# 14 — Resource bounds

Status: not started

Depends on: [13](13-peer-security.md).

## One-turn outcome

Demonstrate bounded storage, parsing, and publication under churn and slow
consumers, with explicit degradation instead of silently incomplete results.

## Work

- Validate limits for configured sources/tables, keys per source, total keys,
  active sessions, in-flight snapshots, parser buffers, and destination queues.
  Include temporary memory used while replacing a source snapshot.
- Make expiry incremental and bounded. Avoid whole-table copies or long scans
  on every update. Schedule rate decay without an unbounded timer per refresh.
- Coalesce superseded output by key where safe. Never discard necessary expiry,
  zero-value, or readiness transitions to stay within a queue cap.
- Define overload behavior: reject/invalidate affected state and withhold its
  authority instead of evicting unexpired contributions while claiming completeness.
  Resume only after a valid recovery path, not merely after a timer expires.
- Exercise native source-table LRU eviction and key recreation. The aggregator
  cannot assume it receives a deletion event for each source eviction; document
  how replicated TTL and full snapshots limit stale retained contributions.
- Validate configuration errors before opening listeners. Configuration changes
  requiring a restart are acceptable; hot reconfiguration is outside scope.

## Acceptance

- [ ] Repeated key churn stays within declared logical storage and queue caps.
- [ ] Partial/aborted snapshots release their temporary state.
- [ ] Expiry work and publication do not starve heartbeats or healthy peers.
- [ ] Capacity exhaustion causes visible degradation and local fallback, not a
      deceptively complete aggregate or a silent arithmetic change.
- [ ] Slow-peer reconnect and malformed-input cycles have bounded retention.
- [ ] Source LRU eviction/recreation has a regression and stated limitations.
- [ ] Memory measurements report configured caps, peak usage, and post-expiry
      retention; a single small example is not offered as proof of a bound.

## Stop and handoff

Do not optimize for an unmeasured throughput claim. Record limits and resource
ownership here; phase 18 measures performance with the frozen workload. If a
required bound cannot coexist with completeness, keep authority false and record
the unresolved tradeoff rather than deleting the guardrail.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 13.
