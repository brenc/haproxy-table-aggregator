# 08 — Counter aggregation

Status: not started

Depends on: [07](07-source-snapshots.md).

## One-turn outcome

Correct diagnostic totals from current source snapshots, including repeated
updates and source entry replacement, without a claim of durable accounting.

## Work

- Sum the latest retained `http_req_cnt` contribution per source/key with checked
  wide arithmetic. Use these totals as test diagnostics, not a lifetime quota.
- Recompute or adjust the sum by replacement; never treat absolute counter
  snapshots as additive events. Source snapshot storage remains authoritative.
- Specify behavior when a count decreases: entry recreation, reset, or wrap
  cannot silently become a huge positive delta. Preserve observed state and
  expose uncertainty where continuity cannot be established.
- Build an independent trace oracle from accepted source snapshots and known
  lab requests. Do not call the production merge routine to compute expectations.
- Keep source expiry and membership changes visible in diagnostics and readiness.
  Removing a required source requires an explicit roster configuration change.

## Acceptance

- [ ] A=10, B=20, then A=11 yields 30 then 31; replaying A=11 stays 31.
- [ ] A=100 and B=100 each increment once, producing 202 after convergence.
- [ ] Repeated full snapshot records do not inflate totals.
- [ ] Distinct keys, sources, and local table-ID collisions remain isolated.
- [ ] Expired/replaced contributions disappear exactly once.
- [ ] Counter reset and 32-bit boundary cases have explicit expected behavior.
- [ ] Overflow in a sum or a proposed narrow output encoding is detected.
- [ ] Known two-node HTTP traffic matches diagnostic counts before entry expiry.

## Stop and handoff

No rate calculation or new stored data types. Document the diagnostic total's
scope so later users cannot confuse it with billing or persistent request counts.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 07.
