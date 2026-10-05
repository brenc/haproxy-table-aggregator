# 09 — Rate evaluation

Status: not started

Depends on: [08](08-counter-aggregation.md).

## One-turn outcome

Evaluate each source's HAProxy rate estimate at a common time and sum the
results, including different window phases and idle decay.

## Work

- Write down the estimator's units, rounding, low-rate behavior, age conversion,
  rollover, and empty-window behavior from pinned upstream references. Implement
  original Go code from that behavioral specification and fresh observations.
- Convert received wire age to a local monotonic basis and advance it as time
  passes. State the transport-delay approximation; do not promise an exact
  reconstruction of event arrival times that the wire does not contain.
- Define the sum as the sum of individually evaluated native integer estimates
  at the same time. Account explicitly for any upstream version differences.
- Inject a clock into the evaluator. Arrange sources with equal periods but
  different starting phases and histories; adding their raw counters is invalid.
- Schedule the next required reevaluation even without incoming updates. Define
  a bounded publication cadence/error budget for changing integer rates.
- Compare the independent specification and source-wise local HAProxy readings
  at controlled times. Use timestamped requests to quantify estimation error,
  not to assert equality to an exact sliding-window log.
- Carried in from phase 08: read source entries through
  `snapshot.Store.KeyView`, which returns the roster and the key's entries
  under one lock, and sum only sources the roster reports Ready, as
  `aggregate.Count` does. A rate built from a syncing, degraded, or
  disconnected source must not be presented as complete.

## Acceptance

- [ ] Equal and deliberately offset windows produce the specified aggregate.
- [ ] One-event, burst, steady, empty, and multi-period-idle cases match the oracle.
- [ ] Integer rounding and native low-rate correction are explicitly tested.
- [ ] Rates decay to zero without new peer updates and without resurrecting keys.
- [ ] 10-second and 60-second periods pass the same semantic cases.
- [ ] Delayed messages and clock advancement have documented error behavior.
- [ ] Mismatched periods fail schema validation; no silent conversion occurs.
- [ ] The evaluated result fits the chosen output field or invalidates authority.

## Stop and handoff

Do not synthesize native `http_req_rate` output fields. If upstream estimators
differ, preserve a reproducible comparison and settle an explicit supported
behavior before publication. Treat rounding and delay as part of the contract.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 08.
