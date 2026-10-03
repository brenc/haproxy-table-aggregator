# 10 — Publication and enforcement

Status: not started

Depends on: [09](09-rate-evaluation.md), using the scheme proven in
[06](06-freshness-gate.md).

## One-turn outcome

The complete one-table path receives real traffic, publishes evaluated rates,
and drives local HAProxy decisions without a per-request aggregator call.

## Work

- Connect the source store/evaluator to the ordinary output publisher. Coalesce
  superseded output updates per key without losing required zero/expiry effects.
- Maintain separate bounded destination queues, send-time encodings, update IDs,
  and progress. Keep heartbeats and recovery controls responsive under load.
- Apply the phase-06 freshness design to real results. Never renew authority
  while source completeness or destination data progress violates the contract.
- Enforce the local limit at all times and the global limit when authoritative.
  Both use the full configured threshold; fallback does not divide by node count.
- Produce complete validated lab/example configurations: isolated raw inputs,
  separately named aggregate outputs, metadata, consistent keys, and ACLs.
- Separate missing aggregate entries from incomplete publication. A missing key
  may be zero only when the proven readiness scheme permits that interpretation.

## Acceptance

- [ ] Each node stays below a chosen local threshold while the combined rate
      crosses it; both nodes return 429 after measured propagation.
- [ ] A single node exceeding its local limit is protected even before a global
      update arrives and while global data is unavailable.
- [ ] Stopping traffic eventually removes the aggregate denial through decay.
- [ ] No aggregate value is counted back into the input or incremented locally.
- [ ] A missing/unready aggregate selects local protection with the full limit.
- [ ] One slow destination does not stall a healthy destination or controls.
- [ ] End-to-end timings include HAProxy observation, not just Go queue insertion.

## Stop and handoff

This is the first functional end-to-end milestone, not a deployment approval.
Record overshoot and convergence measurements; do not claim a strict global
quota. Keep the configurations alongside the implementation and cite them here.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 09.
