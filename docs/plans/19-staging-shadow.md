# 19 — Staging shadow preparation

Status: not started

Depends on: [18](18-load-qualification.md).

## One-turn outcome

A reviewable staging shadow configuration and observation/rollback procedure,
validated locally. This phase prepares a trial; it does not claim a live trial
or authorize production changes.

## Work

- Prepare a parallel one-category tracking table for staging, with locally owned
  inputs and separate aggregate output/readiness. Keep existing enforcement and
  replicated Guard/affinity state intact. Check tracking-slot availability in the
  actual integration before selecting another `track-sc` slot.
- Produce generic public configuration examples with synthetic hosts and keys.
  Private Ansible integration can be prepared separately when explicitly asked;
  the public project must remain independently reproducible.
- Add observation of current decisions versus proposed aggregate decisions,
  source completeness, publication delay, fallback, CPU/memory, and false-positive
  candidates. Limit key-level evidence to what the trial actually needs.
- Validate configuration and lab behavior with shadow decisions unable to reject
  traffic. Define certificate provisioning by references, never embedded secrets.
- Write preflight, start/stop, evidence collection, rollback, and trial-completion
  steps here. Propose an observation period spanning ordinary traffic and a busy
  period; a long-running observation is not hidden inside this implementation turn.
- Define the next decision: whether observed benefit and operational cost justify
  a separately approved enforcement trial and recalibration of existing thresholds.

## Acceptance

- [ ] The prepared configuration validates against the chosen unmodified version.
- [ ] Parallel tracking does not overwrite another sticky counter slot or change
      existing routing, Guard state, session affinity, or enforcement.
- [ ] Aggregate decisions can be observed but cannot deny live traffic in shadow mode.
- [ ] Required metrics and comparison evidence answer whether aggregation changes
      useful abuse decisions rather than merely producing different numbers.
- [ ] Rollback removes the shadow integration without relying on the aggregator.
- [ ] The runbook identifies required access, target scope, stop conditions, and
      who authorizes the eventual staging trial.
- [ ] Preparation results are clearly separated from unperformed live validation.

## Stop and handoff

Do not deploy, publish a release, or start production enforcement as part of this
phase. A later explicitly requested staging trial can execute this runbook and
record its evidence in this file. Completing preparation does not complete that
trial; keep separate trial checkboxes below and do not use them as a release
claim until measured evidence exists.

## Later live trial record

- [ ] Staging trial explicitly requested and its target scope recorded.
- [ ] Check mode/preflight passed before application in the deployment repository.
- [ ] Shadow observation completed and evidence collected.
- [ ] Decision differences and potential false positives reviewed.
- [ ] Continue/adjust/stop decision and any enforcement authorization recorded.

## Execution record

- Preparation commands and versions: not run.
- Preparation acceptance evidence: none yet.
- Live trial: not requested or performed.
- Decisions or deviations: none.
- Remaining work / next action: prepare after phase 18.
