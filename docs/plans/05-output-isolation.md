# 05 — Output tables and isolation

Status: not started

Depends on: [04](04-peer-sessions.md).

## One-turn outcome

Prove that a normal Go peer can write a known integer into a distinct stock
HAProxy output table, and cannot accidentally feed it back as an input.

## Work

- Give each proxy its own input table and a separately named output table in
  its dedicated relationship with the aggregator. Keep other peer sections out
  of this experiment. Add metadata-table scaffolding if needed for phase 06.
- Publish known integer values through the ordinary peers protocol, including
  a full teach and subsequent live updates. No Runtime API writes as the output
  transport; runtime reads are test observations only.
- Specify the initial field layout, units, schema version, and numeric limits
  here. General-purpose tags/arrays are candidates; validate the exact form on
  both upstream versions. Reserve metadata without claiming freshness yet.
- Add an HAProxy ACL/test response that proves the ordinary fetch sees the value.
- Treat incoming output/metadata definitions and replay as non-contributing
  state. Never send input-table updates from the aggregator.

## Acceptance

- [ ] Go-published values are visible through stock HAProxy ACLs on both nodes.
- [ ] A live change appears without a reconnect or full synchronization.
- [ ] Publishing output leaves each node's input count unchanged.
- [ ] Received/replayed output records never change source contributions.
- [ ] Source table-ID collisions do not misroute output or acknowledgments.
- [ ] HAProxy configuration validates without a patch, module, or protocol change.
- [ ] The supported output schema and example lookup are recorded here.

## Stop and handoff

This is the first feasibility gate. If stock HAProxy cannot receive the chosen
ordinary fields, investigate the mapping and wire behavior before continuing.
Do not substitute a custom HAProxy build or an external per-request service.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 04.
