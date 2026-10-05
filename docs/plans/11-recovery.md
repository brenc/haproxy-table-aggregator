# 11 — Restart and reconnect recovery

Status: not started

Depends on: [10](10-publication-enforcement.md).

## One-turn outcome

Recover a restarted aggregator and interrupted source sessions without treating
replay as new traffic or claiming completeness before every source is restored.

## Work

- Kill the aggregator, retain live HAProxy input tables, restart with empty Go
  memory, and request snapshots from every configured source.
- Keep authority unavailable while rebuilding. Reconcile concurrent updates
  during teaching, and reconcile absent keys only when snapshot completeness
  justifies doing so. Define bounded retention of replaced session state.
- Exercise an interrupted teach, lost acknowledgments, repeated reconnects,
  duplicate connections, and same-key updates near entry expiry.
- Distinguish silence from failure using protocol liveness plus application
  progress. Test an entirely quiet but connected source across several windows.
- Treat a source cold restart/reset as a potential history discontinuity.
  Define what the protocol can actually detect and when confidence is restored;
  PID changes alone are not proof of successful or failed handover.
- Do not add a disk database, write-ahead log, consensus, or standby process.
- Carried in from phase 07: keys absent from a later session's finished
  snapshot keep the earlier session's value (tagged `Entry.Session`) until
  their local expiry, and count against `max_source_entries` meanwhile; a
  source near the cap that reconnects with a changed key set is refused
  mid-teach and stays degraded until they expire. Reconcile them here.
- Carried in from phase 08: held-over prior-session entries are counted
  and flagged `aggregate.Contribution.HeldOver` (total `Uncertain`) until
  reconciled here. A key that expired or was evicted at the source and was
  recreated at a count at or above the stored one is indistinguishable from
  continuous counting and is not flagged.

## Acceptance

- [ ] An empty aggregator recovers all retained expected snapshots without 2x totals.
- [ ] All required sources must synchronize before aggregate authority returns.
- [ ] Requests arriving during recovery are reconciled without replay inflation.
- [ ] Expired keys do not reappear or acquire a new full lifetime from replay.
- [ ] A healthy quiet source remains ready; a silent partition triggers local
      fallback within 10 seconds, including destination lease behavior.
- [ ] Cold source restart behavior and any unavoidable detection limits are
      documented and tested; lost history is not called recovered.
- [ ] Repeated reconnection has bounded memory and one contribution per source.

## Stop and handoff

Record ambiguity that the native protocol cannot resolve as a limitation or a
blocked contract gate. Do not guess source continuity or relabel partial state
as complete to get a green test. Graceful reload is qualified separately next.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 10.
