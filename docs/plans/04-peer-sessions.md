# 04 — Peer sessions

Status: not started

Depends on: [03](03-table-messages.md).

## One-turn outcome

A minimal Go daemon establishes ordinary peers-protocol sessions with the two
lab proxies, exchanges controls, and exposes validated table events.

## Work

- Add a small CLI and strict configuration for local identity, listen address,
  explicit source names/addresses, and table mapping. Use a narrow documented
  configuration surface; environment discovery is outside scope.
- Implement handshake/version negotiation for the selected protocol, both
  connection directions, heartbeats, timeouts, acknowledgment events, and the
  required resynchronization controls. Track protocol states explicitly.
- Resolve duplicate/colliding connections deterministically so one logical
  source is never treated as two contributors. Bound reconnection backoff.
- Separate wire/session handling from source storage. Do not acknowledge an
  update that was rejected by the bounded application event path.
- Permit plaintext only through an explicit isolated-lab setting. No supported
  non-loopback deployment until phase 13 adds mutual TLS.
- Handle cancellation and SIGTERM without leaking goroutines or sockets.

## Acceptance

- [ ] Two stock HAProxy instances establish and retain sessions while idle.
- [ ] Handshakes reject unexpected identities and unsupported protocol versions.
- [ ] Heartbeat-only traffic keeps a quiet source live.
- [ ] Both connection directions and simultaneous connection attempts work.
- [ ] Closing/reopening a connection produces one logical source identity.
- [ ] Unknown controls or malformed frames fail the affected session cleanly.
- [ ] Shutdown and a bounded race-detector integration run pass.

## Stop and handoff

No aggregation, persistent state, or production listener. Session control may
request/observe synchronization, but phase 07 owns snapshot completeness.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 03.
