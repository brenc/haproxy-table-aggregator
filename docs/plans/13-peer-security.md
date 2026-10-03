# 13 — Mutual TLS and peer authorization

Status: not started

Depends on: [12](12-reload-continuity.md).

## One-turn outcome

Authenticated peer sessions with explicit source authorization and generated
test certificates; the first supported path beyond a loopback-only lab.

## Work

- Add Go TLS server/client handling and matching stock HAProxy peer configuration.
  Verify chains and expected identities in both directions.
- Bind each allowed certificate identity to permitted logical peer names.
  Possession of any certificate signed by a trusted CA is not sufficient to
  impersonate another configured source.
- Generate short-lived disposable CAs and certificates in temporary test storage.
  Configuration points to key files; do not embed credentials in examples or logs.
- Reject missing/expired/untrusted certificates, identity mismatches, unexpected
  source names, and attempts to replace another source using the wrong identity.
- Keep the plaintext escape hatch explicit and restricted to isolated lab use.
  Document certificate replacement and restart-based reload for this version.
- Re-run a narrow end-to-end count, heartbeat, and reconnect smoke over mTLS.

## Acceptance

- [ ] Authorized mTLS peers exchange input and output on stock HAProxy.
- [ ] Both sides reject failed certificate validation without insecure fallback.
- [ ] A trusted certificate for source A cannot claim source B's identity.
- [ ] Certificate rejection leaves other sources intact and readiness accurate.
- [ ] Reconnection after certificate replacement follows the normal recovery path.
- [ ] Tests generate credentials; tracked examples contain only file references.
- [ ] Non-loopback plaintext deployment is rejected or explicitly unsupported by
      validated configuration, never silently enabled by default.

## Stop and handoff

No secret-manager integration or automatic certificate authority. Document the
certificate identity policy and exact examples here; private deployment material
belongs outside the public repository.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 12.
