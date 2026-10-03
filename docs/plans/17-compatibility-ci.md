# 17 — Upstream compatibility and CI

Status: not started

Depends on: [16](16-failure-qualification.md).

## One-turn outcome

A reproducible local/CI matrix qualifies the explicitly supported upstream
versions and makes regression results inspectable by outside contributors.

## Work

- Run the public suite against pinned unmodified HAProxy 3.4.6 and 3.2.25 with
  recorded build options/digests. Cover plaintext loopback and the required mTLS
  path, plus a bounded multithreaded reload smoke.
- Record supported combinations only when all required gates pass. Keep patched
  experiments in a separately labeled optional comparison outside support results.
- Add CI that runs phase 00's `make check` and `make fuzz-smoke` plus the
  practical upstream integration matrix. Heavy load tests may be an explicit
  qualification job rather than every push.
- Pin CI dependencies and artifact provenance using current documentation.
  Keep permissions minimal and avoid secrets for the public local/CI test route.
- Add local commands matching CI and contribution instructions in this phase
  file, with pointers to implementation/test directories once they exist.
- Check original-code/fixture provenance and the MIT license before preparing
  public artifacts. No release publication or remote-repository creation here.

## Acceptance

- [ ] Each upstream target has independent results for all required contracts.
- [ ] A patched binary cannot accidentally enter the upstream matrix.
- [ ] An outside contributor can reproduce the suite from documented prerequisites.
- [ ] CI and local commands use the same pinned compatibility inputs.
- [ ] Required-test skips, unsupported releases, and known defects are explicit.
- [ ] CI configuration is validated locally; remote-run evidence is distinguished
      from configuration validation if no public remote exists yet.
- [ ] Reports and short reproductions make failures actionable without private data.

## Stop and handoff

A known upstream failure blocks a support claim. Choose another unmodified
release or propose a contract change explicitly; do not silently require custom
patches. Running local checks does not imply that a remote CI run has occurred.

## Execution record

- Commands and versions: not run.
- Acceptance evidence: none yet.
- Decisions or deviations: none.
- Remaining work / next action: start after phase 16.
