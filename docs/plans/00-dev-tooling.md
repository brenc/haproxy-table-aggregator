# 00 — Development tooling

Status: complete

Depends on: [the agreed contract](README.md).

## One-turn outcome

A Go module with a pinned toolchain, pinned lint and vulnerability tools, and
one local gate (`make check`) that every later phase runs before claiming
acceptance. No application behavior.

## Work

- Create the module and pin the current stable Go toolchain in `go.mod`, so
  any Go 1.21+ install downloads the exact version via `GOTOOLCHAIN=auto`.
- Pin golangci-lint v2 and govulncheck with `go tool`, each in its own
  modfile under `tools/` so their dependencies never touch the project's.
- Configure formatting (gofumpt, goimports, dprint for Markdown) and linters
  suited to a network-facing protocol parser with checked arithmetic.
- Provide `make` targets that phase 17's CI will call unchanged.
- Add agent/contributor instructions, `.editorconfig`, and ignore rules.

## Acceptance

- [x] `go version` inside the repo reports the pinned toolchain regardless of
      the locally installed Go.
- [x] Tools run without global installation and at pinned versions.
- [x] `make check` passes on the clean tree.
- [x] Seeded violations fail the gate: formatting, unchecked error, missing
      doc comment on an exported function, unchecked integer narrowing, and
      an unaligned Markdown table.
- [x] `make fuzz-smoke` discovers and runs a fuzz target for a bounded time.
- [x] Agent instructions name the gate and the tool-pinning rules.

## Execution record

- Commands and versions: Go `go1.27.1` (go.mod: `go 1.27.0`,
  `toolchain go1.27.1`), bootstrapped from a local go1.24.6 via
  `GOTOOLCHAIN=auto`. golangci-lint `v2.14.0` (`tools/golangci-lint.mod`),
  govulncheck `v1.8.0` (`tools/govulncheck.mod`). dprint `0.59.0`
  (`package.json`, `bun.lock`) with markdown plugin `0.25.0` pinned by
  SHA-256 in `dprint.json`. Run on Debian 13, amd64.
- Acceptance evidence: `make check` exits 0 (lint `0 issues`, govulncheck
  `No vulnerabilities found`). A temporary `internal/seed` package produced
  a gofumpt diff plus errcheck, gosec G115, and revive `exported` findings;
  `make fuzz-smoke FUZZTIME=2s` ran `FuzzExported` and passed. A seeded
  unaligned Markdown table failed `make fmt-check`. Seeds were removed and
  `make check` passed again. The first `make fmt` aligned the existing tables
  in the plan docs; no prose changed.
- Decisions or deviations:
  - Tool modfiles are separate, per golangci-lint's guidance that its
    dependencies must not mix with a project's. They are not tidied, since
    tidy would add test-only sums to golangci-lint's resolved set.
  - A root `doc.go` holds package documentation so lint has a package to
    load; phase 01 adds the real layout.
  - Recommended host Go: Debian `golang-go` from trixie-backports, used only
    as a bootstrap; `go.mod` decides the build toolchain. Host provisioning
    lives outside this repository.
  - dprint matches the owner's other repositories. It is installed through
    bun with a frozen lockfile, making bun the one non-Go prerequisite.
  - CI workflow files stay in phase 17.
- Remaining work / next action: start phase 01.
