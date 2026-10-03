# Agent instructions

MIT-licensed Go service that aggregates HAProxy stick-table counters over the
peers protocol. Read `docs/plans/README.md` (the contract) and the current
phase file before editing code. Record progress in that phase file, and tick
its box in the `docs/plans/README.md` checklist in the same change that marks
the phase `complete`.

## Toolchain

- `go.mod` pins the exact Go toolchain. Any installed Go 1.21+ with
  `GOTOOLCHAIN=auto` (the upstream and Debian default) downloads it on first
  use. Do not change the `toolchain` line to match a local install.
- golangci-lint and govulncheck are pinned in `tools/*.mod` and run through
  `go tool`. Never install them globally or add them to the main `go.mod`,
  and never hand-edit or `go mod tidy` the tool modfiles; use
  `make tools-update` and review the diff.
- dprint is pinned in `package.json`/`bun.lock` and its Markdown plugin by
  checksum in `dprint.json`. `make` installs it with
  `bun install --frozen-lockfile`; bun is the only non-Go prerequisite.

## Commands

| Command                  | Purpose                                                              |
| ------------------------ | -------------------------------------------------------------------- |
| `make check`             | Required gate: format, tidy, lint, race tests, vuln scan             |
| `make fmt`               | Apply gofumpt, goimports, and dprint (Markdown)                      |
| `make test`              | Unit tests without the race detector                                 |
| `make fuzz-smoke`        | Fuzz each target `FUZZTIME` (10s), minimize `FUZZMINIMIZETIME` (1s)  |
| `make haproxy`           | Build pinned stock HAProxy 3.4.6 and 3.2.25 into `artifacts/haproxy` |
| `make lab-smoke`         | Create the two-node lab, check 10/20 counts, tear it down            |
| `make lab`               | Run the two-node lab until Ctrl-C                                    |
| `make lab-test`          | Lab integration tests against each pinned build                      |
| `make peerwire-captures` | Regenerate peers-protocol capture fixtures from each pinned build    |

Lab targets take any stock binary via `HAPROXY_BIN=/path/to/haproxy`; lab
tests read `HTA_HAPROXY` (absolute path) and skip without it, so `make check`
needs no HAProxy. `make haproxy` needs curl, cmake, make, gcc/g++, perl, and
the Lua 5.4, PCRE2, and libcrypt dev packages (see `scripts/build-haproxy.sh`).

Run `make check` before marking any phase acceptance item complete. Fix lint
findings rather than suppressing them; a `//nolint` needs a specific linter
and an explanation.

## Conventions

- Exported identifiers need doc comments (enforced by revive).
- Integer narrowing on wire data must be checked; gosec G115 flags unchecked
  conversions and is not to be silenced without a proven bound.
- Large generated results go under `artifacts/` (gitignored).
