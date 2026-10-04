# Developer and CI entry points. `make check` is the gate every phase must
# pass. Tools are pinned in tools/*.mod and built by the toolchain that
# go.mod selects, so no global installs are needed. dprint (Markdown) is
# pinned in package.json and installed locally with bun.

GO ?= go
GOLANGCI_LINT = $(GO) tool -modfile=tools/golangci-lint.mod golangci-lint
GOVULNCHECK = $(GO) tool -modfile=tools/govulncheck.mod govulncheck
DPRINT = node_modules/.bin/dprint

# Per-target budget for the bounded fuzz smoke; long fuzzing is manual.
# FUZZMINIMIZETIME caps how long each newly interesting input is minimized,
# so large seeds cannot consume the smoke budget without fuzzing.
FUZZTIME ?= 10s
FUZZMINIMIZETIME ?= 1s

# Local lab (docs/plans/01-local-lab.md). HAPROXY_BIN may be any stock
# haproxy; the default is the pinned build from `make haproxy`.
HAPROXY_VERSIONS ?= 3.4.6 3.2.25
HAPROXY_BIN ?= artifacts/haproxy/3.4.6/haproxy
LAB_RECORD_DIR = $(CURDIR)/artifacts/lab-runs

.PHONY: check fmt fmt-check lint test test-race fuzz-smoke vuln tidy-check \
	tools-update haproxy lab lab-smoke lab-test peerwire-captures

check: fmt-check tidy-check lint test-race vuln

fmt: $(DPRINT)
	$(GOLANGCI_LINT) fmt
	$(DPRINT) fmt

fmt-check: $(DPRINT)
	$(GOLANGCI_LINT) fmt --diff
	$(DPRINT) check

$(DPRINT): package.json bun.lock
	bun install --frozen-lockfile
	@touch $@

lint:
	$(GOLANGCI_LINT) run

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

# Runs every Fuzz target once for FUZZTIME. Go only fuzzes one target per
# invocation, hence the loop.
fuzz-smoke:
	@set -e; for pkg in $$($(GO) list ./...); do \
		list=$$($(GO) test -list '^Fuzz' $$pkg) || exit 1; \
		for fn in $$(echo "$$list" | grep '^Fuzz' || true); do \
			echo "fuzz $$pkg $$fn"; \
			$(GO) test -run='^$$' -fuzz="^$$fn$$" -fuzztime=$(FUZZTIME) \
				-fuzzminimizetime=$(FUZZMINIMIZETIME) $$pkg; \
		done; \
	done

vuln:
	$(GOVULNCHECK) ./...

tidy-check:
	$(GO) mod tidy -diff

# Tool modfiles are deliberately not tidied: golangci-lint asks that its
# dependency set stay exactly as `go get -tool` resolved it.

# Bumps pinned tools to their latest releases; review the diff before
# committing.
tools-update:
	cd tools && $(GO) get -tool -modfile=golangci-lint.mod \
		github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	cd tools && $(GO) get -tool -modfile=govulncheck.mod \
		golang.org/x/vuln/cmd/govulncheck@latest

# Builds the pinned stock HAProxy releases into artifacts/haproxy.
haproxy:
	scripts/build-haproxy.sh $(HAPROXY_VERSIONS)

# Starts the two-node lab and tears it down on Ctrl-C.
lab:
	$(GO) run ./cmd/htalab -haproxy $(abspath $(HAPROXY_BIN)) \
		-record-dir $(LAB_RECORD_DIR)

# Creates the lab, sends 10 requests to node a and 20 to node b, checks the
# counts, and tears it down.
lab-smoke:
	$(GO) run ./cmd/htalab -haproxy $(abspath $(HAPROXY_BIN)) -smoke \
		-record-dir $(LAB_RECORD_DIR)

# Runs the lab integration tests (race detector on) against each pinned
# build, recording each run's Go/HAProxy identity in artifacts/lab-runs.
# This includes the live peers-protocol captures in internal/peerwire and
# the daemon's live session tests in internal/sources and cmd/htad.
lab-test:
	@set -e; for v in $(HAPROXY_VERSIONS); do \
		echo "lab-test haproxy $$v"; \
		HTA_HAPROXY=$(CURDIR)/artifacts/haproxy/$$v/haproxy \
		HTA_HAPROXY_VERSION=$$v HTA_LAB_RECORD_DIR=$(LAB_RECORD_DIR) \
		$(GO) test -race -count=1 -timeout 15m -v ./internal/... ./cmd/...; \
	done

# Regenerates the committed peers-protocol capture fixtures (framing in
# internal/peerwire, table messages in internal/peermsg) from each pinned
# build. Review the diff: it should change only PIDs, ports, timestamps,
# and time-dependent counter fields (ages, remaining lifetimes).
peerwire-captures:
	@set -e; for v in $(HAPROXY_VERSIONS); do \
		echo "peerwire-captures haproxy $$v"; \
		HTA_HAPROXY=$(CURDIR)/artifacts/haproxy/$$v/haproxy \
		HTA_HAPROXY_VERSION=$$v HTA_LAB_RECORD_DIR=$(LAB_RECORD_DIR) \
		HTA_PEERWIRE_CAPTURE_DIR=$(CURDIR)/internal/peerwire/testdata/captures \
		HTA_PEERMSG_CAPTURE_DIR=$(CURDIR)/internal/peermsg/testdata/captures \
		$(GO) test -count=1 -run '^TestLive(Table)?Capture$$' -v \
			./internal/peerwire ./internal/peermsg; \
	done
