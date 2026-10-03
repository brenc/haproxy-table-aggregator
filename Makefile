# Developer and CI entry points. `make check` is the gate every phase must
# pass. Tools are pinned in tools/*.mod and built by the toolchain that
# go.mod selects, so no global installs are needed. dprint (Markdown) is
# pinned in package.json and installed locally with bun.

GO ?= go
GOLANGCI_LINT = $(GO) tool -modfile=tools/golangci-lint.mod golangci-lint
GOVULNCHECK = $(GO) tool -modfile=tools/govulncheck.mod govulncheck
DPRINT = node_modules/.bin/dprint

# Per-target budget for the bounded fuzz smoke; long fuzzing is manual.
FUZZTIME ?= 10s

.PHONY: check fmt fmt-check lint test test-race fuzz-smoke vuln tidy-check \
	tools-update

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
			$(GO) test -run='^$$' -fuzz="^$$fn$$" -fuzztime=$(FUZZTIME) $$pkg; \
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
