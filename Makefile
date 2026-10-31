# Pure forwarder for the Rust gates. The recipes live in the intent monorepo
# root Makefile (https://github.com/intent-hq/intent), where this repo is the
# packages/intentd submodule; nothing is duplicated here and cargo is never
# invoked directly. Requested gate targets run together in one root make when
# this checkout is that submodule, so their shared prerequisite ordering and
# command-line variables (RESUME=1,
# GATE_FORCE=1, BASE=<ref>, DRY_RUN=1, ARGS=..., E='<filterset>', COUNT=N, LOAD=1) reach the root make through
# MAKEFLAGS. In a standalone clone the same targets exit 2 and name the
# monorepo command. Unknown targets keep make's ordinary "No rule" failure.
#
# The one local target, test-release-scripts, needs neither cargo nor the
# monorepo: it runs the scripts/test-* commands of ci.yml's release-scripts job,
# skipping suites that need CI-only tooling (scripts/release-scripts-local.py).

MONOREPO_ROOT := ../..
FORWARDED_TARGETS := check test test-changed test-select test-stress gate list-tests coverage-changed fmt clippy check-windows lint-sources

# Forward only to the intent monorepo: an unrelated Makefile two levels up
# must not be mistaken for it, so ../../.gitmodules has to name this path.
MONOREPO_PRESENT := $(and $(wildcard $(MONOREPO_ROOT)/Makefile),$(shell grep -Eqs '^[[:space:]]*path[[:space:]]*=[[:space:]]*packages/intentd[[:space:]]*$$' $(MONOREPO_ROOT)/.gitmodules && echo yes))

.DEFAULT_GOAL := help
.PHONY: help test-release-scripts $(FORWARDED_TARGETS)

help:
	@echo "intentd Makefile: forwards the Rust gate targets to the intent monorepo root Makefile."
	@echo ""
	@echo "Targets: $(FORWARDED_TARGETS)"
	@echo ""
	@echo "Requested targets run together in 'make -C $(MONOREPO_ROOT) <targets>' in the monorepo's"
	@echo "packages/intentd submodule ($(MONOREPO_ROOT)/Makefile exists and $(MONOREPO_ROOT)/.gitmodules"
	@echo "names packages/intentd); RESUME=1, GATE_FORCE=1, BASE=<ref>, DRY_RUN=1, ARGS=... and"
	@echo "E='<nextest filterset>' (test-select, test-stress) and COUNT=N LOAD=1 (test-stress) pass through."
	@echo "In a standalone clone the targets exit 2 and name the monorepo command."
	@echo ""
	@echo "Local target: test-release-scripts — run ci.yml's release-scripts job tests, skipping suites"
	@echo "that need CI-only tooling (exit 1 only on a real failure)."

test-release-scripts:
	python3 scripts/release-scripts-local.py

ifneq ($(MONOREPO_PRESENT),)
# One shared prerequisite preserves MAKECMDGOALS and deduplicates root work,
# including for `make -j gate check` and `make -j check gate`.
.PHONY: forward-gates
$(FORWARDED_TARGETS): forward-gates
forward-gates:
	$(MAKE) -C $(MONOREPO_ROOT) $(filter $(FORWARDED_TARGETS),$(MAKECMDGOALS))
else
$(FORWARDED_TARGETS):
	@echo "make $@: the Rust gates live in the intent monorepo root Makefile — run 'make -C <monorepo-root> $@' from a monorepo checkout (https://github.com/intent-hq/intent)" >&2
	@exit 2
endif
