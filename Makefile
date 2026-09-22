# Pure forwarder for the Rust gates. The recipes live in the intent monorepo
# root Makefile (https://github.com/intent-hq/intent), where this repo is the
# packages/intentd submodule; nothing is duplicated here and cargo is never
# invoked directly. Each gate target runs `make -C ../.. <target>` when this
# checkout is that submodule, so command-line variables (RESUME=1,
# GATE_FORCE=1, BASE=<ref>, DRY_RUN=1, ARGS=...) reach the root make through
# MAKEFLAGS. In a standalone clone the same targets exit 2 and name the
# monorepo command. Unknown targets keep make's ordinary "No rule" failure.

MONOREPO_ROOT := ../..
FORWARDED_TARGETS := check test test-changed gate list-tests coverage-changed fmt clippy lint-sources

# Forward only to the intent monorepo: an unrelated Makefile two levels up
# must not be mistaken for it, so ../../.gitmodules has to name this path.
MONOREPO_PRESENT := $(and $(wildcard $(MONOREPO_ROOT)/Makefile),$(shell grep -Eqs '^[[:space:]]*path[[:space:]]*=[[:space:]]*packages/intentd[[:space:]]*$$' $(MONOREPO_ROOT)/.gitmodules && echo yes))

.DEFAULT_GOAL := help
.PHONY: help $(FORWARDED_TARGETS)

help:
	@echo "intentd Makefile: forwards the Rust gate targets to the intent monorepo root Makefile."
	@echo ""
	@echo "Targets: $(FORWARDED_TARGETS)"
	@echo ""
	@echo "Each runs 'make -C $(MONOREPO_ROOT) <target>' when this checkout is the monorepo's"
	@echo "packages/intentd submodule ($(MONOREPO_ROOT)/Makefile exists and $(MONOREPO_ROOT)/.gitmodules"
	@echo "names packages/intentd); RESUME=1, GATE_FORCE=1, BASE=<ref>, DRY_RUN=1 and ARGS=..."
	@echo "pass through. In a standalone clone the targets exit 2 and name the monorepo command."

ifneq ($(MONOREPO_PRESENT),)
$(FORWARDED_TARGETS):
	$(MAKE) -C $(MONOREPO_ROOT) $@
else
$(FORWARDED_TARGETS):
	@echo "make $@: the Rust gates live in the intent monorepo root Makefile — run 'make -C <monorepo-root> $@' from a monorepo checkout (https://github.com/intent-hq/intent)" >&2
	@exit 2
endif
