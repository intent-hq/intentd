# Agents — intentd

Per-package supplement to the [monorepo-root `AGENTS.md`](../../AGENTS.md). Read the root
guide first for the cross-package workflow (submodule PR → monorepo bump, conventional
commits). This file covers conventions specific to `packages/intentd`, the
Rust backend daemon.

**Assistant app guide:** When adding, changing, moving, renaming, or removing a
user-facing feature, update the affected section of
[`crates/intent-services/resources/assistant-app-guide.md`](crates/intent-services/resources/assistant-app-guide.md)
in the same change. This is the single bundled guide; keep it concise and replace
obsolete paths, labels, and prerequisites, including changes coordinated with the frontend.
Keep the guide and user-facing help focused on features ready for users. Do not promote
unfinished, experimental, Labs-only, internal, or unreleased features as normal options;
code or tool availability is not proof of readiness. When explicitly asked about such
a feature, or assigned to develop or test it, discuss it honestly and label its status.
Update the guide and its source pointers when readiness or UI behavior changes.
Before opening a PR, record the affected guide section (and companion intentd PR
for frontend changes), or explain why the change has no user-help impact. Coordinate
companion releases; this checkpoint does not require simultaneous cross-repo merges.

> **Merge permission**: never merge a PR or arm auto-merge without explicit permission
> from a human — approved + green is not enough. See the
> [root `AGENTS.md`](../../AGENTS.md) for the full rule.

## Tech stack

- Rust 2021, cargo workspace of crates under `crates/` (see the layout below).
- **Transports**: Unix-domain socket (local-first default) and WSS/TLS (LAN); both speak
  **JSON-RPC 2.0** with identical envelope and method catalog.
- Persistence: SQLite via `sqlx` with embedded migrations (owned by `intent-store`).
- One service layer, many transports: `intent-services` is the single code path every
  listener calls.

## Project layout

```text
crates/
├── intent-core/           # leaf: domain types, errors, Config, WorkspaceApi trait
├── intent-store/          # SQLite + sqlx + migrations
├── intent-services/       # WorkspaceApi implementation (business logic)
├── intent-svc-transfer/   # feature crate: workspace transfer, above intent-services
├── intent-app/            # composition root: registers the feature crates (compose)
├── intent-transport/      # UDS + WSS listeners, JSON-RPC router
├── intent-acp/            # ACP client + agent provider plumbing
├── intent-context/        # context engines (e.g. Auggie)
├── intent-git/            # git operations
├── intent-linear/         # Linear integration
├── intent-providers/      # provider catalog / config
├── intent-pty/            # PTY / terminal helpers
├── intent-search/         # search
├── intent-sentry/         # Sentry wiring
├── intent-sourcecontrol/  # source-control helpers
└── intentd/               # binary: composition root + tests/
```

The `intentd` binary is the only composition root that wires concrete implementations
together; it registers the feature crates through `intent_app::compose`, and tests that
need a feature crate together with `intent-transport` live in `crates/intent-app/tests/`. Dependency direction is enforced per the "Dependency-direction rules" in
`../../docs/ARCHITECTURE.md` — `intent-core` is the leaf and `intent-transport` depends
only on `intent-services`, never on `intent-store`.

## Where to look

| Working on…                  | Open                                                         |
| ---------------------------- | ------------------------------------------------------------ |
| wire protocol / envelopes    | `../../docs/protocol/`                                     |
| architecture / crate map     | `../../docs/ARCHITECTURE.md`                                 |
| UDS JSON-RPC router          | `crates/intent-transport/`                                   |
| WSS / TLS                    | `crates/intent-transport/` (WSS listener, fingerprint)      |
| domain logic / `WorkspaceApi`| `crates/intent-services/`                                    |
| new services feature crate (`crates/intent-svc-<x>`) | Name it `intent-svc-<x>`; depend on `intent-services` (its `test-hooks` feature only from `[dev-dependencies]`), register it in `crates/intent-app` `compose`, and never let `intent-services` or `intent-transport` depend on it (dev-dependencies included). Feature-to-feature edges must be in `ALLOWED` in `scripts/check-feature-crate-deps.py` (CI `check` job; regression test `scripts/test-check-feature-crate-deps.sh`). Also: `[lints] workspace = true`, a `[profile.dev.package.intent-svc-<x>]` entry in the root `Cargo.toml`, a `release-plz.toml` `[[package]]` plus intentd's `changelog_include`, and a `git grep` of ci.yml, `.config/nextest.toml` and `scripts/` for the moved paths. `scripts/changed-tests.sh` picks the crate up from its directory name |
| SQLite schema + migrations   | `crates/intent-store/`                                       |
| ACP streaming / permissions  | `crates/intent-acp/`                                         |
| user-only agent field (hidden from agents) | `AGENT_HIDDEN_FIELDS` in `crates/intent-core/src/model.rs`; the egress registry + contract test in `crates/intent-acp/src/tests_hidden_field_egress.rs` — a new agent-facing egress that serves session/event data must be registered there |
| queue-entry visibility (who sees which queued message where) | `QueueSurface` + the contract table in `crates/intent-core/src/queue_visibility_contract.rs`, driven by the services harness (`crates/intent-services/src/agent_ops/queue_visibility_contract_tests.rs`) and the transport harness (`crates/intent-transport/src/events/tests.rs`); `queue_entry_egress_lint` fails until a new handler that serializes `queue_snapshot()` / publishes an `agent:queue:*` event is registered against a variant |
| browser tab contract / `ws.browser.docs` text | `crates/intent-acp/src/mcp_server/bindings/browser_docs/*.md` — change together with the cloudlands-fe executor (`src/features/browser/main/browser-action-executor.ts`, `embedded-browser-cdp-service.ts`, the browser-tab-registry saga) and `../../docs/protocol/methods/files-terminal-browser.md`; monorepo `make docs-check` cross-checks the shared `errorCode` / `displayed` tokens |
| binary CLI + composition     | `crates/intentd/src/`                                        |
| integration / e2e tests      | `crates/intentd/tests/`                                      |
| deterministic ACP fixture    | `crates/intentd/tests/fixtures/mock-acp-agent.mjs`           |
| writing a source lint (`*_lint.rs`) | `crates/intentd-test-support/src/source_lint.rs` (lexer, markers, cfg(test) blanking, statement splitter, file walker); "Gates → source lints" below |
| CI release / workflow scripts (`scripts/test-*` in ci.yml's `release-scripts` job) | `make test-release-scripts` (from `packages/intentd`; `make -C packages/intentd test-release-scripts` at the monorepo root) runs every test that job runs, in CI order, and skips suites that need CI-only tooling (release-plz, sccache, cargo-dist) with "SKIP <suite>: needs <X>" — a skipped suite is unverified, so install the tool when your change touches it; run it before pushing a change to `.github/workflows/*.yml` or `scripts/` (CI Gate's job list is pinned by `test-retire-obsolete-merge-runs-workflow.py`, intentd#2499) |
| a fake Codex/Claude CLI in a test | `intent_providers::test_support::current_cli_version` / `current_cli_script` (enable the intent-providers `test-support` feature from `[dev-dependencies]`); keep a literal version only for tests about that exact version, marked for `cli_fixture_version_lint` |
| a test that needs a provider "installed", or passes locally but fails on CI with "not installed" | `crates/intent-services/src/test_support/provider_installed.rs` (mark a provider installed; Codex and Claude Code ignore `providers.paths` overrides, so `/bin/sh` overrides do nothing); `scripts/test-without-provider-clis.sh <command>` runs tests with every provider CLI hidden, the way CI's runners lack them (Linux, needs `bwrap`; CI's `check` job asserts its runners have none) |
| RPC performance / cost rules | "Performance — the RPC cost contract" below; durable principles in `../../docs/ARCHITECTURE.md` |

## Performance — the RPC cost contract

Read paths have bounded-cost expectations; every recent performance regression came from
attaching unbounded-cost work to one of them. Precedent: intent-hq/monorepo#958
(full-transcript hydration per page), intent-hq/monorepo#1010 (blob materialization
before window filtering), intent-hq/monorepo#1061 (N+1 full-workdir git scans in
`git.diffs`), intent-hq/monorepo#963 (diffSummary rollup loop), and
intent-hq/monorepo#1396 (diskUsage enrichment on list). The durable version of these
principles lives in `../../docs/ARCHITECTURE.md`; this section is the day-to-day
contract for any PR touching the RPC boundary.

### Hot RPCs and the invariant

`workspace.list` / `workspace.get`, `agent.list` / `agent.get`, `agent.getConversation`,
`note.list`, `git.diffs`, and subscription seq-0 snapshots are on the FE's hot path and
fire constantly during normal use.

**Invariant: handler cost must be O(rows returned).** Concretely:

- No filesystem walks.
- No per-item git operations or subprocess spawns.
- No full-blob hydration — load projections, not whole payloads.
- Paging, filtering, and projection happen in SQL, never in memory after fetching a
  superset.

### Derived fields — the decision ladder

Any derived field on a wire payload must sit on exactly one rung:

1. **Invalidated only by daemon-owned mutations** → compute on write and persist it
   (scoped `UPDATE`); reads just select the column.
2. **Invalidated by external activity (git / filesystem)** → TTL or watch-invalidated
   cache refreshed *off* the read path (stale-while-revalidate), with a global
   concurrency cap on the refresher.
3. **Consumed only on hover / detail / expand** → keep it out of list payloads; expose
   a dedicated on-demand RPC.

### Burden of proof

A PR that adds a field to a list-shaped payload must state which ladder rung the field
sits on. "Computed inline on read" is not an option — that is exactly how the incidents
above happened.

### Runtime backstop

The daemon profiles each RPC dispatch and logs one WARN (method, statement count,
duration) when a dispatch exceeds the statement-count or duration threshold. Reviewers
should watch dogfooding logs for these warnings after merging anything that touches a
read path — a new WARN on a hot RPC is a regression, not noise.

## Testing — end-to-end against the real WSS transport

**Every feature MUST have an end-to-end test that drives the real WSS transport — not just
crate-level unit tests.** Unit tests around services/stores are necessary but not
sufficient: the wire path that production clients use (HTTPS upgrade → JSON-RPC 2.0 over
WebSocket → router → services → store and back) is the contract the FE and any other
client depends on, and only an e2e test exercises it.

### Required for every new feature

- A WSS e2e test that:
  - Opens a real WebSocket connection to the daemon's `/ws` endpoint (TLS, bearer auth,
    origin allow-list, fingerprint pinning all in play).
  - Sends the JSON-RPC **request** envelope for each new method.
  - Asserts the **response** envelope shape — `id`, `jsonrpc`, `result` / `error` —
    matches what `../../docs/protocol/` defines for that method, byte-for-byte.
  - For methods that emit events, subscribes via `events.subscribe`, drives the action,
    and asserts the resulting `events.event` notifications.
- Crate-level unit tests for non-trivial logic stay alongside the implementation; the WSS
  e2e is **in addition to**, not instead of, unit coverage.

This applies to every new `agent.*`, `chat.*`, `note.*`, `task.*`, `events.*`, `host.*`,
etc. method that lands in the router. If a method is in the catalog and clients call it,
it has a WSS e2e.

### Existing e2e infrastructure — plug into this

New tests should reuse the harness already in `crates/intentd/tests/`:

- **Suite targets** — the crate sets `autotests = false`: each `tests/<file>.rs` that uses
  `common` is a module of one `tests/suite_*.rs` target (the header of each root states
  which files belong to it), because every test executable links the whole daemon. Add a
  new file with `mod <file>;` in its suite root and `use crate::common;` instead of
  `mod common;`; `test_target_registration_lint` fails on an unregistered file. Its tests
  are named `<file>::<test>`. A file that changes process-global state in-process (`set_var`,
  `PR_SET_CHILD_SUBREAPER`, …) gets its own `[[test]]` instead: plain `cargo test` runs a
  suite's tests as threads of one process. Run a file's tests with
  `cargo nextest run -p intentd -E 'test(/^<file>::/)'`, and a test that re-executes
  its own binary with `--exact` must pass `common::exact_test_name(module_path!(), name)`.
- **WSS agent-lifecycle e2e** — landed in commit
  [`500b33c`](https://github.com/intent-hq/intentd/commit/500b33c) (`test(intentd): WSS
  e2e agent lifecycle over websocket (mock ACP provider)`). Drives the full agent
  lifecycle over a real WSS connection using the mock ACP provider. See
  `crates/intentd/tests/e2e_wss_agent_lifecycle.rs`.
- **WSS coverage sweep** — landed in commit
  [`887bbad`](https://github.com/intent-hq/intentd/commit/887bbad) (`test(intentd):
  WSS coverage sweep — router read/lifecycle arms, mid-stream disconnect, subscription
  filters, upgrade head guard`). Covers router read/lifecycle arms, mid-stream disconnect,
  subscription filters, and the upgrade head guard. See
  `crates/intentd/tests/wss_integration.rs`.
- **Deterministic mock ACP agent** — `crates/intentd/tests/fixtures/mock-acp-agent.mjs`.
  Use this fixture for any test that needs an ACP provider; it is deterministic and lets
  tests assert exact request/response shapes without external dependencies. There is also
  a mock MCP server fixture next to it (`mock-mcp-server.mjs`) for MCP-touching tests.
- **UDS integration tests** — `uds_*.rs` files exercise the same router over UDS. The
  envelope and method catalog are identical across UDS and WSS (per docs/protocol/01-transport.md §1), so
  the UDS suites are a useful reference for shaping new tests, but they do **not** replace
  the WSS e2e requirement; the WSS path has its own concerns (TLS upgrade, bearer auth,
  origin allow-list, fingerprint pinning, heartbeat) that only the WSS harness covers.
- **Scratch dirs** — create them with `common::test_tempdir(prefix)` /
  `common::test_tempdir_in("/tmp", prefix)` (or `test_support::test_tempdir` inside
  `intent-services`), declared before any guard that kills a daemon child: the `TempDir`
  sweeps on drop (including on panic) and `INTENTD_TEST_KEEP_TMP=1` keeps it for
  debugging. `tmp_hygiene_lint.rs` fails the suite on any raw `PathBuf::from("/tmp")` /
  `Path::new("/tmp")` / `temp_dir().join(..)` in test code unless the line ends with
  `// tmp-hygiene: allow — <reason>` (pure path arithmetic only).
- **Repo-cache paths** — derive them with `intent_git::repo_cache::cache_root_for` /
  `cache_path_for`, never `join(".repo-cache")`. `repo_cache_path_lint.rs` fails the
  suite on a literal `".repo-cache"` in test code unless the line ends with
  `// repo-cache-path: allow — <reason>`.
- **Daemon spawns** — use `common::hermetic_serve_command(data_dir)`, or
  `hermetic_serve_command_fixed_port(data_dir)` when the settings-file port is required.
  Both remove `GITHUB_TOKEN`, `GH_TOKEN`, `GH_ENTERPRISE_TOKEN`,
  `GITHUB_ENTERPRISE_TOKEN`, and `GH_HOST`, and select private gh config/secrets under the
  owned directory; retain it through shutdown. They also pin startup resume of interrupted
  agents off (`INTENTD_TEST_RESUME_ON_START_DEFAULT=off` resolves `auto`, which resumes
  only on display-less hosts such as CI; explicit `on` / `off` still win); a recovery
  test opts back in with `common::opt_into_resume_on_start(&mut cmd)`. Reapply `hermetic_fixture_identity` after
  generic environment overrides. Intentional mock identity uses `common::mock_github_token`
  with a local `// fixture-identity: allow — <private mock reason>` comment.
  `serve_spawn_lint.rs` checks raw/retired builders and identity mutations on simple command
  bindings, including split statements beside safe calls. The wrapper/port marker
  `// serve-spawn: allow — <reason>` never exempts identity. Its module docs describe the
  bounded analysis: no alias, interprocedural, macro, or control-flow proof.

### Asserting the protocol contract

`../../docs/protocol/` in the monorepo is the single source of truth for the wire
contract. When adding or changing a method, the WSS e2e is what proves the daemon meets
that contract:

- Assert the request shape the client sends matches the method's subsection file under docs/protocol/methods/ (§5.x).
- Assert the success response carries the documented `result` payload (field names,
  optional vs required, nested envelope shapes).
- Assert error responses use the codes from docs/protocol/09-error-codes.md §9, not ad-hoc strings.
- For event-emitting methods, assert the `events.event` payload shape from docs/protocol/06-events.md §6.

## Gates — keep them green

Before opening a submodule PR (and before bumping the monorepo gitlink), the gates must
pass. Run them from the monorepo root via the top-level `Makefile` (the same targets also
work from `packages/intentd`, whose `Makefile` forwards them to the monorepo root; in a
standalone clone they exit 2 and name the monorepo command):

```bash
make check    # cargo fmt --check + cargo clippy --workspace --all-targets -- -D warnings
make test     # cargo nextest run --workspace (resumable; see the root AGENTS.md)
make test-changed  # nextest for only the crates this branch touched vs origin/main (BASE=<ref>); falls back to make test on manifest/lockfile/nextest-config changes; also runs the end-to-end modules .config/e2e-test-map.txt maps changed source to — add an entry when an end-to-end test exercises another crate's source (scripts/changed-tests.sh --check-map validates it)
make test-select E='test(/name/)'  # just the tests matching one nextest filterset, with make test's fixtures and gate-run record — use it to iterate on a few tests
make coverage-changed  # the same changed selection under cargo llvm-cov (BASE=<ref>, DRY_RUN=1 prints the plan) — the local equivalent of CI's coverage-changed job (PRs and merge queue)
make gate     # check, then test
make check-windows  # cargo check + clippy --all-targets for x86_64-pc-windows-gnu (needs MinGW-w64), as CI's windows-cross-lint runs on every PR — run it while iterating on cfg(unix) / cfg(windows) code, before pushing
scripts/changed-tests.sh --instrumented  # (from packages/intentd) the same changed selection under cargo llvm-cov — what CI's coverage-changed job runs on PRs and in the merge queue
```

The raw equivalents in `packages/intentd` are `cargo fmt --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`cargo nextest run --workspace --show-progress none` — except that raw nextest leaves
the callback fixture the `make` targets set unset, so intent-acp/intent-services callback
tests fail in bulk (95 bogus failures while fixing #2559); run those via `make test-select`
(Linux x64 only, like the fixture). Saved `ws.script` runs are
PTY-backed, so raw invocations need `CARGO_TERM_PROGRESS_WHEN=never` in the
environment (all three) and `--show-progress none` on the nextest command (the `make`
targets already set both) or progress-bar redraws flood the output buffer.
Raw invocations must also run under the rustup-managed toolchain pinned in
`rust-toolchain.toml` (`rustup run <pin> cargo ...` when a non-rustup cargo shadows
`PATH` — a Homebrew cargo once ran the gates on the wrong toolchain, see
intent-hq/intentd#1853); the `make` targets are the supported path.

`make check` lints only the host target, so code gated to `cfg(unix)` or `cfg(windows)` can be
clean there and still fail Windows Clippy (an import that only `cfg(unix)` code uses, intentd#2672).
CI's `windows-cross-lint` job runs the same check on every PR, on Linux, so a local run only
saves a push round trip; the merge queue's native `windows-lint` job stays the final authority.

The CI `check` job also runs the **source lints**: every `crates/<crate>/tests/*_lint.rs`
integration test is a source-scanning lint, selected by convention — run them all with
`make lint-sources` from the monorepo root or `cargo test --workspace --test '*_lint'` in
`packages/intentd`. Each lint fails naming `file:line`; its rationale, heuristic, and
limits live in its module doc. A new lint imports `intentd_test_support::source_lint`
rather than copying scaffolding and needs no CI, Makefile, or docs wiring — add the file
and a row below; `source_lint_discovery_lint` enforces all of this (a `*_lint.rs` the glob
does not select, a ci.yml `check` job with no non-comment `run:` line invoking the glob,
or copied scaffolding). Every opt-out marker requires a reason; baselines only ratchet
down (the lint fails until a fixed file's entry is removed or lowered).

| Lint | Fails on | Opt-out / baseline |
| --- | --- | --- |
| `repo_slug_fold_lint` | a case-fold call on an `owner` / `repo` / `repository` / `slug` identifier outside `intent_core::RepoRef` (intent-hq/intentd#1809 → #1815); route slug identity through `RepoRef` regardless | `// repo-slug-fold: allow — <reason>` on the line above |
| `event_type_lint` | a `note:` / `task:` / `workspace:` / `agent:` string literal outside test code not in `intent_core::events::ALL_EVENT_TYPES`; add the type there, regenerate the golden `crates/intent-core/tests/goldens/event_types.json` with `INTENTD_UPDATE_GOLDENS=1`, and emit via the constant | `// event-type-lint: allow — <reason>` on the line above |
| `running_turn_rule_lint` | an or-pattern outside test code and `crates/intent-core/src/model.rs` spelling exactly `AgentStatus::Pending \| Active \| Processing` (any order, `Self::` / qualified paths, `matches!` or match arm), or a string literal in non-test `crates/intent-store/src/**` containing both `'active'` and `'Processing'` (a hand-written SQL status list) — a fifth copy of the running-turn rule #2058 consolidated onto `AgentStatus::is_running_turn`; call the method, or generate the SQL list from `AgentStatus::ALL` filtered by it as `delegated_counts_sql` does | `// running-turn: allow — <reason>` on the line above the pattern / literal or its statement |
| `fixed_sleep_lint` | `thread::sleep(` / `time::sleep(` / shell `sleep <n>` under `crates/*/tests/**` (intent-hq/intentd#1924); wait on an observable event instead | `// timing-guard: <reason>` on the line or the line above; `crates/intent-core/tests/fixed_sleep_baseline.txt` (`<path> <count>`) |
| `fixed_deadline_lint` | a literal `Duration::from_*(…)` (not scaled by a non-literal `.mul_f64(m)` / `* m`), or a test-code `const` / `static` / `let` bound to exactly one (intentd#2649), used as the budget of `timeout(` / `recv_timeout(` / `wait_timeout(` (either argument, for `Condvar`) / `wait_timeout_while(` / `wait_with_timeout(`, or added to `Instant::now()`, in test code (`crates/*/tests/**`, plus `#[cfg(test)]` / `#[cfg(all(test, …))]` items, `tests.rs` / `*_tests.rs` files, `tests/` dirs and test-module files under `src/`) — it ignores `INTENTD_TEST_TIMEOUT_MULTIPLIER` and flakes on slow coverage runners (intentd#2514); wrap it in `intentd_test_support::test_timeout(…)` (a hang-guard const becomes a `fn` returning it) | `// fixed-deadline: allow — <reason>` on the line above (above a binding, it covers every use); `crates/intent-core/tests/fixed_deadline_baseline.txt` (`<path> <count>`; `INTENTD_UPDATE_FIXED_DEADLINE_BASELINE=1` only lowers it) |
| `raw_child_lint` | a test file naming `std::process::Child` as a type instead of holding an `intentd_test_support::GuardedChild` (borrows and `use` paths are not hits) | `// raw-child: allow — <reason>` on the line above (no baseline: every suite is migrated) |
| `tmp_hygiene_lint` | a raw `PathBuf::from("/tmp")` / `Path::new("/tmp")` / `temp_dir().join(..)` in test code instead of `test_tempdir` | trailing `// tmp-hygiene: allow — <reason>` |
| `repo_cache_path_lint` | a literal `".repo-cache"` in test code instead of `intent_git::repo_cache::cache_root_for` / `cache_path_for` | trailing `// repo-cache-path: allow — <reason>` |
| `serve_spawn_lint` | raw/retired daemon builders, missing WSS builder calls, or identity overrides on tracked commands without a later complete reset | `serve-spawn: allow` for launch mechanics only; `fixture-identity: allow` for named mock helper calls only; both require a reason (see module docs) |
| `agent_hidden_field_egress_lint` | a `mcp_server/bindings/` file that reads session/event rows (`AgentLite` / `Event`, `agent_get(` / `agent_list(` / `event_query(` …) without a `SCRUBBED_BINDINGS` row (scrubs with `strip_agent_hidden_fields` + `EGRESS_REGISTRY` entries), or an `intent-services` fn copying `.data` wholesale into `json!` without a `WAKE_METADATA_BUILDERS` row; allowlist rows and registry entries are cross-checked for staleness | `HAND_PICKED_BINDINGS` / `SAFE_DATA_COPIES` rows in the lint (reason required) |
| `queue_entry_egress_lint` | a non-test `queue_snapshot(` / `queue_snapshot_preview(` call whose enclosing fn, or an `event_type: <const>` publish of any const whose string value in `crates/intent-core/src/events.rs` starts with `agent:queue:` (the watched set is derived from that file, so a new `agent:queue:*` constant is watched without editing the lint) whose const, is named by no `REGISTERED_EGRESS` row (the row maps it to a `QueueSurface` variant in `crates/intent-core/src/queue_visibility_contract.rs`, so the contract harnesses drive it); outside `events.rs`, any other reference to a watched const (helper argument, local binding, `json!` value, comparison; `use` items excepted) or an `"agent:queue:*"` string literal whose enclosing fn is named by no `Key::Fn` row; a variant no row claims, or a row naming a variant / fn / const that no longer exists, also fails (multiplayer queue visibility, intentd#2068) | `// queue-egress: allow — <reason>` on the line above the call / reference or its statement, only when no entry leaves the daemon (a consumer matching on the type) |
| `source_lint_discovery_lint` | a `*_lint.rs` file the glob does not select, a ci.yml `check` job with no non-comment `run:` line invoking the glob, or a `*_lint.rs` file defining its own `fn lex(` / `markers_by_line(` / `blank_cfg_test_items(` / `cfg_test_item_ranges(` / `split_statements(` instead of importing `intentd_test_support::source_lint` (the #2073 fixes had to be re-applied per private copy) | `// source-lint-scaffolding: allow — <reason>` on the line above the definition; none for the first two rules |
| `test_target_registration_lint` | a top-level `crates/*/tests/*.rs` that is neither a cargo test target nor a bare `mod <file>;` of one (a crate with `autotests = false`, like `intentd`'s `tests/suite_*.rs` grouping, would otherwise never build it), a `tests/suite_*.rs` module list out of sorted order, or a suite module calling `env::set_var(` / `env::remove_var(` / `set_current_dir(` / `set_child_subreaper(` (plain `cargo test` shares one process across a suite; make such a file a standalone `[[test]]`), or a `--test <name>` in `.github/` / `scripts/` / `Makefile` naming a suite module instead of a target (merge-queue-only jobs otherwise fail late, intentd#2635) | `// process-global: allow — <reason>` on the line above, only for a call that runs in a separately exec'd child |
| `transfer_version_source_lint` | `CARGO_PKG_VERSION` within 20 lines of the transfer manifest's `creating_intentd_version` / `creatingIntentdVersion` field outside `crates/intent-core/src/transfer.rs` — crate versions can drift in a release, so export, the import check and test manifests must all use `intent_core::transfer::CREATING_INTENTD_VERSION` | none |
| `workflow_grep_quiet_lint` | a `\| grep -q` / `-Eq` / `--quiet` / `--silent` pipeline in `.github/workflows/*.yml` — under `bash -eo pipefail` a large matched input makes the producer die of SIGPIPE and the step report "no match" (cloudlands-fe#2709); use a here-string for variable input, `\| grep -E pat >/dev/null` for a real producer, or `grep -q pat file` | none |
| `cli_fixture_version_lint` | a hand-written Codex/Claude `--version` output (`codex-cli X`, `X (Claude Code)`, `claude X`, `fixture-cli X`, …) in a string literal of any `crates/**/*.rs` file or in a `.mjs`/`.cjs`/`.js`/`.py`/`.sh` fixture script — old or new, it breaks once an adapter bump moves the minimum (intentd#2567); print `intent_providers::test_support::current_cli_version` or use `current_cli_script`; a marker that covers no version is stale | `// cli-fixture-version: allow — <reason>` (`#` in shell/Python) on the line above, or above a multi-line literal's opening line |
| `workspace_literal_lint` | a `Workspace { … }` struct expression or pattern in any `crates/**/*.rs` file, test code included, outside `crates/intent-core/src/model.rs` that names every `Workspace` field with no `..` rest element — each new optional field otherwise means editing ~150 literals (intentd#2605, #2637, #2688); set only the fields that matter and end with `..Default::default()` or spread `intent_services::test_support::fixtures::workspace` | `// workspace-literal: allow — <reason>` on the line above the `Workspace` token |
| `settle_only_wait_lint` | a use of `SubHandle::wait_settled_even_if_failed` in any `crates/**/*.rs` file, test code included — it returns on a registration settled as FAILED, so a test writing next races the shared watch's creation retry and its event wait hangs (intent-hq/intent#4845, #4852, #6128); sync on `wait_established` / `wait_all_established` / `RegistrationProbe::wait_live`, which ride the retry out; one marker covers one use, and a marker that covers no use is stale | `// settle-only-wait: allow — <reason>` on the line above the use or its statement, only for a test that expects the registration to fail |

See the [root `AGENTS.md`](../../AGENTS.md) for the full submodule-PR → monorepo-bump
workflow and conventional-commit / breadcrumb conventions.

One commit-message hazard worth repeating here: release tooling treats the literal
`BREAKING CHANGE:` / `BREAKING-CHANGE:` (and `Release-As:`) token appearing anywhere in
a commit body as a real footer, and squash merges fold every branch commit message into
the squash body — so a commit that merely *quotes* the token causes a false major bump
(or, for `Release-As:`, a forced pinned version); this accidentally cut cloudlands-fe
v3.0.0 — see intent-hq/monorepo#2988. Never write
the literal token in commit messages, PR titles/bodies, or review comments unless an
actual breaking change is intended; when describing the mechanism, write "the
breaking-change footer token" or similar instead.

## Filing issues

File bugs on [intent-hq/intent](https://github.com/intent-hq/intent/issues) — the
single tracker for all components; never track issues in markdown files. Use labels
`component:intentd` + `agent-filed`. See the [root `AGENTS.md`](../../AGENTS.md) →
Filing Issues for the full conventions (dedup, cross-referencing,
`Fixes intent-hq/intent#N` — the release notifier is completeness-gated: it comments
on the issue only once the issue is closed, at least one linked intentd fix PR is
merged and contained in the released tag, no linked intentd fix PR is still open, and
every merged one is contained (PRs closed without merging are ignored); a plain
`intent-hq/intent#N` mention never earns a release comment, only a closing-keyword
reference on the actual fix PR does).
