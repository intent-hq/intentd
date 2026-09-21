# Agents — intentd

Per-package supplement to the [monorepo-root `AGENTS.md`](../../AGENTS.md). Read the root
guide first for the cross-package workflow (submodule PR → monorepo bump, conventional
commits). This file covers conventions specific to `packages/intentd`, the
Rust backend daemon.

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
together. Dependency direction is enforced per the "Dependency-direction rules" in
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
| SQLite schema + migrations   | `crates/intent-store/`                                       |
| ACP streaming / permissions  | `crates/intent-acp/`                                         |
| user-only agent field (hidden from agents) | `AGENT_HIDDEN_FIELDS` in `crates/intent-core/src/model.rs`; the egress registry + contract test in `crates/intent-acp/src/tests_hidden_field_egress.rs` — a new agent-facing egress that serves session/event data must be registered there |
| browser tab contract / `ws.browser.docs` text | `crates/intent-acp/src/mcp_server/bindings/browser_docs/*.md` — change together with the cloudlands-fe executor (`src/features/browser/main/browser-action-executor.ts`, `embedded-browser-cdp-service.ts`, the browser-tab-registry saga) and `../../docs/protocol/methods/files-terminal-browser.md`; monorepo `make docs-check` cross-checks the shared `errorCode` / `displayed` tokens |
| binary CLI + composition     | `crates/intentd/src/`                                        |
| integration / e2e tests      | `crates/intentd/tests/`                                      |
| deterministic ACP fixture    | `crates/intentd/tests/fixtures/mock-acp-agent.mjs`           |
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
- **Daemon spawns** — build them with `common::serve_command()` (the `intentd` binary,
  `serve`, and the `INTENTD_TCP_PORT=0` ephemeral-port seam so WSS daemons never race for
  the port `enable_ws_api` seeded), or `common::serve_command_fixed_port()` only when the
  test must bind the settings-file port. `serve_spawn_lint.rs` is a bounded textual
  backstop: it fails the suite on a single-statement
  `Command::new(env!("CARGO_BIN_EXE_intentd")) … "serve"` (30-line cap), and on a file
  whose code calls `enable_ws_api(` without `serve_command` in code (comments stripped;
  a split-statement raw spawn beside a genuine builder call is not detected). Opt out
  with `// serve-spawn: allow — <reason>` — on the offending statement's line for the
  first rule, anywhere in the file for the second (wrapper-program launchers only;
  reason required).

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
pass. Run them from the monorepo root via the top-level `Makefile`:

```bash
make check    # cargo fmt --check + cargo clippy --workspace --all-targets -- -D warnings
make test     # cargo nextest run --workspace (resumable; see the root AGENTS.md)
make test-changed  # nextest for only the crates this branch touched vs origin/main (BASE=<ref>); falls back to make test on manifest/lockfile/nextest-config changes
make coverage-changed  # the same changed selection under cargo llvm-cov (BASE=<ref>, DRY_RUN=1 prints the plan) — the local equivalent of CI's pull_request coverage-changed job
make gate     # check, then test
scripts/changed-tests.sh --instrumented  # (from packages/intentd) the same changed selection under cargo llvm-cov — what CI's coverage-changed job runs on every PR
```

The raw equivalents in `packages/intentd` are `cargo fmt --check`,
`cargo clippy --workspace --all-targets -- -D warnings`, and
`cargo nextest run --workspace --show-progress none`. Saved `ws.script` runs are
PTY-backed, so raw invocations need `CARGO_TERM_PROGRESS_WHEN=never` in the
environment (all three) and `--show-progress none` on the nextest command (the `make`
targets already set both) or progress-bar redraws flood the output buffer.
Raw invocations must also run under the rustup-managed toolchain pinned in
`rust-toolchain.toml` (`rustup run <pin> cargo ...` when a non-rustup cargo shadows
`PATH` — a Homebrew cargo once ran the gates on the wrong toolchain, see
intent-hq/intentd#1853); the `make` targets are the supported path.

The CI `check` job also runs the **source lints**: every `crates/<crate>/tests/*_lint.rs`
integration test is a source-scanning lint, selected by convention — run them all with
`make lint-sources` from the monorepo root or `cargo test --workspace --test '*_lint'` in
`packages/intentd`. Each lint fails naming `file:line`; its rationale, heuristic, and
limits live in its module doc. `source_lint_discovery_lint` fails when a `*_lint.rs` file
exists that the glob does not select (nested dir, `autotests = false`, renamed `[[test]]`)
or when ci.yml's `check` job has no non-comment `run:` line invoking the glob, so a new
lint needs no CI, Makefile, or docs wiring — add the file and a row below. Every opt-out
marker requires a reason; baselines only ratchet down (the lint fails until a fixed
file's entry is removed or lowered).

| Lint | Fails on | Opt-out / baseline |
| --- | --- | --- |
| `repo_slug_fold_lint` | a case-fold call on an `owner` / `repo` / `repository` / `slug` identifier outside `intent_core::RepoRef` (intent-hq/intentd#1809 → #1815); route slug identity through `RepoRef` regardless | `// repo-slug-fold: allow — <reason>` on the line above |
| `event_type_lint` | a `note:` / `task:` / `workspace:` / `agent:` string literal outside test code not in `intent_core::events::ALL_EVENT_TYPES`; add the type there, regenerate the golden `crates/intent-core/tests/goldens/event_types.json` with `INTENTD_UPDATE_GOLDENS=1`, and emit via the constant | `// event-type-lint: allow — <reason>` on the line above |
| `fixed_sleep_lint` | `thread::sleep(` / `time::sleep(` / shell `sleep <n>` under `crates/*/tests/**` (intent-hq/intentd#1924); wait on an observable event instead | `// timing-guard: <reason>` on the line or the line above; `crates/intent-core/tests/fixed_sleep_baseline.txt` (`<path> <count>`) |
| `raw_child_lint` | a test file naming `std::process::Child` as a type instead of holding an `intentd_test_support::GuardedChild` (borrows and `use` paths are not hits) | `// raw-child: allow — <reason>` on the line above; the lint's `BASELINE` |
| `tmp_hygiene_lint` | a raw `PathBuf::from("/tmp")` / `Path::new("/tmp")` / `temp_dir().join(..)` in test code instead of `test_tempdir` | trailing `// tmp-hygiene: allow — <reason>` |
| `repo_cache_path_lint` | a literal `".repo-cache"` in test code instead of `intent_git::repo_cache::cache_root_for` / `cache_path_for` | trailing `// repo-cache-path: allow — <reason>` |
| `serve_spawn_lint` | a single-statement `Command::new(env!("CARGO_BIN_EXE_intentd")) … "serve"`, or a file calling `enable_ws_api(` without `serve_command` in code | `// serve-spawn: allow — <reason>` on the statement line, or anywhere in the file for the second rule |
| `agent_hidden_field_egress_lint` | a `mcp_server/bindings/` file that reads session/event rows (`AgentLite` / `Event`, `agent_get(` / `agent_list(` / `event_query(` …) without a `SCRUBBED_BINDINGS` row (scrubs with `strip_agent_hidden_fields` + `EGRESS_REGISTRY` entries), or an `intent-services` fn copying `.data` wholesale into `json!` without a `WAKE_METADATA_BUILDERS` row; allowlist rows and registry entries are cross-checked for staleness | `HAND_PICKED_BINDINGS` / `SAFE_DATA_COPIES` rows in the lint (reason required) |
| `source_lint_discovery_lint` | a `*_lint.rs` file the glob does not select, or a ci.yml `check` job with no non-comment `run:` line invoking the glob | none |
| `workflow_grep_quiet_lint` | a `\| grep -q` / `-Eq` / `--quiet` / `--silent` pipeline in `.github/workflows/*.yml` — under `bash -eo pipefail` a large matched input makes the producer die of SIGPIPE and the step report "no match" (cloudlands-fe#2709); use a here-string for variable input, `\| grep -E pat >/dev/null` for a real producer, or `grep -q pat file` | none |

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
