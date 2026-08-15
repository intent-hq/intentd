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
| wire protocol / envelopes    | `../../docs/PROTOCOL.md`                                     |
| architecture / crate map     | `../../docs/ARCHITECTURE.md`                                 |
| UDS JSON-RPC router          | `crates/intent-transport/`                                   |
| WSS / TLS                    | `crates/intent-transport/` (WSS listener, fingerprint)      |
| domain logic / `WorkspaceApi`| `crates/intent-services/`                                    |
| SQLite schema + migrations   | `crates/intent-store/`                                       |
| ACP streaming / permissions  | `crates/intent-acp/`                                         |
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
    matches what `../../docs/PROTOCOL.md` defines for that method, byte-for-byte.
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
  envelope and method catalog are identical across UDS and WSS (per PROTOCOL.md §1), so
  the UDS suites are a useful reference for shaping new tests, but they do **not** replace
  the WSS e2e requirement; the WSS path has its own concerns (TLS upgrade, bearer auth,
  origin allow-list, fingerprint pinning, heartbeat) that only the WSS harness covers.

### Asserting the protocol contract

`../../docs/PROTOCOL.md` in the monorepo is the single source of truth for the wire
contract. When adding or changing a method, the WSS e2e is what proves the daemon meets
that contract:

- Assert the request shape the client sends matches PROTOCOL.md §5 for that method.
- Assert the success response carries the documented `result` payload (field names,
  optional vs required, nested envelope shapes).
- Assert error responses use the codes from PROTOCOL.md §9, not ad-hoc strings.
- For event-emitting methods, assert the `events.event` payload shape from PROTOCOL.md §6.

## Gates — keep them green

Before opening a submodule PR (and before bumping the monorepo gitlink), all three of the
following must pass in `packages/intentd`:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test
```

From the monorepo root the same gates are exposed via the top-level `Makefile`:

```bash
make check    # fmt + clippy against packages/intentd
make test     # cargo test against packages/intentd
```

See the [root `AGENTS.md`](../../AGENTS.md) for the full submodule-PR → monorepo-bump
workflow and conventional-commit / breadcrumb conventions.

## Filing issues

File bugs on [intent-hq/monorepo](https://github.com/intent-hq/monorepo/issues) — the
single tracker for all components; never track issues in markdown files. Use labels
`component:intentd` + `agent-filed`. See the [root `AGENTS.md`](../../AGENTS.md) →
Filing Issues for the full conventions (dedup, cross-referencing,
`Fixes intent-hq/monorepo#N` — the release notifier is completeness-gated: it comments
on the issue only once every linked intentd fix PR is merged and contained in the
released tag).
