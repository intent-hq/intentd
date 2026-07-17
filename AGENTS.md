# Agents — intentd

Per-package supplement to the [monorepo-root `AGENTS.md`](../../AGENTS.md). Read the root
guide first for the cross-package workflow (submodule PR → monorepo bump, conventional
commits, breadcrumbs). This file covers conventions specific to `packages/intentd`, the
Rust backend daemon.

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
together. Dependency direction is enforced per `docs/00_initial_porting/IMPLEMENTATION_SPEC.md`
§3.2 — `intent-core` is the leaf and `intent-transport` depends only on `intent-services`,
never on `intent-store`.

## Where to look

| Working on…                  | Open                                                         |
| ---------------------------- | ------------------------------------------------------------ |
| wire protocol / envelopes    | `../../docs/00_initial_porting/PROTOCOL.md`                  |
| architecture / crate map     | `../../docs/00_initial_porting/IMPLEMENTATION_SPEC.md`       |
| porting progress             | `../../docs/00_initial_porting/BREADCRUMBS.md`               |
| UDS JSON-RPC router          | `crates/intent-transport/`                                   |
| WSS / TLS                    | `crates/intent-transport/` (WSS listener, fingerprint)      |
| domain logic / `WorkspaceApi`| `crates/intent-services/`                                    |
| SQLite schema + migrations   | `crates/intent-store/`                                       |
| ACP streaming / permissions  | `crates/intent-acp/`                                         |
| binary CLI + composition     | `crates/intentd/src/`                                        |
| integration / e2e tests      | `crates/intentd/tests/`                                      |
| deterministic ACP fixture    | `crates/intentd/tests/fixtures/mock-acp-agent.mjs`           |

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
    matches what `docs/00_initial_porting/PROTOCOL.md` defines for that method, byte-for-byte.
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

`docs/00_initial_porting/PROTOCOL.md` is the single source of truth for the wire
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
cargo clippy -- -D warnings
cargo test
```

From the monorepo root the same gates are exposed via the top-level `Makefile`:

```bash
make check    # fmt + clippy against packages/intentd
make test     # cargo test against packages/intentd
```

See the [root `AGENTS.md`](../../AGENTS.md) for the full submodule-PR → monorepo-bump
workflow and conventional-commit / breadcrumb conventions.
