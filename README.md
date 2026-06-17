# intentd

`intentd` is a local-first, headless Rust daemon that owns the Intent domain model —
workspaces, notes, tasks, comments, agents, git, pull requests, scripts, terminals, files,
and events — and exposes it over a **JSON-RPC 2.0** API. Clients (a desktop UI, a CLI, or an
agent acting as an MCP client) are thin: all business logic lives in the daemon.

> ⚠️ Private Repository — This repo is internal to the Cloudlands engineering team. It is
> consumed as a git submodule by [cloudlands-ai/monorepo](https://github.com/cloudlands-ai/monorepo).

## What it is

- **Local-first.** The default transport is a **Unix-domain socket** (mode `0600`,
  newline-delimited JSON-RPC frames). LAN transports (TCP/TLS/WSS) are planned, not built.
- **Single source of truth.** The daemon owns all durable state in **SQLite** (via `sqlx`
  with embedded migrations); clients hold only ephemeral UI state.
- **One service layer, many transports.** Transports are thin; the shared `WorkspaceApi`
  service surface is the single code path every listener (and, later, the agent-facing MCP
  server) calls.

## Architecture

A single cargo **workspace** with 12 crates. Dependency direction is enforced per
`IMPLEMENTATION_SPEC.md` §3.2: `intent-core` is the leaf, `intent-transport` depends only on
`intent-services` (never on `intent-store`), and the `intentd` binary is the only composition
root that wires concrete implementations together.

```text
                       ┌──────────────────────────────┐
                       │     intentd (bin)            │  CLI: serve / call / status / doctor
                       │   composition root (§3.2)    │  wires store → services → transport
                       └───────────────┬──────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        ▼                              ▼                               ▼
┌───────────────┐            ┌──────────────────┐            ┌──────────────────┐
│ intent-store  │  ◄───────  │ intent-services  │  ◄───────  │ intent-transport │
│ SQLite + sqlx │            │  WorkspaceApi    │            │ UDS listener +   │
│  migrations   │            │  implementation  │            │ JSON-RPC router  │
└───────┬───────┘            └────────┬─────────┘            └────────┬─────────┘
        │                             │                               │
        └─────────────────────────────┴───────────────┬───────────────┘
                                                       ▼
                                              ┌──────────────────┐
                                              │   intent-core    │  leaf: ids, errors→codes,
                                              │  domain + traits │  Config/paths, Workspace/Note,
                                              └──────────────────┘  WorkspaceApi trait
```

Stub crates (present and compiling, implementation deferred to later waves):
`intent-acp`, `intent-providers`, `intent-sourcecontrol`, `intent-git`, `intent-context`,
`intent-pty`, `intent-search`.

| Crate | Role |
| --- | --- |
| `intent-core` | Leaf domain vocabulary: ids, `Error`→JSON-RPC code mapping, `Config`/path resolution, `Workspace`/`Note` model, the `WorkspaceApi` trait. |
| `intent-store` | SQLite persistence via `sqlx` + embedded migrations. |
| `intent-services` | `WorkspaceApi` implementation (the shared service surface). |
| `intent-transport` | JSON-RPC router + UDS listener (TLS/auth/mDNS/heartbeat are stubs). |
| `intentd` | Binary composition root + CLI (`serve`/`call`/`status`/`doctor`). |
| `intent-acp` / `intent-providers` / `intent-sourcecontrol` / `intent-git` / `intent-context` / `intent-pty` / `intent-search` | Stub crates for future waves. |

## Quickstart

```bash
# 1. Build the workspace
cargo build --workspace

# 2. Start the daemon (UDS) in one shell
cargo run -p intentd -- serve --listen uds
#   intentd listening on UDS path=~/Library/Application Support/intentd/intentd.sock

# 3. In another shell, make a JSON-RPC call
cargo run -p intentd -- call workspace.list
#   { "workspaces": [] }
cargo run -p intentd -- call note.list --params '{"workspaceId":"ws-1"}'
#   { "notes": [] }

# 4. Probe liveness and run diagnostics
cargo run -p intentd -- status   # intentd: up / workspaces: N
cargo run -p intentd -- doctor   # data-dir writable + SQLite/migrations current
```

Paths are resolved via the `directories` crate and can be overridden with the
`INTENTD_DATA_DIR` and `INTENTD_CONFIG` environment variables. The data dir holds the SQLite
database (`intentd.db`) and the socket (`intentd.sock`).

## Current status

This repo currently implements a **thin UDS vertical slice** that proves the architecture
end-to-end. Be aware of what is real versus planned:

**Implemented**

- JSON-RPC 2.0 over a Unix-domain socket (newline-delimited, mode `0600`).
- Methods: `workspace.list` and `note.list`, served from SQLite.
- CLI subcommands: `intentd serve --listen uds`, `intentd call <method> [--params '<json>']`,
  `intentd status`, `intentd doctor`.
- Standard JSON-RPC error codes: `-32700`, `-32600`, `-32601`, `-32602`, `-32603`.

**Planned / not yet implemented**

- The remaining ~104 methods in `PROTOCOL.md` (the full `workspace.*`/`note.*`/`comment.*`/
  `task.*`/`agent.*`/`git.*`/`pr.*`/`script.*`/`file.*`/`event.*`/… catalog).
- TCP/TLS/WSS transport, mDNS discovery, bearer auth + origin allow-list.
- ACP client + agent-facing MCP server, provider spawning, GitHub source control, context
  engine, PTY/terminals, and search — these crates exist today only as stubs.
- Event bus + `events.*` subscriptions.

## Development

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo build --workspace
cargo test --workspace
```

**Conventional commits** are required (`feat`, `fix`, `chore`, `docs`, `refactor`, `test`,
`ci`, `perf`); PR titles are validated in CI and changelogs are generated with `git-cliff`.

## Documentation

The design lives in the monorepo under `docs/00_initial_porting/`:

- `IMPLEMENTATION_SPEC.md` — architecture, crate layout (§3), persistence, roadmap.
- `PROTOCOL.md` — the wire contract: transport, JSON-RPC envelope, full method catalog,
  events, and error codes.

## Related Repositories

- [cloudlands-ai/monorepo](https://github.com/cloudlands-ai/monorepo) — engineering monorepo
  that mounts this repo at `packages/intentd` and holds the cross-cutting docs and tooling.
