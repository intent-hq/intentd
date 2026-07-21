# intentd

`intentd` is a local-first, headless Rust daemon that owns the Intent domain model —
workspaces, notes, tasks, comments, agents, git, pull requests, scripts, terminals, files,
and events — and exposes it over a **JSON-RPC 2.0** API. Clients (a desktop UI, a CLI, or an
agent acting as an MCP client) are thin: all business logic lives in the daemon.

This repo is consumed as a git submodule by [intent-hq/monorepo](https://github.com/intent-hq/monorepo).

## What it is

- **Local-first.** The default transport is a **Unix-domain socket** (mode `0600`,
  newline-delimited JSON-RPC frames). A secure **WSS/TLS** LAN transport (bearer auth,
  origin allow-list, TLS fingerprint pinning) runs alongside it; there
  is no plaintext TCP product transport.
- **Single source of truth.** The daemon owns all durable state in **SQLite** (via `sqlx`
  with embedded migrations); clients hold only ephemeral UI state.
- **One service layer, many transports.** Transports are thin; the shared `WorkspaceApi`
  service surface is the single code path every listener — and the agent-facing MCP
  server — calls.

## Architecture

A single cargo **workspace** with 12 crates. Dependency direction is enforced per
`IMPLEMENTATION_SPEC.md` §3.2: `intent-core` is the leaf, `intent-transport` depends only on
`intent-services` (never on `intent-store`), and the `intentd` binary is the only composition
root that wires concrete implementations together.

```text
                       ┌──────────────────────────────┐
                       │     intentd (bin)            │  CLI: serve / call / status / stop / doctor / …
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

The diagram shows the core service path. The composition root also wires the ACP agent
runtime, source-control, git, PTY, and search engines into the service layer.

| Crate | Role |
| --- | --- |
| `intent-core` | Leaf domain vocabulary: ids, `Error`→JSON-RPC code mapping, `Config`/path resolution, domain model, the `WorkspaceApi` trait. |
| `intent-store` | SQLite persistence via `sqlx` + embedded migrations. |
| `intent-services` | `WorkspaceApi` implementation (the shared service surface) + the `AgentManager`, MCP callback server, and per-domain ops. |
| `intent-transport` | JSON-RPC router + UDS listener, the WSS/TLS listener, bearer auth + origin allow-list, and heartbeat. |
| `intentd` | Binary composition root + CLI (`serve`/`call`/`status`/`stop`/`doctor`/`import`/`service`/`mcp-bridge`). |
| `intent-acp` | ACP client core + `AgentManager` orchestration, agent→BE MCP callback server, and the loopback MCP bridge. |
| `intent-providers` | Provider registry + model resolution for spawning agent runtimes. |
| `intent-sourcecontrol` | GitHub/PR via `octocrab` (REST + GraphQL), token resolution, GHE support. |
| `intent-git` | Local git over `libgit2` (status/stage/commit/branches/merge-conflict + worktree/diff helpers). |
| `intent-context` | Ripgrep/symbol-backed context engine (auggie codebase-retrieval is **won't-port**). |
| `intent-pty` | Unified `portable-pty` host for interactive terminals **and** scripts. |
| `intent-search` | Gitignore-aware content + filename search over ripgrep/`ignore`/`globset`. |

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

The backend port is well past a vertical slice: Milestones 1–10 are implemented, plus the
recent-repository registry, ACP session resume-on-respawn, and iOS-driven wire-parity fixes.
For the authoritative, dated progress log see
`docs/00_initial_porting/BREADCRUMBS.md` in the monorepo.

**Implemented**

- **JSON-RPC surface (~143 request methods + 1 server-initiated `events.event` notification),
  served from SQLite,** across these namespaces:
  - Domain CRUD: `workspace.*` (9), `repo.*` (1, recent-repository registry), `note.*` (12),
    `task.*` (8), `comment.*` (5).
  - Events: `event.*` (7) + the `events.*` subscribe/unsubscribe fast-path and `events.event`
    push notification.
  - Source control & review: `git.*` (6), `pr.*` (12, active-PR gated), `file-tracking.*` (8),
    `metrics.*` (4), `accept-changes.*` (5).
  - Search, terminals & scripts: `search.*` (8), `terminal.*` (6), `script.*` (9).
  - Agent ecosystem: `settings.*` (4), `rules.*` (3), `specialist.*` (5), `mcp.servers.*` (7).
  - intentd transport extensions: `host.status`, `forward.*` (3), `client.hello`,
    `drafts.*` (3).
  - Plus the `agent.*` ACP runtime surface (24).
- **Transports:** UDS (default, mode `0600`) and a **WSS/TLS** LAN listener with bearer auth,
  origin allow-list, and TLS fingerprint pinning.
- **ACP agent runtime:** provider registry, ACP client, `AgentManager`, the agent→BE MCP
  callback server, live spawn-wiring, and the `mcp-bridge` stdio↔TCP proxy.
- **Source control:** GitHub/PR via `octocrab`, local git via `libgit2`, the change-tracking
  / accept-changes pipeline, and ripgrep-backed search.
- **Terminals & scripts:** a unified `intent-pty` host backing both interactive terminals and
  scripts (back-fill-then-tail scrollback, multi-client fan-out, process-group reaping).
- **CLI:** `serve`, `call`, `status`, `stop`, `doctor`, `import`, `service install|uninstall|status`,
  and `mcp-bridge`.
- **Persistence:** SQLite via `sqlx` with embedded migrations through `0012_known_repo`
  (WAL, `foreign_keys`, `busy_timeout`).
- Standard JSON-RPC error codes: `-32700`, `-32600`, `-32601`, `-32602`, `-32603`.

**Won't port**

- auggie codebase-retrieval (no structured CLI; `search.codebase` stays ripgrep/symbol-backed).
- `mcp.servers` **http/sse** transports (stdio only).
- the `accept-changes.execute` **`export`** action and the legacy `memories.*` RPC.

**Planned**

- The Tauri/Svelte desktop frontend (not yet a submodule; will live in the monorepo).

## Releases & channels

Releases are tag-driven and built by [dist (cargo-dist)](https://axodotdev.github.io/cargo-dist/)
(`dist-workspace.toml` + the generated `.github/workflows/release.yml`). Pushing a tag
publishes a GitHub Release with per-platform archives, `.sha256` checksums, and shell /
PowerShell installer scripts for: `aarch64-apple-darwin`, `x86_64-apple-darwin`,
`x86_64-unknown-linux-musl`, `aarch64-unknown-linux-musl`, `x86_64-pc-windows-msvc`.

- **Stable**: `vX.Y.Z` tags (e.g. `v0.9.0`).
- **Beta**: `vX.Y.Z-beta.N` tags (e.g. `v0.9.0-beta.1`) — published as GitHub prereleases.

To cut a release: bump `version` in `crates/intentd/Cargo.toml` to match, commit, then
push the tag.

### Channel manifests

After each release, CI updates a machine-readable channel manifest on a fixed release
(`.github/workflows/publish-channel-manifest.yml`): stable tags update the `stable.json`
asset on the `channel-stable` release; prerelease tags update `beta.json` on
`channel-beta`. Consumers (e.g. cloudlands-fe pin-bump automation, installer scripts)
resolve "latest per channel" from these fixed URLs. Schema (version 1):

```json
{
  "schema": 1,
  "channel": "stable",
  "version": "0.9.0",
  "tag": "v0.9.0",
  "published_at": "2026-07-21T00:00:00Z",
  "platforms": {
    "aarch64-apple-darwin": {
      "asset": "intentd-aarch64-apple-darwin.tar.xz",
      "url": "https://github.com/intent-hq/intentd/releases/download/v0.9.0/intentd-aarch64-apple-darwin.tar.xz",
      "sha256": "<hex digest of the archive>"
    }
  }
}
```

`platforms` is keyed by Rust target triple. While this repo is private, download the
manifest and artifacts via the GitHub API with a token (the `url` fields work unauthenticated
once the repo is public).

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

- [intent-hq/monorepo](https://github.com/intent-hq/monorepo) — engineering monorepo
  that mounts this repo at `packages/intentd` and holds the cross-cutting docs and tooling.

